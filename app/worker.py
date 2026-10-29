"""Worker process. Run two of these; they share the same queue in Postgres.

    python -m app.worker

Loop: claim a ready ticket with a lease -> heartbeat -> do the work -> charge ->
mark done. If the process dies, the lease expires and the sweeper requeues it.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import time

from . import cache, config, credits, db, events, finalize, limits, providers, queue

OWNER = f"{socket.gethostname()}:{os.getpid()}"
TERMINAL_STAGES = {"compose"}


# ----------------------------------------------------------------- stage helpers
async def _job(job_id):
    pool = await db.get_pool()
    return await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)


async def _shot(job_id, idx):
    pool = await db.get_pool()
    return await pool.fetchrow(
        "SELECT * FROM shots WHERE job_id = $1 AND idx = $2", job_id, idx
    )


async def _charge(ticket, cost: int, kind: str = "charge") -> None:
    idem = f"charge:{ticket['job_id']}:{ticket['stage']}:{ticket['shot_idx']}"
    if kind == "cache_hit":
        idem = f"cache:{ticket['job_id']}:{ticket['stage']}:{ticket['shot_idx']}"
    await credits.charge(str(ticket["job_id"]), cost, idem, kind=kind)


async def _set_shot(job_id, idx, **fields) -> None:
    cols = ", ".join(f"{k} = ${i + 3}" for i, k in enumerate(fields))
    await (await db.get_pool()).execute(
        f"UPDATE shots SET {cols} WHERE job_id = $1 AND idx = $2",
        job_id, idx, *fields.values(),
    )


# --------------------------------------------------------------------- handlers
async def stage_plan(ticket):
    job = await _job(ticket["job_id"])
    result = await providers.plan(job["story"], job["shots"])
    pool = await db.get_pool()
    for item in result["shots"]:
        await pool.execute(
            "UPDATE shots SET prompt = $3 WHERE job_id = $1 AND idx = $2",
            job["id"], int(item["idx"]), item["prompt"],
        )
    await _charge(ticket, int(ticket["cost_cents"]))
    await events.emit(str(job["id"]), "stage.plan.done",
                      shots=len(result["shots"]), cost_cents=int(ticket["cost_cents"]))
    return result, int(ticket["cost_cents"]), None


async def fetch_image(prompt: str, seed: int, ref: str) -> tuple[dict, bool]:
    """Cache-first image fetch with single-flight. Returns (payload, cache_hit)."""
    key = cache.make_key("image", "mock-image", "v1.0", prompt, seed, ref)
    payload = await cache.get(key)
    if payload is not None:
        return payload, True

    token = await cache.acquire(key)
    if token is None:
        # Someone else is generating this exact content: wait for them.
        payload = await cache.wait_for(key, timeout=8)
        if payload is not None:
            return payload, True
    try:
        async with limits.Slots("io", config.IO_CONCURRENCY):
            payload = await providers.image(prompt, seed, ref)
        await cache.put(key, payload)
        return payload, False
    finally:
        if token:
            await cache.release(key, token)


async def stage_image(ticket):
    job = await _job(ticket["job_id"])
    idx = ticket["shot_idx"]
    shot = await _shot(job["id"], idx)
    prompt = shot["prompt"] or f"shot {idx + 1}"
    seed = 1000 + idx
    ref = job["story"]
    cost = int(ticket["cost_cents"])

    payload, cache_hit = await fetch_image(prompt, seed, ref)

    await _set_shot(job["id"], idx,
                    status="image_ready",
                    embedding=payload.get("embedding"),
                    cost_cents=0 if cache_hit else cost,
                    cache_hit=cache_hit)
    if cache_hit:
        await _charge(ticket, 0, kind="cache_hit")
    else:
        await _charge(ticket, cost)
    await events.emit(str(job["id"]), "shot.image.done", shot=idx,
                      cache_hit=cache_hit, cost_cents=0 if cache_hit else cost)
    return ({"url": payload.get("url"), "embedding": payload.get("embedding")},
            0 if cache_hit else cost, None)


async def stage_clip(ticket):
    job = await _job(ticket["job_id"])
    idx = ticket["shot_idx"]
    shot = await _shot(job["id"], idx)
    prompt = shot["prompt"] or f"shot {idx + 1}"
    seed = 2000 + idx
    ref = job["story"]
    key = cache.make_key("clip", "router", "v1.0", prompt, seed, ref)

    cost = int(ticket["cost_cents"])
    payload = await cache.get(key)
    cache_hit = payload is not None
    down: list[str] = []
    provider_name = None

    if payload is None:
        token = await cache.acquire(key)
        if token is None:
            payload = await cache.wait_for(key, timeout=8)
            cache_hit = payload is not None
        if payload is None:
            try:
                async with limits.Slots("io", config.IO_CONCURRENCY):
                    routed = await providers.video_routed(prompt, seed, ref)
                provider_name = routed["provider"]
                down = routed["down"]
                payload = {
                    "url": routed["result"].get("url"),
                    "provider": provider_name,
                    "degraded": routed["degraded"],
                }
                await cache.put(key, payload)
            finally:
                if token:
                    await cache.release(key, token)

    provider_name = payload.get("provider")
    for name in down:
        await events.emit(str(job["id"]), "provider.unavailable", provider=name,
                          reason="out of funds (402)")
    await _set_shot(job["id"], idx,
                    status="clip_ready",
                    provider=provider_name,
                    cost_cents=0 if cache_hit else cost,
                    cache_hit=cache_hit,
                    degraded=bool(payload.get("degraded")))
    if cache_hit:
        await _charge(ticket, 0, kind="cache_hit")
    else:
        await _charge(ticket, cost)
    await events.emit(str(job["id"]), "shot.clip.done", shot=idx,
                      provider=provider_name,
                      degraded=bool(payload.get("degraded")),
                      cache_hit=cache_hit, cost_cents=0 if cache_hit else cost)
    return payload, 0 if cache_hit else cost, provider_name


async def stage_check(ticket):
    job = await _job(ticket["job_id"])
    pool = await db.get_pool()
    shots = await pool.fetch(
        "SELECT idx, prompt, embedding FROM shots WHERE job_id = $1 ORDER BY idx",
        job["id"],
    )
    payload = [
        {"idx": s["idx"], "prompt": s["prompt"], "embedding": s["embedding"]}
        for s in shots if s["embedding"] is not None
    ]
    result = await providers.check(job["story"], payload)
    scores = {int(k): float(v) for k, v in result["scores"].items()}

    regenerated = []
    for idx, initial in scores.items():
        score = initial
        while score < config.CONSISTENCY_THRESHOLD:
            shot = await _shot(job["id"], idx)
            attempt_no = int(shot["regen_count"])
            if attempt_no >= 2:
                break
            new_seed = 1000 + idx + 100 * (attempt_no + 1)
            fresh, regen_cached = await fetch_image(
                shot["prompt"] or f"shot {idx + 1}", new_seed, job["story"]
            )
            recheck = await providers.check(job["story"], [{
                "idx": idx, "prompt": shot["prompt"], "embedding": fresh["embedding"],
            }])
            score = float(recheck["scores"][str(idx)])
            await _set_shot(job["id"], idx,
                            embedding=fresh["embedding"],
                            consistency=score,
                            regen_count=attempt_no + 1)
            image_cost = int(await pool.fetchval(
                "SELECT unit_cents FROM price_table WHERE stage = 'image' AND version = 1"
            ))
            if not regen_cached:
                await credits.charge(
                    str(job["id"]), image_cost,
                    f"regen:{job['id']}:{idx}:{attempt_no + 1}",
                )
            scores[idx] = score
            if idx not in regenerated:
                regenerated.append(idx)
            await events.emit(str(job["id"]), "shot.regenerated", shot=idx,
                              consistency=round(score, 3),
                              attempt=attempt_no + 1,
                              cache_hit=regen_cached,
                              cost_cents=0 if regen_cached else image_cost)

    for idx, score in scores.items():
        await _set_shot(job["id"], idx, consistency=score)

    await _charge(ticket, int(ticket["cost_cents"]))
    await events.emit(str(job["id"]), "stage.check.done",
                      scores={str(k): round(v, 3) for k, v in scores.items()},
                      regenerated=regenerated,
                      threshold=config.CONSISTENCY_THRESHOLD,
                      cost_cents=int(ticket["cost_cents"]))
    return {"scores": scores, "regenerated": regenerated}, int(ticket["cost_cents"]), None


async def stage_compose(ticket):
    job = await _job(ticket["job_id"])
    pool = await db.get_pool()
    shots = await pool.fetch(
        "SELECT idx, prompt FROM shots WHERE job_id = $1 ORDER BY idx", job["id"]
    )
    async with limits.Slots("cpu", config.CPU_CONCURRENCY):
        result = await providers.compose(str(job["id"]), [dict(s) for s in shots])
    cost = int(ticket["cost_cents"])
    await _charge(ticket, cost)
    await pool.execute(
        "UPDATE jobs SET status = 'done', updated_at = now() WHERE id = $1", job["id"]
    )
    released = await credits.release_remaining(str(job["id"]))
    await events.emit(str(job["id"]), "job.done", film_url=result.get("url"),
                      cost_cents=cost, released_cents=released)
    return result, cost, None


HANDLERS = {
    "plan": stage_plan,
    "image": stage_image,
    "clip": stage_clip,
    "check": stage_check,
    "compose": stage_compose,
}


# ------------------------------------------------------------------- main loop
async def heartbeat_loop(ticket_id: int, stop: asyncio.Event):
    while not stop.is_set():
        try:
            await queue.heartbeat(ticket_id, OWNER)
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop.wait(), timeout=config.HEARTBEAT_SECONDS)
        except asyncio.TimeoutError:
            pass


async def run() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await db.get_pool()
    last_sweep = 0.0
    print(f"[worker {OWNER}] up", flush=True)

    while not stop.is_set():
        if time.time() - last_sweep > 5:
            try:
                requeued = await queue.sweep()
                if requeued:
                    print(f"[worker {OWNER}] swept {requeued} expired lease(s)", flush=True)
            except Exception as exc:
                print(f"[worker {OWNER}] sweep error: {exc}", flush=True)
            last_sweep = time.time()

        try:
            ticket = await queue.claim(OWNER)
        except Exception as exc:
            print(f"[worker {OWNER}] claim error: {exc}", flush=True)
            await asyncio.sleep(1)
            continue

        if ticket is None:
            await asyncio.sleep(0.4)
            continue

        stage = ticket["stage"]
        print(f"[worker {OWNER}] {stage} job={ticket['job_id']} "
              f"shot={ticket['shot_idx']} attempt={ticket['attempt']}", flush=True)

        hb_stop = asyncio.Event()
        hb = asyncio.create_task(heartbeat_loop(ticket["id"], hb_stop))
        try:
            result, cost, provider = await HANDLERS[stage](ticket)
            await queue.complete(ticket["id"], OWNER, result or {}, cost, provider)
        except Exception as exc:
            outcome = await queue.fail(ticket["id"], OWNER, f"{type(exc).__name__}: {exc}")
            await events.emit(str(ticket["job_id"]), "stage.failed",
                              stage=stage, shot=ticket["shot_idx"],
                              error=str(exc), outcome=outcome)
            print(f"[worker {OWNER}] {stage} failed ({outcome}): {exc}", flush=True)
        finally:
            hb_stop.set()
            hb.cancel()

        try:
            await finalize.maybe_finalize(str(ticket["job_id"]))
        except Exception as exc:
            print(f"[worker {OWNER}] finalize error: {exc}", flush=True)

    await db.close_pool()
    print(f"[worker {OWNER}] bye", flush=True)


if __name__ == "__main__":
    asyncio.run(run())
