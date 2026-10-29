"""HTTP API: jobs, quote, ledger, SSE, and a tiny UI."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from prometheus_client import CONTENT_TYPE_LATEST
from pydantic import BaseModel, Field

from . import cache, config, credits, db, events, finalize, limits, metrics, providers
from .db import get_pool

templates = Jinja2Templates(directory="templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.get_pool()
    yield
    await db.close_pool()


app = FastAPI(title="Cornflakes Media - Agentic Shot Pipeline", lifespan=lifespan)


# --------------------------------------------------------------------- models
class JobIn(BaseModel):
    story: str = Field(min_length=3, max_length=2000)
    shots: int = Field(default=3, ge=1, le=6)
    user_id: str = config.USER_ID


class QuoteIn(BaseModel):
    story: str = Field(min_length=3, max_length=2000)
    shots: int = Field(default=3, ge=1, le=6)


# ---------------------------------------------------------------------- prices
async def prices() -> dict[str, int]:
    pool = await get_pool()
    rows = await pool.fetch("SELECT stage, unit_cents FROM price_table WHERE version = 1")
    return {r["stage"]: int(r["unit_cents"]) for r in rows}


async def quote_for(shots: int) -> dict:
    p = await prices()
    breakdown = {
        "plan": p["plan"],
        "images": p["image"] * shots,
        "clips": p["clip"] * shots,
        "check": p["check"],
        "compose": p["compose"],
    }
    return {"breakdown": breakdown, "total_cents": sum(breakdown.values())}


# ------------------------------------------------------------------------- API
@app.get("/healthz")
async def healthz():
    pool = await get_pool()
    await pool.fetchval("SELECT 1")
    await cache.redis().ping()
    return {"ok": True}


@app.get("/metrics")
async def metrics_endpoint():
    return Response(await metrics.render(), media_type=CONTENT_TYPE_LATEST)


@app.post("/v1/quote")
async def post_quote(body: QuoteIn):
    return await quote_for(body.shots)


@app.post("/v1/jobs", status_code=202)
async def create_job(
    body: JobIn,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    q = await quote_for(body.shots)
    p = await prices()
    # Reserve the quote plus headroom for up to 2 consistency regenerations per shot.
    reserve_amount = q["total_cents"] + 2 * body.shots * p["image"]
    pool = await get_pool()

    # A replay must never be blocked by admission control: same key, same answer.
    existing = await pool.fetchrow(
        "SELECT id FROM jobs WHERE user_id = $1 AND idem_key = $2",
        body.user_id,
        idempotency_key,
    )
    if existing:
        return {"job_id": str(existing["id"]), "replayed": True}

    retry_after = await limits.note_submission(body.user_id)
    if retry_after is not None:
        raise HTTPException(
            status_code=429,
            detail=f"submission rate limit exceeded ({config.RATE_LIMIT_PER_MIN}/min)",
            headers={"Retry-After": str(retry_after)},
        )
    active = await limits.active_jobs(body.user_id)
    if active >= config.ACTIVE_JOB_LIMIT:
        raise HTTPException(
            status_code=429,
            detail=(
                f"too many active jobs ({active}/{config.ACTIVE_JOB_LIMIT}); "
                "wait for one to finish or cancel it"
            ),
            headers={"Retry-After": "5"},
        )

    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                existing = await conn.fetchrow(
                    "SELECT id FROM jobs WHERE user_id = $1 AND idem_key = $2",
                    body.user_id,
                    idempotency_key,
                )
                if existing:
                    return {"job_id": str(existing["id"]), "replayed": True}

                if not await credits.reserve(conn, body.user_id, reserve_amount):
                    raise HTTPException(status_code=402, detail="insufficient credits")

                job_id = await conn.fetchval(
                    """
                    INSERT INTO jobs (id, user_id, idem_key, story, shots,
                                      quote_cents, reserved_cents)
                    VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id
                    """,
                    uuid.uuid4(),
                    body.user_id,
                    idempotency_key,
                    body.story,
                    body.shots,
                    q["total_cents"],
                    reserve_amount,
                )
                await conn.executemany(
                    "INSERT INTO shots (job_id, idx, prompt) VALUES ($1, $2, $3)",
                    [(job_id, i, None) for i in range(body.shots)],
                )

                plan_id = await conn.fetchval(
                    "INSERT INTO tickets (job_id, stage, cost_cents) VALUES ($1, 'plan', $2) "
                    "RETURNING id",
                    job_id,
                    p["plan"],
                )
                image_ids, clip_ids = [], []
                for i in range(body.shots):
                    image_ids.append(
                        await conn.fetchval(
                            "INSERT INTO tickets (job_id, stage, shot_idx, deps, cost_cents) "
                            "VALUES ($1, 'image', $2, $3, $4) RETURNING id",
                            job_id, i, [plan_id], p["image"],
                        )
                    )
                    clip_ids.append(
                        await conn.fetchval(
                            "INSERT INTO tickets (job_id, stage, shot_idx, deps, cost_cents) "
                            "VALUES ($1, 'clip', $2, $3, $4) RETURNING id",
                            job_id, i, [image_ids[-1]], p["clip"],
                        )
                    )
                check_id = await conn.fetchval(
                    "INSERT INTO tickets (job_id, stage, deps, cost_cents) "
                    "VALUES ($1, 'check', $2, $3) RETURNING id",
                    job_id, clip_ids, p["check"],
                )
                await conn.execute(
                    "INSERT INTO tickets (job_id, stage, deps, cost_cents) "
                    "VALUES ($1, 'compose', $2, $3)",
                    job_id, [check_id], p["compose"],
                )
                await conn.execute(
                    "INSERT INTO ledger (job_id, kind, amount_cents, idem_key) "
                    "VALUES ($1, 'reserve', $2, $3)",
                    job_id, reserve_amount, f"reserve:{job_id}",
                )
        except Exception as exc:
            if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
                row = await pool.fetchrow(
                    "SELECT id FROM jobs WHERE user_id = $1 AND idem_key = $2",
                    body.user_id, idempotency_key,
                )
                if row:
                    return {"job_id": str(row["id"]), "replayed": True}
            raise

    await events.emit(str(job_id), "job.created", shots=body.shots,
                      quote_cents=q["total_cents"])
    return {"job_id": str(job_id), "quote_cents": q["total_cents"], "replayed": False}


@app.get("/v1/jobs")
async def list_jobs(limit: int = 20):
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT id, status, story, shots, quote_cents, created_at "
        "FROM jobs ORDER BY created_at DESC LIMIT $1",
        min(limit, 100),
    )
    return [dict(r) for r in rows]


@app.get("/v1/jobs/{job_id}")
async def get_job(job_id: str):
    pool = await get_pool()
    job = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    shots = await pool.fetch(
        "SELECT idx, prompt, status, provider, cost_cents, cache_hit, consistency, "
        "regen_count FROM shots WHERE job_id = $1 ORDER BY idx",
        job_id,
    )
    ledger = await pool.fetch(
        "SELECT kind, amount_cents, idem_key, created_at FROM ledger "
        "WHERE job_id = $1 ORDER BY id",
        job_id,
    )
    tickets = await pool.fetch(
        "SELECT stage, shot_idx, state, attempt, error FROM tickets "
        "WHERE job_id = $1 ORDER BY id",
        job_id,
    )
    charged = sum(
        int(r["amount_cents"]) for r in ledger if r["kind"] in ("charge", "cache_hit")
    )
    return {
        "job": dict(job),
        "shots": [dict(s) for s in shots],
        "tickets": [dict(t) for t in tickets],
        "ledger": [dict(r) for r in ledger],
        "summary": {
            "quote_cents": int(job["quote_cents"]),
            "charged_cents": charged,
            "released_cents": sum(
                int(r["amount_cents"]) for r in ledger if r["kind"] == "release"
            ),
        },
    }


@app.get("/v1/jobs/{job_id}/events")
async def job_events(job_id: str, request: Request, last_id: int = 0):
    pool = await get_pool()
    if await pool.fetchval("SELECT 1 FROM jobs WHERE id = $1", job_id) is None:
        raise HTTPException(status_code=404, detail="job not found")
    header_last = request.headers.get("last-event-id")
    if header_last and header_last.isdigit():
        last_id = int(header_last)
    return StreamingResponse(
        events.stream(job_id, last_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/v1/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            job = await conn.fetchrow(
                "SELECT status FROM jobs WHERE id = $1 FOR UPDATE", job_id
            )
            if job is None:
                raise HTTPException(status_code=404, detail="job not found")
            if job["status"] in events.TERMINAL:
                raise HTTPException(status_code=409, detail=f"job is {job['status']}")
            await conn.execute(
                "UPDATE jobs SET status = 'cancelled', updated_at = now() WHERE id = $1",
                job_id,
            )
            await conn.execute(
                "UPDATE tickets SET state = 'cancelled' "
                "WHERE job_id = $1 AND state = 'pending'",
                job_id,
            )
    released = await finalize.maybe_finalize(job_id)
    await events.emit(job_id, "job.cancelled", released_cents=released or 0)
    return {
        "cancelled": True,
        "released_cents": released or 0,
        "settling": released is None,
    }


@app.post("/v1/jobs/{job_id}/shots/{shot_idx}/retry")
async def retry_shot(job_id: str, shot_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            job = await conn.fetchrow(
                "SELECT user_id, status FROM jobs WHERE id = $1 FOR UPDATE", job_id
            )
            if job is None:
                raise HTTPException(status_code=404, detail="job not found")
            rows = await conn.fetch(
                "SELECT id, cost_cents FROM tickets WHERE job_id = $1 AND "
                "(shot_idx = $2 OR stage IN ('check', 'compose'))",
                job_id, shot_idx,
            )
            extra = sum(int(r["cost_cents"]) for r in rows if int(r["cost_cents"]) > 0)
            if not await credits.reserve(conn, job["user_id"], extra):
                raise HTTPException(status_code=402, detail="insufficient credits")
            await conn.execute(
                "UPDATE tickets SET state = 'pending', attempt = 0, error = NULL, "
                "run_after = now(), deps = deps "
                "WHERE job_id = $1 AND (shot_idx = $2 OR stage IN ('check', 'compose'))",
                job_id, shot_idx,
            )
            await conn.execute(
                "UPDATE shots SET status = 'pending', consistency = NULL, "
                "cache_hit = false, embedding = NULL WHERE job_id = $1 AND idx = $2",
                job_id, shot_idx,
            )
            await conn.execute(
                "UPDATE jobs SET status = 'running', reserved_cents = reserved_cents + $2, "
                "updated_at = now() WHERE id = $1",
                job_id, extra,
            )
    await events.emit(job_id, "shot.retry_requested", shot=shot_idx, reserved_cents=extra)
    return {"retried": True, "reserved_cents": extra}


@app.get("/v1/ledger")
async def get_ledger(limit: int = 50):
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT l.id, l.job_id, j.story, l.kind, l.amount_cents, l.idem_key, l.created_at "
        "FROM ledger l LEFT JOIN jobs j ON j.id = l.job_id ORDER BY l.id DESC LIMIT $1",
        min(limit, 200),
    )
    return [dict(r) for r in rows]


@app.get("/v1/accounts/{user_id}")
async def get_account(user_id: str):
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM accounts WHERE user_id = $1", user_id)
    if row is None:
        raise HTTPException(status_code=404, detail="account not found")
    return dict(row)


@app.post("/v1/admin/providers/reset")
async def admin_reset_providers():
    """Clear provider cooldown flags kept in Redis (demo convenience)."""
    await cache.reset_providers()
    return {"reset": True}


class TopupIn(BaseModel):
    amount_cents: int = Field(gt=0, le=10_000_000)


@app.post("/v1/admin/accounts/{user_id}/topup")
async def admin_topup(user_id: str, body: TopupIn):
    """Demo-only credit top-up. Not part of the charged job ledger."""
    pool = await get_pool()
    row = await pool.fetchrow(
        "UPDATE accounts SET balance_cents = balance_cents + $2 "
        "WHERE user_id = $1 RETURNING *",
        user_id,
        body.amount_cents,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="account not found")
    return dict(row)


# -------------------------------------------------------------------------- UI
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    pool = await get_pool()
    jobs = await pool.fetch(
        "SELECT id, status, story, shots, quote_cents, created_at "
        "FROM jobs ORDER BY created_at DESC LIMIT 20"
    )
    account = await pool.fetchrow("SELECT * FROM accounts WHERE user_id = $1", config.USER_ID)
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"jobs": [dict(j) for j in jobs], "account": dict(account)},
    )


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job_page(request: Request, job_id: str):
    pool = await get_pool()
    job = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return templates.TemplateResponse(
        request=request, name="job.html", context={"job": dict(job)}
    )
