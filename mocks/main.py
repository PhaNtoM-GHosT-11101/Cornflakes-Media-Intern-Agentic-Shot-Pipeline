"""Mock providers. No real AI, no paid APIs — deterministic fixtures with chaos.

Video providers:
  A = cheap / slow / flaky, B = mid, C = premium / fast.
Any provider can be switched into "out of funds" (402) at runtime:

    curl -X POST localhost:9000/chaos/providers/A/down
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import os
import random

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Mock providers")

DOWN: set[str] = set()
DRIFT_RATE = float(os.getenv("DRIFT_RATE", "0.3"))
FAIL_RATE = float(os.getenv("VIDEO_FAIL_RATE", "0.05"))
LATENCY = {"A": (1.5, 2.5), "B": (0.9, 1.6), "C": (0.4, 0.9)}


class PlanIn(BaseModel):
    story: str
    shots: int


class ImageIn(BaseModel):
    prompt: str
    seed: int
    reference: str = ""


class VideoIn(BaseModel):
    prompt: str
    seed: int
    reference: str = ""
    provider: str = "A"


class CheckIn(BaseModel):
    reference: str
    shots: list[dict]


class ComposeIn(BaseModel):
    job_id: str
    shots: list[dict]


def _vector(text: str, salt: str) -> list[float]:
    digest = hashlib.sha256(f"{salt}|{text}".encode()).digest()
    values = [(b - 127.5) / 127.5 for b in digest[:32]]
    norm = math.sqrt(sum(v * v for v in values)) or 1.0
    return [v / norm for v in values]


def _norm(values: list[float]) -> list[float]:
    n = math.sqrt(sum(v * v for v in values)) or 1.0
    return [v / n for v in values]


def _embedding(prompt: str, seed: int, reference: str) -> list[float]:
    """Close to the character reference, unless this seed drifts."""
    base = _vector(reference or "character", "reference")
    noise = _vector(prompt, f"seed:{seed}")
    mixed = [0.88 * b + 0.12 * n for b, n in zip(base, noise)]
    if random.Random(f"{prompt}|{seed}").random() < DRIFT_RATE:
        return _vector(prompt, f"drift:{seed}")
    return _norm(mixed)


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(y * y for y in b)) or 1.0
    return dot / (na * nb)


@app.get("/healthz")
async def healthz():
    return {"ok": True, "drift_rate": DRIFT_RATE, "down": sorted(DOWN)}


@app.post("/plan")
async def plan(body: PlanIn):
    await asyncio.sleep(random.uniform(0.2, 0.5))
    beats = ["establishing shot", "close-up", "wide shot", "reaction shot",
             "tracking shot", "final shot"]
    return {
        "shots": [
            {"idx": i, "prompt": f"{beats[i % len(beats)]}: {body.story}"}
            for i in range(body.shots)
        ]
    }


@app.post("/image")
async def image(body: ImageIn):
    await asyncio.sleep(random.uniform(0.2, 0.6))
    ref = body.reference or "character"
    return {
        "url": f"mock://images/{hashlib.sha256(body.prompt.encode()).hexdigest()[:12]}.png",
        "embedding": _embedding(body.prompt, body.seed, ref),
    }


@app.post("/video")
async def video(body: VideoIn):
    name = body.provider.upper()
    if name in DOWN:
        raise HTTPException(status_code=402, detail=f"provider {name}: out of funds")
    lo, hi = LATENCY.get(name, (0.5, 1.0))
    await asyncio.sleep(random.uniform(lo, hi))
    roll = random.random()
    if roll < FAIL_RATE:
        raise HTTPException(status_code=500, detail="provider internal error")
    if roll < FAIL_RATE * 2:
        raise HTTPException(status_code=429, detail="slow down",
                            headers={"Retry-After": "1"})
    return {
        "url": f"mock://clips/{name.lower()}-{hashlib.sha256(body.prompt.encode()).hexdigest()[:10]}.mp4",
        "provider": name,
        "seconds": 4,
    }


@app.post("/check")
async def check(body: CheckIn):
    await asyncio.sleep(0.1)
    reference = _vector(body.reference or "character", "reference")
    scores: dict[str, float] = {}
    for shot in body.shots:
        emb = shot.get("embedding")
        if not emb:
            continue
        scores[str(shot["idx"])] = round(_cosine(reference, emb), 4)
    return {"scores": scores}


@app.post("/compose")
async def compose(body: ComposeIn):
    await asyncio.sleep(0.8)  # burn a little "CPU"
    return {"url": f"mock://films/{body.job_id}.mp4", "clips": len(body.shots)}


# ---------------------------------------------------------------------- chaos
@app.post("/chaos/providers/{name}/down")
async def provider_down(name: str):
    DOWN.add(name.upper())
    return {"down": sorted(DOWN)}


@app.post("/chaos/providers/{name}/up")
async def provider_up(name: str):
    DOWN.discard(name.upper())
    return {"down": sorted(DOWN)}


@app.post("/chaos/reset")
async def chaos_reset():
    DOWN.clear()
    return {"down": []}


@app.get("/chaos")
async def chaos_state():
    return {"down": sorted(DOWN), "drift_rate": DRIFT_RATE, "fail_rate": FAIL_RATE}
