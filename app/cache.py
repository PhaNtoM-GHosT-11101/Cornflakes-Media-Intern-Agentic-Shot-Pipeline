"""Content-addressed cache with a Redis hot layer and a Postgres durable layer.

    key = sha256(provider, model, prompt, seed, reference)

Redis gives latency and the single-flight lock. Postgres makes a hit survive a
Redis restart. A provider call is never made for a key that is already stored.
"""

import asyncio
import hashlib
import json
import os
import uuid

import redis.asyncio as aioredis

from . import config
from .db import get_pool

_redis: aioredis.Redis | None = None


def redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)
    return _redis


def make_key(*parts: object) -> str:
    blob = json.dumps(parts, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


async def get(key: str) -> dict | None:
    hot = await redis().get(f"cache:{key}")
    if hot:
        return json.loads(hot)
    row = await (await get_pool()).fetchval(
        "SELECT payload FROM content_cache WHERE key = $1", key
    )
    if row is not None:
        await redis().set(f"cache:{key}", json.dumps(row), ex=300)
    return row


async def put(key: str, payload: dict) -> None:
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO content_cache (key, payload) VALUES ($1, $2) "
        "ON CONFLICT (key) DO NOTHING",
        key,
        payload,
    )
    await redis().set(f"cache:{key}", json.dumps(payload), ex=300)


async def acquire(key: str, ttl: int = 20) -> str | None:
    """Single-flight lock. Returns a token if this caller is the winner."""
    token = uuid.uuid4().hex
    ok = await redis().set(f"lock:{key}", token, nx=True, ex=ttl)
    return token if ok else None


async def release(key: str, token: str) -> None:
    # Only the owner may release (compare-and-delete, atomically).
    await redis().eval(
        "if redis.call('get', KEYS[1]) == ARGV[1] then "
        "return redis.call('del', KEYS[1]) else return 0 end",
        1,
        f"lock:{key}",
        token,
    )


async def wait_for(key: str, timeout: float = 8.0) -> dict | None:
    """A loser waits for the winner's result instead of calling the provider."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        hit = await get(key)
        if hit is not None:
            return hit
        await asyncio.sleep(0.15)
    return None


async def provider_available(name: str) -> bool:
    return not await redis().exists(f"provider:down:{name}")


async def mark_provider_down(name: str, seconds: int = 120) -> None:
    await redis().set(f"provider:down:{name}", "1", ex=seconds)


async def reset_providers() -> None:
    keys = [k async for k in redis().scan_iter("provider:down:*")]
    if keys:
        await redis().delete(*keys)


def _demo_guard() -> None:
    if os.environ.get("DEMO_MODE") != "1":
        return
