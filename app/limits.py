"""Admission control and cross-process concurrency limits.

Both live in Redis so they apply to every API/worker replica, not per process.

Admission (API side)
-------------------
* fixed-window submission rate per user
* cap on how many jobs one user may have queued/running at once

Slots (worker side)
-------------------
A counting semaphore implemented as a Redis sorted set:

    ZADD sem:<pool> <now+lease> <token>   # acquire if ZCARD < limit
    ZREM sem:<pool> <token>               # release

Members carry an expiry score, so a killed worker cannot hold a slot forever:
the next attempt prunes expired holders. That is the same lease idea as tickets,
one layer down.
"""

from __future__ import annotations

import asyncio
import time
import uuid

from . import cache, config
from .db import get_pool

_ACQUIRE = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local token = ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
if redis.call('ZCARD', key) < limit then
    redis.call('ZADD', key, now + ttl, token)
    return 1
end
return 0
"""


async def note_submission(user_id: str) -> int | None:
    """Fixed-window limiter. Returns Retry-After seconds when over the limit."""
    redis = cache.redis()
    bucket = int(time.time() // 60)
    key = f"rate:{user_id}:{bucket}"
    count = await redis.incr(key)
    if count == 1:
        await redis.expire(key, 70)
    if count > config.RATE_LIMIT_PER_MIN:
        return max(60 - int(time.time() % 60), 1)
    return None


async def active_jobs(user_id: str) -> int:
    pool = await get_pool()
    return int(
        await pool.fetchval(
            "SELECT count(*) FROM jobs WHERE user_id = $1 "
            "AND status IN ('queued', 'running')",
            user_id,
        )
    )


class Slots:
    """Async context manager for a global counting semaphore."""

    def __init__(self, pool: str, limit: int, lease: int = 60, wait: float = 15.0):
        self.key = f"sem:{pool}"
        self.limit = limit
        self.lease = lease
        self.wait = wait
        self.token = uuid.uuid4().hex

    async def __aenter__(self):
        redis = cache.redis()
        deadline = time.monotonic() + self.wait
        while True:
            ok = await redis.eval(
                _ACQUIRE,
                1,
                self.key,
                str(time.time()),
                str(self.lease),
                str(self.limit),
                self.token,
            )
            if int(ok) == 1:
                return self
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"no free slot in {self.key} ({self.limit}) after {self.wait}s"
                )
            await asyncio.sleep(0.1 + (uuid.uuid4().int % 100) / 1000)

    async def __aexit__(self, *exc) -> None:
        await cache.redis().zrem(self.key, self.token)


async def slots_in_use(pool: str) -> int:
    redis = cache.redis()
    await redis.zremrangebyscore(f"sem:{pool}", "-inf", time.time())
    return int(await redis.zcard(f"sem:{pool}"))
