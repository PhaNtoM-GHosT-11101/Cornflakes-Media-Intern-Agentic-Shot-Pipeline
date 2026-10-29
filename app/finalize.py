"""Settle a job's money after its last in-flight ticket finishes.

Cancel and failure can happen while a worker is still inside a provider call.
Releasing the unused reservation at that instant would over-release, because the
late charge lands afterwards. Instead we release only when the job is terminal
*and* nothing is pending or running, and the ledger's unique release key makes
the finalize idempotent.
"""

from __future__ import annotations

from . import credits
from .db import get_pool


async def maybe_finalize(job_id: str) -> int | None:
    """Release unused credits once the job is terminal and quiet.

    Returns the released amount, 0 if already released, or None if the job is
    still running (caller may retry after the next completion).
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            job = await conn.fetchrow(
                "SELECT status FROM jobs WHERE id = $1 FOR UPDATE", job_id
            )
            if job is None or job["status"] not in ("done", "failed", "cancelled"):
                return None
            busy = await conn.fetchval(
                "SELECT COUNT(*) FROM tickets WHERE job_id = $1 "
                "AND state IN ('pending', 'running')",
                job_id,
            )
            if busy:
                return None
    return await credits.release_remaining(job_id)
