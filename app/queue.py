"""Durable ticket queue: claim with a lease, heartbeat while working, sweep dead workers.

The claim is a single SQL statement. Two workers can run it at the same time and
they will always get different tickets, with no central scheduler:

    FOR UPDATE SKIP LOCKED
"""

import asyncpg

from . import config
from .db import get_pool

_CLAIM = """
UPDATE tickets t
   SET state = 'running',
       lease_owner = $1,
       lease_expires_at = now() + ($2 || ' seconds')::interval,
       attempt = t.attempt + 1,
       error = NULL
 WHERE t.id = (
        SELECT c.id
          FROM tickets c
          JOIN jobs j ON j.id = c.job_id
         WHERE c.state = 'pending'
           AND c.run_after <= now()
           AND j.status NOT IN ('cancelled', 'failed', 'done')
           AND NOT EXISTS (
                 SELECT 1
                   FROM unnest(c.deps) AS d(dep_id)
                   JOIN tickets dt ON dt.id = d.dep_id
                  WHERE dt.state <> 'done'
               )
         ORDER BY c.id
         FOR UPDATE OF c SKIP LOCKED
         LIMIT 1
       )
RETURNING t.*;
"""


async def claim(owner: str) -> asyncpg.Record | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            ticket = await conn.fetchrow(_CLAIM, owner, str(config.LEASE_SECONDS))
            if ticket:
                await conn.execute(
                    "UPDATE jobs SET status = 'running', updated_at = now() "
                    "WHERE id = $1 AND status = 'queued'",
                    ticket["job_id"],
                )
            return ticket


async def heartbeat(ticket_id: int, owner: str) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE tickets SET lease_expires_at = now() + ($3 || ' seconds')::interval "
        "WHERE id = $1 AND lease_owner = $2 AND state = 'running'",
        ticket_id,
        owner,
        str(config.LEASE_SECONDS),
    )


async def complete(ticket_id: int, owner: str, result: dict, cost: int,
                   provider: str | None = None) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE tickets
           SET state = 'done', result = $3, cost_cents = $4, provider = $5,
               lease_owner = NULL, lease_expires_at = NULL
         WHERE id = $1 AND lease_owner = $2
        """,
        ticket_id,
        owner,
        result,
        cost,
        provider,
    )


async def fail(ticket_id: int, owner: str, error: str) -> str:
    """Retry with backoff, or bury the ticket once attempts are exhausted."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT job_id, attempt FROM tickets WHERE id = $1 AND lease_owner = $2",
            ticket_id,
            owner,
        )
        if row is None:
            return "lost-lease"
        if row["attempt"] >= config.MAX_ATTEMPTS:
            await conn.execute(
                "UPDATE tickets SET state = 'dead', error = $2, lease_owner = NULL, "
                "lease_expires_at = NULL WHERE id = $1",
                ticket_id,
                error,
            )
            await conn.execute(
                "UPDATE tickets SET state = 'cancelled' "
                "WHERE job_id = $1 AND state = 'pending'",
                row["job_id"],
            )
            await conn.execute(
                "UPDATE jobs SET status = 'failed', updated_at = now() WHERE id = $1",
                row["job_id"],
            )
            return "dead"
        delay = min(2 ** row["attempt"], 10)
        await conn.execute(
            "UPDATE tickets SET state = 'pending', error = $2, lease_owner = NULL, "
            "lease_expires_at = NULL, run_after = now() + ($3 || ' seconds')::interval "
            "WHERE id = $1",
            ticket_id,
            error,
            str(delay),
        )
        return "retry"


async def sweep() -> int:
    """Return leases whose owner stopped heartbeating."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        UPDATE tickets
           SET state = 'pending', lease_owner = NULL, lease_expires_at = NULL,
               run_after = now()
         WHERE state = 'running'
           AND lease_expires_at < now()
           AND attempt < $1
        RETURNING id
        """,
        config.MAX_ATTEMPTS,
    )
    dead = await pool.fetch(
        """
        UPDATE tickets
           SET state = 'dead', lease_owner = NULL, lease_expires_at = NULL
         WHERE state = 'running'
           AND lease_expires_at < now()
           AND attempt >= $1
        RETURNING job_id
        """,
        config.MAX_ATTEMPTS,
    )
    for row in dead:
        await pool.execute(
            "UPDATE jobs SET status = 'failed', updated_at = now() WHERE id = $1",
            row["job_id"],
        )
        await pool.execute(
            "UPDATE tickets SET state = 'cancelled' "
            "WHERE job_id = $1 AND state = 'pending'",
            row["job_id"],
        )
    return len(rows) + len(dead)
