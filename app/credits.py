"""Credit meter: reserve up front, charge per successful stage, release the rest.

The ledger is append-only and `idem_key` is UNIQUE, so charging twice for the
same node is impossible even if a worker crashes and retries.
"""

import asyncpg

from .db import get_pool


async def reserve(conn: asyncpg.Connection, user_id: str, amount: int) -> bool:
    """Fail closed: no balance, no job."""
    row = await conn.fetchval(
        """
        UPDATE accounts
           SET reserved_cents = reserved_cents + $2
         WHERE user_id = $1
           AND balance_cents - reserved_cents >= $2
        RETURNING 1
        """,
        user_id,
        amount,
    )
    return row is not None


async def charge(job_id: str, amount: int, idem: str, kind: str = "charge") -> bool:
    """Move `amount` from reserved to spent. Returns False if already charged."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            inserted = await conn.fetchval(
                """
                INSERT INTO ledger (job_id, kind, amount_cents, idem_key)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (idem_key) DO NOTHING
                RETURNING 1
                """,
                job_id,
                kind,
                amount,
                idem,
            )
            if not inserted:
                return False
            await conn.execute(
                """
                UPDATE accounts a
                   SET balance_cents  = a.balance_cents  - $2,
                       reserved_cents = GREATEST(a.reserved_cents - $2, 0)
                  FROM jobs j
                 WHERE j.id = $1 AND a.user_id = j.user_id
                """,
                job_id,
                amount,
            )
            return True


async def release_remaining(job_id: str) -> int:
    """Give back whatever was reserved but never charged. Returns the amount."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            job = await conn.fetchrow(
                "SELECT user_id, reserved_cents FROM jobs WHERE id = $1", job_id
            )
            if job is None:
                return 0
            spent = await conn.fetchval(
                "SELECT COALESCE(SUM(amount_cents), 0) FROM ledger "
                "WHERE job_id = $1 AND kind IN ('charge', 'cache_hit')",
                job_id,
            )
            remaining = max(int(job["reserved_cents"]) - int(spent), 0)
            idem = f"release:{job_id}"
            inserted = await conn.fetchval(
                """
                INSERT INTO ledger (job_id, kind, amount_cents, idem_key)
                VALUES ($1, 'release', $2, $3)
                ON CONFLICT (idem_key) DO NOTHING
                RETURNING 1
                """,
                job_id,
                remaining,
                idem,
            )
            if inserted and remaining > 0:
                await conn.execute(
                    """
                    UPDATE accounts
                       SET reserved_cents = GREATEST(reserved_cents - $2, 0)
                     WHERE user_id = $1
                    """,
                    job["user_id"],
                    remaining,
                )
            return remaining
