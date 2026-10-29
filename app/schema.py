"""Apply app/schema.sql from inside the app.

Postgres' initdb hook only runs on a brand-new volume, and only if the SQL file
is mounted from the host. Applying the (idempotent) schema from the app makes a
published image self-contained: point it at an empty database and it bootstraps.

An advisory lock serialises concurrent starters (the API and two workers may all
come up at once).
"""

from __future__ import annotations

from pathlib import Path

from . import db

SCHEMA = Path(__file__).with_name("schema.sql")
LOCK_ID = 0x0C0FFEE  # arbitrary, but stable across processes


async def ensure_schema() -> None:
    statements = [s.strip() for s in SCHEMA.read_text().split(";") if s.strip()]
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock($1)", LOCK_ID)
        try:
            for statement in statements:
                await conn.execute(statement)
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", LOCK_ID)
