"""Durable event log + SSE stream.

Events live in Postgres (not Redis pub/sub) so a reconnecting client can ask for
everything after `Last-Event-ID` and receive exactly one copy of each event.
"""

import asyncio
import json

from .db import get_pool


async def emit(job_id: str, type_: str, **payload) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "INSERT INTO events (job_id, type, payload) VALUES ($1, $2, $3) RETURNING id",
        job_id,
        type_,
        payload,
    )


TERMINAL = {"done", "failed", "cancelled"}


async def stream(job_id: str, last_id: int):
    """Async generator of SSE frames. Polls the durable log, so reconnects are safe."""
    pool = await get_pool()
    cursor = last_id
    idle = 0.0
    while True:
        rows = await pool.fetch(
            "SELECT id, type, payload, created_at FROM events "
            "WHERE job_id = $1 AND id > $2 ORDER BY id",
            job_id,
            cursor,
        )
        for row in rows:
            cursor = row["id"]
            data = json.dumps(
                {"id": row["id"], "type": row["type"], "payload": row["payload"],
                 "at": row["created_at"].isoformat()},
                default=str,
            )
            yield f"id: {row['id']}\nevent: {row['type']}\ndata: {data}\n\n"
        if rows:
            idle = 0.0
        else:
            idle += 0.25
            status = await pool.fetchval("SELECT status FROM jobs WHERE id = $1", job_id)
            if status in TERMINAL and idle > 1.0:
                yield f"event: stream.end\ndata: {json.dumps({'status': status})}\n\n"
                return
            if idle >= 15:
                yield ": keepalive\n\n"
                idle = 0.0
        await asyncio.sleep(0.25)
