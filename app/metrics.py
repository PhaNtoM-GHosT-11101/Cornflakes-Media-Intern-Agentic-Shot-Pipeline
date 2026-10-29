"""Prometheus metrics, derived from the Postgres source of truth.

Why not process-local counters? Because a scrape hits *one* HTTP process while the
work happens in *two* workers. Reading the durable tables on each scrape means a
single `/metrics` response describes the whole system, and a worker restart cannot
reset the numbers.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Gauge, generate_latest

from . import cache, config, limits
from .db import get_pool

REGISTRY = CollectorRegistry()

jobs_total = Gauge(
    "pipeline_jobs_total", "Jobs by terminal or active status", ["status"], registry=REGISTRY
)
queue_depth = Gauge(
    "pipeline_queue_depth", "Tickets by state", ["state"], registry=REGISTRY
)
cache_hit_ratio = Gauge(
    "pipeline_cache_hit_ratio",
    "Cache hits / (hits + paid renders) per stage",
    ["stage"],
    registry=REGISTRY,
)
provider_fallbacks = Gauge(
    "pipeline_provider_fallbacks_total",
    "Times a provider was skipped (402 / error) and work fell back",
    ["provider"],
    registry=REGISTRY,
)
provider_down = Gauge(
    "pipeline_provider_down", "1 while a provider is in cooldown", ["provider"], registry=REGISTRY
)
spend_cents_total = Gauge(
    "pipeline_spend_cents_total", "Sum of charged cents across all jobs", registry=REGISTRY
)
account_balance_cents = Gauge(
    "pipeline_account_balance_cents", "Demo account balance in cents", registry=REGISTRY
)
account_reserved_cents = Gauge(
    "pipeline_account_reserved_cents", "Demo account reserved in cents", registry=REGISTRY
)
job_duration_seconds_avg = Gauge(
    "pipeline_job_duration_seconds_avg", "Mean terminal job duration", registry=REGISTRY
)
consistency_score_avg = Gauge(
    "pipeline_consistency_score_avg", "Mean character-consistency score", registry=REGISTRY
)
cache_entries_total = Gauge(
    "pipeline_cache_entries_total", "Durable content-cache entries", registry=REGISTRY
)
slots_in_use = Gauge(
    "pipeline_slots_in_use", "Redis semaphore holders per pool", ["pool"], registry=REGISTRY
)


async def render() -> bytes:
    pool = await get_pool()

    for gauge in (jobs_total, queue_depth, cache_hit_ratio, provider_fallbacks, slots_in_use):
        gauge.clear()

    for row in await pool.fetch("SELECT status, count(*) AS n FROM jobs GROUP BY status"):
        jobs_total.labels(status=row["status"]).set(row["n"])

    for row in await pool.fetch(
        "SELECT state, count(*) AS n FROM tickets GROUP BY state"
    ):
        queue_depth.labels(state=row["state"]).set(row["n"])

    # idem keys look like charge:<job>:<stage>:<shot> / cache:<job>:<stage>:<shot>
    rows = await pool.fetch(
        """
        SELECT split_part(idem_key, ':', 3) AS stage,
               count(*) FILTER (WHERE kind = 'cache_hit') AS hits,
               count(*) AS total
          FROM ledger
         WHERE (idem_key LIKE 'charge:%' OR idem_key LIKE 'cache:%')
           AND split_part(idem_key, ':', 3) IN ('image', 'clip')
         GROUP BY 1
        """
    )
    for row in rows:
        if row["total"]:
            cache_hit_ratio.labels(stage=row["stage"]).set(
                row["hits"] / row["total"]
            )

    for row in await pool.fetch(
        "SELECT payload->>'provider' AS provider, count(*) AS n "
        "FROM events WHERE type = 'provider.unavailable' GROUP BY 1"
    ):
        provider_fallbacks.labels(provider=row["provider"] or "?").set(row["n"])

    for name in ("A", "B", "C"):
        available = await cache.provider_available(name)
        provider_down.labels(provider=name).set(0 if available else 1)

    for pool_name in ("io", "cpu"):
        slots_in_use.labels(pool=pool_name).set(
            await limits.slots_in_use(pool_name)
        )

    spend_cents_total.set(
        await pool.fetchval(
            "SELECT COALESCE(SUM(amount_cents), 0) FROM ledger WHERE kind = 'charge'"
        )
    )
    acct = await pool.fetchrow(
        "SELECT balance_cents, reserved_cents FROM accounts WHERE user_id = $1",
        config.USER_ID,
    )
    if acct:
        account_balance_cents.set(acct["balance_cents"])
        account_reserved_cents.set(acct["reserved_cents"])

    duration = await pool.fetchval(
        "SELECT AVG(EXTRACT(EPOCH FROM updated_at - created_at)) FROM jobs "
        "WHERE status IN ('done', 'failed', 'cancelled')"
    )
    job_duration_seconds_avg.set(float(duration or 0))
    consistency_score_avg.set(
        float(await pool.fetchval(
            "SELECT COALESCE(AVG(consistency), 0) FROM shots WHERE consistency IS NOT NULL"
        ) or 0)
    )
    cache_entries_total.set(await pool.fetchval("SELECT count(*) FROM content_cache"))

    return generate_latest(REGISTRY)
