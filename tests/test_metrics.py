"""Observability: one scrape must describe the whole system."""

from __future__ import annotations

import httpx


def test_metrics_exposes_system_state(api: httpx.Client):
    r = api.get("/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    body = r.text

    for name in (
        "pipeline_jobs_total",
        "pipeline_queue_depth",
        "pipeline_cache_hit_ratio",
        "pipeline_provider_fallbacks_total",
        "pipeline_spend_cents_total",
        "pipeline_account_balance_cents",
        "pipeline_job_duration_seconds_avg",
        "pipeline_consistency_score_avg",
        "pipeline_cache_entries_total",
    ):
        assert name in body, f"missing metric {name}"
