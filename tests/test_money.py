"""Money correctness (the heaviest-graded area).

T1  Idempotency-Key replay: same key -> same job, one reservation, one charge
    per pipeline node even if the client retries after the provider succeeded.
T1b Ledger integrity on every terminal job:
    reserve == sum(charges) + release, and every ledger key is unique.
"""

from __future__ import annotations

import httpx

from helpers import account, charges, job_json, new_key, submit, wait_terminal


def test_idempotent_submit_replays_same_job(api: httpx.Client):
    key = new_key()
    before = account(api)
    story = f"idempotency {key}"
    a = api.post("/v1/jobs", json={"story": story, "shots": 2},
                 headers={"Idempotency-Key": key}).json()
    b = api.post("/v1/jobs", json={"story": story, "shots": 2},
                 headers={"Idempotency-Key": key}).json()
    after = wait_terminal(api, a["job_id"])
    at_end = account(api)

    assert a["job_id"] == b["job_id"]
    assert b["replayed"] is True

    reserve_rows = [r for r in after["ledger"] if r["kind"] == "reserve"]
    assert len(reserve_rows) == 1, "a replayed key must never reserve twice"
    spent = sum(charges(after).values())
    assert at_end["balance_cents"] - before["balance_cents"] == -spent, (
        "the account is debited only for actual charges"
    )
    assert at_end["reserved_cents"] == before["reserved_cents"], (
        "settled jobs hold no reservation"
    )


def test_ledger_balance_and_unique_keys(api: httpx.Client):
    job_id = submit(api, f"ledger balance {new_key()}", shots=3)
    data = wait_terminal(api, job_id, timeout=180)
    assert data["job"]["status"] == "done"

    reserve = next(r["amount_cents"] for r in data["ledger"] if r["kind"] == "reserve")
    charged = sum(charges(data).values())
    released = sum(r["amount_cents"] for r in data["ledger"] if r["kind"] == "release")
    assert reserve == charged + released, "every reserved cent is spent or released"

    keys = [r["idem_key"] for r in data["ledger"]]
    assert len(keys) == len(set(keys)), "no ledger key may repeat"


def test_cache_hit_does_not_cost_anything(api: httpx.Client):
    """A cache hit is a real completion with a zero-cost ledger row."""
    job_id = submit(api, f"cache hit costs zero {new_key()}", shots=2)
    data = wait_terminal(api, job_id)
    hit_rows = [r for r in data["ledger"] if r["kind"] == "cache_hit"]
    assert sum(r["amount_cents"] for r in hit_rows) == 0
    for shot in data["shots"]:
        if shot["cache_hit"]:
            assert shot["cost_cents"] == 0
            assert shot["status"] in ("image_ready", "clip_ready")