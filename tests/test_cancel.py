"""Cancellation settles exactly once (T10).

Cancel mid-run stops new claims, lets in-flight provider calls finish, and only
releases the unused reservation after the last running ticket settles. The
ledger must still satisfy: reserve == charged + released.
"""

from __future__ import annotations

import time

import httpx

from helpers import account, charges, job_json, new_key, submit


def test_cancel_settles_money_after_inflight_work(api: httpx.Client):
    job_id = submit(api, f"cancel {new_key()}", shots=6)
    time.sleep(2.0)  # let a couple of stages get in flight

    r = api.post(f"/v1/jobs/{job_id}/cancel")
    assert r.status_code == 200
    assert r.json()["cancelled"] is True

    deadline = time.time() + 90
    data = job_json(api, job_id)
    while time.time() < deadline:
        data = job_json(api, job_id)
        busy = [t for t in data["tickets"] if t["state"] in ("pending", "running")]
        if data["job"]["status"] == "cancelled" and not busy:
            break
        time.sleep(1.0)

    assert data["job"]["status"] == "cancelled"
    assert busy == [], "cancelled jobs must not have pending/running tickets left"

    reserve = next(r["amount_cents"] for r in data["ledger"] if r["kind"] == "reserve")
    charged = sum(charges(data).values())
    released = sum(r["amount_cents"] for r in data["ledger"] if r["kind"] == "release")
    assert reserve == charged + released
    assert released > 0, "cancelling early must give unused credits back"
    assert charged < reserve, "a cancelled job must not have paid for the whole film"

    # Nothing is left reserved for the demo account once every job is terminal.
    assert account(api)["reserved_cents"] == 0
