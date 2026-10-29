"""Shot retry: re-running a shot after the job is done must not leak money.

Regression test for the retry path: the retry bumps the reservation, and the
final release must give every unused cent back — `reserved == charged + released`
has to hold after the job settles a second time.
"""

from __future__ import annotations

import httpx

from helpers import account, charges, new_key, submit, wait_terminal


def test_retry_shot_settles_without_leaking_reservation(api: httpx.Client):
    job_id = submit(api, f"retry {new_key()}", shots=2)
    wait_terminal(api, job_id)

    r = api.post(f"/v1/jobs/{job_id}/shots/0/retry")
    assert r.status_code == 200
    assert r.json()["retried"] is True

    after = wait_terminal(api, job_id, timeout=180)
    assert after["job"]["status"] == "done"

    charged = sum(charges(after).values())
    released = sum(x["amount_cents"] for x in after["ledger"] if x["kind"] == "release")
    reserved = int(after["job"]["reserved_cents"])

    assert reserved == charged + released, (
        f"retry must not leak the reservation: reserved {reserved} != "
        f"charged {charged} + released {released}"
    )
    assert released > 0, "the extra headroom from the retry must be given back"

    # The demo account holds nothing reserved once the job is terminal.
    assert account(api)["reserved_cents"] == 0
