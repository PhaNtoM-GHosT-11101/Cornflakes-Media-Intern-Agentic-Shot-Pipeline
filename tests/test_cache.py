"""Cache correctness.

T4  Identical work must never be paid for twice:
    - rerunning an identical story: images/clips are free (cache hits),
      and the second job's expected spend is exactly plan + check + compose.
    - failures cached? request failure is not stored as a success (no negative cache).
"""

from __future__ import annotations

import httpx

from helpers import charges, new_key, submit, wait_terminal

FREE_TOTAL = 50 + 20 + 80  # plan + check + compose, in cents


def test_identical_rerun_is_non_paid(api: httpx.Client):
    story = f"never pay twice {new_key()}"
    first = wait_terminal(api, submit(api, story, shots=3), timeout=180)
    second = wait_terminal(api, submit(api, story, shots=3), timeout=180)

    assert second["job"]["status"] == "done"
    hits = [s for s in second["shots"] if s["cache_hit"]]
    # At least the images and clips must come from the cache on rerun.
    assert len(hits) >= 3, "rerun should replay images/clips from the cache"

    regens = [r for r in second["ledger"] if r["idem_key"].startswith("regen:")]
    regen_dupes_paid = [r for r in regens if r["amount_cents"] > 0]
    assert regen_dupes_paid == [], (
        "regeneration of identical content must be served from the cache too"
    )
    assert sum(charges(second).values()) == FREE_TOTAL, (
        f"second identical run must cost exactly {FREE_TOTAL} cents "
        f"(was {sum(charges(second).values())})"
    )
    # Sanity: the first run covered its drift work, or was already warm-cache.
    assert sum(charges(first).values()) >= FREE_TOTAL