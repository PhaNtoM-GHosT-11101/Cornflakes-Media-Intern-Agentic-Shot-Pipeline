"""Admission control: one user cannot flood the pipeline.

* two active jobs per user, the third is refused with 429 + Retry-After
* an idempotent replay of an existing job still succeeds while at the cap
  (a retry must never be punished by backpressure)
"""

from __future__ import annotations

import httpx

from helpers import new_key, wait_terminal


def test_active_job_cap_and_replay(api: httpx.Client):
    story = f"cap {new_key()}"
    created: list[str] = []
    keys: list[str] = []
    last: httpx.Response | None = None
    try:
        codes: list[int] = []
        for _ in range(3):
            key = new_key()
            keys.append(key)
            last = api.post(
                "/v1/jobs",
                json={"story": story, "shots": 2},
                headers={"Idempotency-Key": key},
            )
            codes.append(last.status_code)
            if last.status_code == 202:
                created.append(last.json()["job_id"])

        assert codes[:2] == [202, 202]
        assert codes[2] == 429, f"third concurrent job should be refused, got {codes}"
        assert last is not None and "retry-after" in {k.lower() for k in last.headers}

        replay = api.post(
            "/v1/jobs",
            json={"story": story, "shots": 2},
            headers={"Idempotency-Key": keys[0]},
        )
        assert replay.status_code == 202
        assert replay.json()["replayed"] is True
        assert replay.json()["job_id"] == created[0]
    finally:
        for job_id in created:
            wait_terminal(api, job_id, timeout=180)
