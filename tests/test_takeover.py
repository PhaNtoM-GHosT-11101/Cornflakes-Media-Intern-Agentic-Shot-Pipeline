"""Worker takeover (T2). Local-only opt-in: kills a real worker process.

    RUN_TAKEOVER=1 .venv/bin/pytest -v tests/test_takeover.py

Requires two local workers (the default `run_local.py` setup). With Docker,
do the equivalent by hand: `docker kill <worker1>` mid-job and watch the job
finish without duplicate charges.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time

import httpx
import pytest

from helpers import charges, new_key, submit, wait_terminal

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_TAKEOVER") != "1",
    reason="set RUN_TAKEOVER=1 to kill a live worker (local stack only)",
)


def _worker_pids() -> list[int]:
    out = subprocess.run(
        ["pgrep", "-f", "python -m app.worker"], capture_output=True, text=True
    ).stdout.split()
    return [int(p) for p in out]


def test_killed_worker_job_is_taken_over(api: httpx.Client):
    job_id = submit(api, f"takeover {new_key()}", shots=5)
    time.sleep(3.0)  # workers are mid-ticket by now

    pids = _worker_pids()
    if len(pids) < 2:
        pytest.skip("need two live worker processes (run_local default)")
    victim = pids[0]
    os.kill(victim, signal.SIGKILL)

    data = wait_terminal(api, job_id, timeout=150)
    assert data["job"]["status"] == "done", (
        "the surviving worker must sweep the dead lease and finish the job"
    )

    keys = [r["idem_key"] for r in data["ledger"]]
    assert len(keys) == len(set(keys)), "takeover must not double-charge"

    retried = [t for t in data["tickets"] if t["attempt"] > 1]
    assert retried, "the killed worker's ticket should have been retried"
