"""Shared black-box helpers. The tests never import app code."""

import os
import time
import uuid

import httpx

BASE = os.environ.get("PIPE_BASE", "http://127.0.0.1:8000")
MOCKS = os.environ.get("PIPE_MOCKS", "http://127.0.0.1:9000")
TERMINAL = {"done", "failed", "cancelled"}


def new_key() -> str:
    return uuid.uuid4().hex


def account(api: httpx.Client) -> dict:
    r = api.get("/v1/accounts/demo")
    r.raise_for_status()
    return r.json()


def job_json(api: httpx.Client, job_id: str) -> dict:
    r = api.get(f"/v1/jobs/{job_id}")
    r.raise_for_status()
    return r.json()


def submit(api: httpx.Client, story: str, shots: int, key: str | None = None) -> str:
    r = api.post(
        "/v1/jobs",
        json={"story": story, "shots": shots},
        headers={"Idempotency-Key": key or new_key()},
    )
    r.raise_for_status()
    return r.json()["job_id"]


def wait_terminal(api: httpx.Client, job_id: str, timeout: float = 120.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = job_json(api, job_id)
        if data["job"]["status"] in TERMINAL:
            return data
        time.sleep(1.0)
    raise TimeoutError(f"job {job_id} did not settle within {timeout}s")


def charges(data: dict) -> dict[str, int]:
    """idem_key -> amount for spend rows only."""
    return {
        row["idem_key"]: row["amount_cents"]
        for row in data["ledger"]
        if row["kind"] in ("charge", "cache_hit")
    }


def ensure_balance(api: httpx.Client, floor: int = 20_000) -> None:
    """Keep the demo account solvent across repeated test runs."""
    acct = account(api)
    if acct["balance_cents"] < floor:
        r = api.post(
            f"/v1/admin/accounts/{acct['user_id']}/topup",
            json={"amount_cents": 100_000},
        )
        r.raise_for_status()
