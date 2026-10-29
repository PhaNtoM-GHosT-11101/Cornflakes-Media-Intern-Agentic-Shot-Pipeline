"""T9: SSE reconnect must replay the durable log with no gaps or duplicates.

Plan: read the first few events, disconnect, resume with Last-Event-ID, then
compare against a fresh replay from 0. If the resume cursor semantics are
right, union(partial, resume) == full replay.
"""

from __future__ import annotations

import httpx

from helpers import new_key, submit


def _read_ids(
    api: httpx.Client,
    job_id: str,
    last_id: int | None = None,
    stop_after: int | None = None,
    timeout: float = 120.0,
) -> tuple[list[int], list[str]]:
    headers = {"Last-Event-ID": str(last_id)} if last_id is not None else {}
    ids: list[int] = []
    types: list[str] = []
    with api.stream(
        "GET", f"/v1/jobs/{job_id}/events", headers=headers, timeout=timeout
    ) as resp:
        assert resp.status_code == 200
        for line in resp.iter_lines():
            if line.startswith("id: "):
                ids.append(int(line[4:]))  # "id: " is 4 chars
            elif line.startswith("event: "):
                types.append(line[7:])
            if stop_after and len(ids) >= stop_after:
                break
    return ids, types


def test_sse_reconnect_is_gap_free_and_duplicate_free(api: httpx.Client):
    job_id = submit(api, f"sse {new_key()}", shots=2)

    # 1) watch a little, then drop the connection
    partial, _ = _read_ids(api, job_id, stop_after=3, timeout=30)
    assert len(partial) == 3, "expected 3 events before disconnecting"

    # 2) resume from the last seen id and run to stream.end
    resumed, types = _read_ids(api, job_id, last_id=partial[-1])
    assert "stream.end" in types, "the stream must close with stream.end"
    assert resumed and resumed[0] > partial[-1]

    # 3) a fresh replay from 0 must equal the union, element for element
    full, full_types = _read_ids(api, job_id)
    assert full == sorted(set(partial + resumed)), (
        "partial + resume must reproduce the full log exactly"
    )
    assert len(full) == len(set(full)), "no duplicate ids in a replay"
    assert "stream.end" in full_types
