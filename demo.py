#!/usr/bin/env python3
"""Drive the demo from the terminal (stdlib only).

    python3 demo.py            # one job, watch it finish
    python3 demo.py --twice    # same story twice: second run is all cache hits
    python3 demo.py --shots 4
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
import uuid

BASE = "http://127.0.0.1:8000"


def call(method: str, path: str, body: dict | None = None, key: str | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if key:
        req.add_header("Idempotency-Key", key)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code}: {e.read().decode()}", file=sys.stderr)
        raise


def submit(story: str, shots: int, key: str) -> str:
    q = call("POST", "/v1/quote", {"story": story, "shots": shots})
    print(f"quote: {q['total_cents']/100:.2f}  {q['breakdown']}")
    r = call("POST", "/v1/jobs", {"story": story, "shots": shots}, key)
    print(f"job:   {r['job_id']}  (replayed={r['replayed']})")
    return r["job_id"]


def watch(job_id: str, timeout: float = 120) -> dict:
    started = time.time()
    last = None
    while time.time() - started < timeout:
        d = call("GET", f"/v1/jobs/{job_id}")
        shots = " ".join(
            f"{s['idx'] + 1}:{s['status'][:5]}" for s in sorted(d["shots"], key=lambda s: s["idx"])
        )
        line = f"{d['job']['status']:<9} charged={d['summary']['charged_cents']/100:>6.2f}  {shots}"
        if line != last:
            print(line, flush=True)
            last = line
        if d["job"]["status"] in ("done", "failed", "cancelled"):
            return d
        time.sleep(1)
    raise SystemExit("timed out")


def table(d: dict) -> None:
    print("\nshot  status      provider  cost    cache  consistency  regen")
    for s in sorted(d["shots"], key=lambda s: s["idx"]):
        consistency = "-" if s["consistency"] is None else f"{s['consistency']:.3f}"
        print(
            f"{s['idx'] + 1:>4}  {s['status']:<10}  {str(s['provider'] or '-'):<8}  "
            f"{s['cost_cents'] / 100:>6.2f}  {str(bool(s['cache_hit'])):<5}  "
            f"{consistency:<11}  {s['regen_count']}"
        )
    s = d["summary"]
    print(f"\nquote={s['quote_cents']/100:.2f}  charged={s['charged_cents']/100:.2f}  "
          f"released={s['released_cents']/100:.2f}  status={d['job']['status']}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shots", type=int, default=3)
    ap.add_argument("--story", default="A cat learns to sail across the Arabian Sea")
    ap.add_argument("--twice", action="store_true")
    args = ap.parse_args()

    job = submit(args.story, args.shots, str(uuid.uuid4()))
    result = watch(job)
    table(result)

    if args.twice:
        print("\n--- submitting the identical story again (expect cache hits) ---")
        job2 = submit(args.story, args.shots, str(uuid.uuid4()))
        result2 = watch(job2)
        table(result2)
        hits = sum(1 for s in result2["shots"] if s["cache_hit"])
        print(f"\ncache hits: {hits}/{args.shots} shots · "
              f"charged {result2['summary']['charged_cents']/100:.2f} vs "
              f"quote {result2['summary']['quote_cents']/100:.2f}")


if __name__ == "__main__":
    main()
