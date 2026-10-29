#!/usr/bin/env python3
"""Run the whole demo without Docker.

Uses bundled Postgres/Redis binaries from the `pgserver` and `redislite` pip
packages, then starts the mock providers, the API and two workers locally.

    .venv/bin/python run_local.py

Docker users should use `docker compose up --build` instead.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import signal
import subprocess
import sys
import time

import asyncpg
import pgserver
import redislite

ROOT = pathlib.Path(__file__).resolve().parent
DATA = ROOT / ".data"


def apply_schema(uri: str) -> None:
    async def _run() -> None:
        conn = await asyncpg.connect(uri)
        try:
            sql = (ROOT / "app" / "schema.sql").read_text()
            for statement in (s.strip() for s in sql.split(";")):
                if statement:
                    await conn.execute(statement)
        finally:
            await conn.close()

    asyncio.run(_run())


def main() -> None:
    DATA.mkdir(exist_ok=True)
    print("starting postgres…", flush=True)
    server = pgserver.get_server(str(DATA / "pg"))
    uri = server.get_uri()

    print("starting redis…", flush=True)
    redis = redislite.Redis(str(DATA / "redis.db"))

    apply_schema(uri)
    print("schema ready", flush=True)

    env = {
        **os.environ,
        "DATABASE_URL": uri,
        "REDIS_URL": f"unix://{redis.socket_file}",
        "MOCKS_URL": "http://127.0.0.1:9000",
        "PYTHONUNBUFFERED": "1",
    }

    procs: list[subprocess.Popen] = []

    def spawn(args: list[str], wait: float = 0.0) -> None:
        procs.append(subprocess.Popen(args, cwd=ROOT, env=env))
        if wait:
            time.sleep(wait)

    print("starting mocks, api, worker1, worker2…", flush=True)
    spawn([sys.executable, "-m", "uvicorn", "mocks.main:app", "--port", "9000"], 1.5)
    spawn([sys.executable, "-m", "uvicorn", "app.main:app", "--port", "8000"], 1.5)
    spawn([sys.executable, "-m", "app.worker"])
    spawn([sys.executable, "-m", "app.worker"])

    print("\n  UI  → http://127.0.0.1:8000/\n  API → http://127.0.0.1:8000/docs\n", flush=True)

    stopping = False

    def stop(*_args) -> None:
        nonlocal stopping
        if stopping:
            return
        stopping = True
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        while not stopping:
            for p in procs:
                if p.poll() is not None:
                    # Report, but keep the remaining services alive: a workload
                    # may kill a worker on purpose (lease-takeover testing).
                    print(f"process exited: {p.args} ({p.returncode})", flush=True)
                    procs.remove(p)
            time.sleep(1)
    finally:
        stop()
        try:
            server.clean_shutdown()
        except Exception:
            pass
        print("stopped", flush=True)


if __name__ == "__main__":
    main()
