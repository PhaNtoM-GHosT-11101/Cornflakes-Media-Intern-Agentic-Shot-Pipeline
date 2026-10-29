# Corflackes Inter-Agentic Shot Pipeline

A small but runnable slice of the Cornflakes Media technical project. It shows the
parts that are actually graded: **durability, money correctness, cache single-flight,
provider fallback, live progress**, with mock providers only.

```
story + refs ─► plan ─► images ─► clips (routed A/B/C) ─► consistency check
                                      │                        │
                                      └──── regenerate only ───┘
                                                   │
                                                compose ─► done
```

## Run it

### Docker (the real deliverable shape)

```bash
docker compose up --build
# open http://127.0.0.1:8000
```

Six containers: `postgres`, `redis`, `mocks`, `api`, `worker1`, `worker2`.

### No Docker (local dev)

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt pgserver redislite
.venv/bin/python run_local.py
```

`run_local.py` starts bundled Postgres and Redis binaries, applies `app/schema.sql`,
then runs the mocks, the API and two workers.

### Drive it from the terminal

```bash
python3 demo.py            # one job, live status lines
python3 demo.py --twice    # identical story twice -> second run is cache hits
```

## What to try (the graded behaviours)

| Try this | Expect |
|---|---|
| `curl -X POST localhost:8000/v1/jobs -H 'Idempotency-Key: k1' -H 'content-type: application/json' -d '{"story":"a cat sails","shots":3}'` twice with the same key | Same `job_id`, one reservation, one charge per stage (T1) |
| `docker kill cornflakes-demo-worker1-1` mid-job | `worker2` sweeps the expired lease, resumes, done shots are **not** redone or re-charged (T2) |
| Run `demo.py --twice` | Second job: images and clips show `cache_hit`, cost 0, ledger `cache_hit` rows (T4) |
| `curl -X POST localhost:9000/chaos/providers/A/down` then submit a job | A returns 402, one `provider.unavailable` event, B used, shot still completes (T5) |
| `curl -X POST localhost:8000/v1/jobs/<id>/cancel` mid-run | In-flight work ignored, pending tickets cancelled, unused credits released, ledger still balances (T10) |
| Watch the job page | SSE stream shows plan → images → clips → check/regenerate → compose; refresh still shows the same history (T9) |

Reset chaos: `make chaos-reset` (clears the mock's 402 flags **and** the app's
120-second provider cooldown in Redis).

## API

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/jobs` | `Idempotency-Key` required, returns 202 |
| GET | `/v1/jobs` | recent jobs |
| GET | `/v1/jobs/{id}` | job + shots + ledger + summary |
| GET | `/v1/jobs/{id}/events` | SSE, supports `Last-Event-ID` |
| POST | `/v1/jobs/{id}/cancel` | cancels pending work, releases credits |
| POST | `/v1/jobs/{id}/shots/{n}/retry` | re-runs one shot (+ check/compose), reserves the extra cost |
| POST | `/v1/quote` | price breakdown before submitting |
| GET | `/v1/ledger` | append-only ledger |
| GET | `/v1/accounts/{user}` | balance and reserved |
| POST | `/v1/admin/providers/reset` | clears provider cooldowns in Redis |
| GET | `/healthz` | Postgres + Redis check |

## How it works

**Tickets, not jobs, are the unit of work.** One row per `(job, stage, shot)`.
Each row carries `deps` (ticket ids that must be `done` first), so the pipeline is a
persisted state machine and resumes at node granularity.

**Claiming is one SQL statement** (`app/queue.py`):

```sql
FOR UPDATE OF c SKIP LOCKED
```

Two workers poll the same table and never receive the same ticket. The winner holds a
30-second lease and heartbeats every 10 s; a sweeper requeues expired leases and buries
tickets after 3 attempts.

**Money.** `POST /v1/jobs` reserves the quote with a conditional `UPDATE` (fail closed,
402 if the balance is short). Each successful stage charges its ticket price through an
append-only ledger whose `idem_key` is `UNIQUE`, so a crash-and-retry cannot charge twice.
Whatever is unused is released when the job finishes or is cancelled.

**Cache.** Key = SHA-256 of `(provider family, model version, prompt, seed, reference)`.
Redis is the hot layer, Postgres the durable layer. `SET NX` gives single-flight: 20
identical requests produce one provider call, the rest wait and log a zero-cost
`cache_hit`. Failures are never cached.

**Providers.** Three mock video providers (A cheap/slow, B mid, C premium). Retries use
exponential backoff with jitter; `402` marks a provider unavailable, emits one event and
falls back; other errors fall back too and mark the shot as degraded.

**Events.** Every state change appends to `events` in Postgres. SSE polls that log, so a
reconnect with `Last-Event-ID` gets exactly the missing events, no gaps or duplicates.

## Deliberately simplified (say this out loud in the walkthrough)

- Regeneration runs inside the `check` handler instead of enqueueing new tickets.
- Video cache key uses the logical model (`router`) rather than the chosen provider, so a
  cached clip is reused even if that provider later goes down.
- Charges use the quoted per-stage price, so a fallback provider never changes what the
  customer pays (quote == charge on every run, degraded or not).
- No auth: `user_id` defaults to `demo`. Rate limiting and the global I/O/CPU pools from
  the full brief are not implemented here.
- The consistency threshold (`0.75`) is hand-picked for the mock embedding distribution,
  not calibrated on a labelled set with precision/recall.

## Files

```
app/
  main.py      routes, quote, ledger, UI
  worker.py    claim -> heartbeat -> stage handler -> charge -> complete
  queue.py     claim / heartbeat / sweep
  credits.py   reserve / charge / release
  cache.py     key, get/put, single-flight, provider availability
  providers.py timeout, retry+backoff, 402, fallback
  events.py    durable log + SSE
  schema.sql   tables, prices, demo account
mocks/main.py  planner, image, video A/B/C, check, compose, chaos
templates/     index + live job page
compose.yaml   postgres, redis, mocks, api, worker1, worker2
```

## Next steps toward the full brief

1. Calibrate the consistency threshold on a labelled drift/no-drift set; report
   precision and recall.
2. Move regeneration into the queue (new tickets) with a per-job cost cap.
3. Global Redis semaphores for the I/O and CPU pools.
4. Per-user sliding-window rate limit and active-job cap.
5. `/metrics` (queue depth, provider latency/failure, cache hit ratio, spend vs quote).
6. Tests for T1–T4 against the real Postgres and Redis.
