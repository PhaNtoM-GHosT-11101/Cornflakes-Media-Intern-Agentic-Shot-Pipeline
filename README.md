# Cornflakes Media · Agentic Shot Pipeline

Technical project (intern round). A runnable slice of the brief with **mock providers
only**: story → plan → images → clips → character-consistency check → compose, with
money, durability, caching, provider fallback and live progress all observable from the
outside.

```
story + refs ─► plan ─► images ─► clips (routed A/B/C) ─► consistency check
                                      │                        │
                                      └──── regenerate only ───┘
                                                   │
                                                compose ─► done
```

The rules this repo tries to keep, in one line each:

| Guarantee | How |
|---|---|
| A retried request never creates a second job | `jobs.idem_key` is `UNIQUE`; the second request returns the same `job_id` |
| The account can never go negative | quote + headroom reserved in one conditional `UPDATE`; 402 if short |
| Crash-and-retry never double-charges | append-only ledger with `UNIQUE idem_key` per `(job, stage, shot[, regen])` |
| Nobody pays for work that did not happen | unused reservation released only after the last in-flight ticket settles |
| A killed worker loses at most one lease | 30 s leases + 10 s heartbeats + sweeper requeue, 3 attempts then bury |
| Identical work is rendered once | SHA-256 content key, Redis hot + Postgres durable, `SET NX` single-flight |
| A dead provider does not fail the film | 402 triggers fallback A→B→C, a 120 s cooldown and one event |
| The customer pays the quote, not the provider | every charge uses the quoted per-stage price |
| One user cannot flood the system | 30 submissions/min, 2 active jobs, 429 + `Retry-After` |
| A killed slot holder cannot deadlock a pool | Redis semaphores with expiring lease scores (I/O 8, CPU 2) |

## Run it

### Docker (the deliverable shape)

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

`run_local.py` starts bundled Postgres and Redis, applies `app/schema.sql`, then runs the
mocks, the API and two workers. It keeps the remaining processes alive if one worker
exits — that is what makes the worker-kill test below meaningful.

### Drive it from the terminal

```bash
python3 demo.py            # one job, live status lines
python3 demo.py --twice    # identical story twice -> second run is cache hits
```

Real output from a `--twice` run:

```
quote=10.50  charged=13.50  released=3.00  status=done
...
cache hits: 3/3 shots · charged 1.50 vs quote 10.50
```

The first run paid three consistency regenerations (3 × 1.00); the rerun replayed the
cached renders *and the cached regenerations*, so it only paid plan + check + compose.

### Tests

The suite is black-box against a running stack (local or compose):

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest -v tests          # 10 passed, 1 skipped
RUN_TAKEOVER=1 .venv/bin/pytest -v tests/test_takeover.py
```

`test_takeover.py` is opt-in because it `SIGKILL`s a real worker process and waits for
the surviving worker to sweep the lease (it needs two local workers).

### Metrics

```bash
curl -s localhost:8000/metrics | grep pipeline_
```

`/metrics` is one endpoint for the whole system: it reads Postgres/Redis at scrape time
instead of counting inside a process, so the worker replicas are included and a restart
cannot reset the numbers.

```
pipeline_jobs_total{status="done"} 20
pipeline_queue_depth{state="done"} 167
pipeline_cache_hit_ratio{stage="clip"} 0.173
pipeline_spend_cents_total 18250
pipeline_account_balance_cents 91750
pipeline_slots_in_use{pool="io"} 0
```

## What to try (the graded behaviours)

| Try this | Expect |
|---|---|
| Same `POST /v1/jobs` twice with one `Idempotency-Key` | Same `job_id`, `replayed: true`, one reservation, one charge per node (T1) |
| `kill -9` a worker mid-job (or `docker kill <worker1>`) | The other worker sweeps the expired lease, done shots are not redone, spent shots are not recharged (T2) |
| `python3 demo.py --twice` | Second job: `cache_hit` on images and clips, `charged 1.50 vs quote 10.50` (T4) |
| `curl -X POST localhost:9000/chaos/providers/A/down` then submit | A returns 402, one `provider.unavailable` event, B is used, clip price stays 2.00 (T5) |
| `curl -X POST localhost:8000/v1/jobs/<id>/cancel` mid-run | Pending tickets cancelled, in-flight charges land, unused credits released, ledger balances (T10) |
| Third job while two are active | 429 with `Retry-After`; replaying an existing key still works |
| Watch the job page, then refresh | SSE shows plan → images → clips → check/regen → compose; a reload replays the durable event log (T9) |

Reset chaos: `make chaos-reset` (clears the mock's 402 flags **and** the app's 120 s
provider cooldown in Redis).

## API

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/jobs` | `Idempotency-Key` required, 202; 429 on admission limits |
| GET | `/v1/jobs` | recent jobs |
| GET | `/v1/jobs/{id}` | job + shots + ledger + summary |
| GET | `/v1/jobs/{id}/events` | SSE, supports `Last-Event-ID` |
| POST | `/v1/jobs/{id}/cancel` | cancels pending work, releases credits |
| POST | `/v1/jobs/{id}/shots/{n}/retry` | re-runs one shot (+ check/compose), reserves the extra cost |
| POST | `/v1/quote` | price breakdown before submitting |
| GET | `/v1/ledger` | append-only ledger |
| GET | `/v1/accounts/{user}` | balance and reserved |
| GET | `/metrics` | Prometheus text, derived from durable state |
| POST | `/v1/admin/providers/reset` | clears provider cooldowns in Redis |
| POST | `/v1/admin/accounts/{user}/topup` | demo-only credit top-up (used by test fixture) |
| GET | `/healthz` | Postgres + Redis check |

## How it works

### Tickets, not jobs, are the unit of work

One row per `(job, stage, shot)` carrying `deps` (ticket ids that must be `done` first).
The pipeline is therefore a persisted graph, and a restart resumes at node granularity
instead of re-running a whole job.

Claiming is one statement (`app/queue.py`):

```sql
FOR UPDATE OF c SKIP LOCKED
```

Two workers poll the same table and never receive the same ticket. The winner holds a
30-second lease and heartbeats every 10 s; the sweeper requeues expired leases and
buries a ticket after 3 attempts. `done` shots are never redone, and because charges are
keyed by `(job, stage, shot)` a retried ticket cannot charge twice.

### Money

Submission computes the quote from `price_table` and reserves:

```
reserve = quote + 2 × shots × image_price        # headroom for up to 2 regens/shot
```

The reservation is a single conditional `UPDATE ... WHERE balance_cents - reserved_cents
>= $2` — it fails closed with 402 rather than allowing an overdraft. Each successful
stage inserts a ledger row whose `idem_key` is `UNIQUE`; the charge uses the **quoted**
price, so a fallback provider never changes the customer's bill.

Settlement (`app/finalize.py`) runs only when the job is terminal **and** no ticket is
`pending`/`running`, then releases `reserved - charged`. That ordering is the whole
trick: releasing early would let a late in-flight charge push the balance under the
already-released amount.

Worked example from the demo above (3 shots):

| | cents |
|---|---|
| quote | 1050 |
| reserve (quote + 2 × 3 × 100) | 1650 |
| charged: plan 50, 3 images 300, 3 clips 600, check 20, compose 80, 3 regens 300 | 1350 |
| released | 300 |
| invariant | 1650 = 1350 + 300 |

Cancellation is the same settlement path with the remaining tickets cancelled, so the
invariant holds there too (`tests/test_cancel.py` asserts it, plus "released > 0").

### Cache

Key = SHA-256 of `(provider family, model version, prompt, seed, reference)` for images
and `(router, prompt, seed, ref)` for clips. Redis is the hot layer, Postgres the durable
layer; `SET NX` with a short TTL gives single-flight, so N identical requests produce one
provider call and N−1 zero-cost `cache_hit` ledger rows. Failures are never cached, and
regeneration seeds are deterministic, so a rerun replays the accepted regenerations too.

Measured on the identical-story rerun: `3/3` shots cached, `charged 1.50 vs quote 10.50`
(plan 0.50 + check 0.20 + compose 0.80; all renders and regens 0).

### Providers

Three mock video providers (A cheap/slow, B mid, C premium). Calls have timeouts and
retry with exponential backoff + jitter. A `402` marks the provider unavailable in Redis
for 120 s, emits exactly one `provider.unavailable` event and falls back; other errors
also fall back and mark the shot `degraded`. The shot still pays the quoted price.

`tests/test_resilience.py` asserts that with A down: every render came from B or C, the
clip charge stayed 200/shots, and total spend equals `quote + 100 × regenerations`.

### Consistency

`app/worker.py` scores each generated image against the character reference (cosine
similarity) and regenerates at most twice with a new deterministic seed when the score
is below `CONSISTENCY_THRESHOLD=0.75`. Regenerations are billed only when the fresh
image was not already cached.

The threshold is defended in [`docs/calibration.md`](docs/calibration.md), generated by
`tools/calibrate.py` (4000 labelled synthetic shots):

| class | min | p50 | max |
|---|---|---|---|
| consistent | 0.991 | 0.991 | 0.995 |
| drifted | −0.467 | −0.001 | 0.504 |

The zero-cost band on a 0.01 sweep is **0.60–0.98**, midpoint **0.74** — the shipped
default 0.75 was chosen before the script and the sweep independently lands beside it.
The mock gap (0.504 … 0.991) is unrealistically clean; the doc says so and says what to
recalibrate on real embeddings.

### Backpressure and concurrency

* **Admission** (`app/limits.py`, Redis): fixed-window 30 submissions/min per user, and
  at most 2 jobs in `queued`/`running`. Both return 429 with `Retry-After`.
  Idempotent replays are checked *before* admission, so a client retry is never punished.
* **Slots**: counting semaphores in Redis sorted sets. Acquire is a small Lua script
  (`ZREMRANGEBYSCORE` expired holders, then `ZADD` if `ZCARD < limit`), release is
  `ZREM`. Members expire after 60 s, so a killed worker cannot hold a slot forever.
  Provider HTTP calls take the `io` pool (8); compose takes the `cpu` pool (2); both are
  global across workers and reported as `pipeline_slots_in_use`.

### Events and live progress

Every state change appends to `events` in Postgres; SSE polls that log by id. A reconnect
with `Last-Event-ID` gets exactly the missing events — no gaps, no duplicates — and a
page refresh reconstructs the whole history from the same table.

## Measured numbers (the ones I will be asked about)

| Number | Where it comes from |
|---|---|
| quote 1050¢, reserve 1650¢, charged 1350¢, released 300¢ | demo run above; invariant `reserve = charged + released` |
| rerun 150¢ vs 1050¢ | `demo.py --twice`, 3/3 cache hits incl. regens |
| takeover test 41.5 s wall clock | `RUN_TAKEOVER=1 pytest tests/test_takeover.py`; lease 30 s + poll/heartbeat overhead |
| 0 regens lost / 0 double charges after kill | same test asserts unique ledger keys and `attempt > 1` |
| threshold 0.75, sweep midpoint 0.74 | `docs/calibration.md`, 4000 shots |
| residual miss after 2 regens: 9% | `DRIFT_RATE² = 0.3²` under the mock's independent-drift model |
| 30/min, 2 active, io 8, cpu 2, lease 60 s | `app/config.py`, `app/limits.py`; configurable by env |
| 10 passed, 1 skipped | `.venv/bin/pytest -v tests` |

## Deliberately simplified (and what I would do next)

Say these out loud before they are found:

- **Regeneration runs inline in `check`**, not as new tickets. Correct and idempotent,
  but it holds a ticket for up to 3 provider calls. Next: enqueue regen tickets with a
  per-job cost cap (`reserved` headroom already exists).
- **Clip cache key uses the logical router**, not the winning provider, and the clip
  says whatever provider produced it. Next: version by chosen provider when quality
  tiers must be honoured; today it is a cost optimisation.
- **Charges use quoted prices**, which keeps the customer invariant simple. Next: a
  separate provider-cost ledger for margin reporting.
- **No authentication**; `user_id` defaults to `demo`, and the top-up endpoint is
  admin-unauthenticated (labelled demo-only). Next: real auth, per-tenant keys.
- **Mock embeddings are bimodal**, so the calibration table is a method demo, not a
  production precision/recall claim.
- **Admission rate is fixed-window**, not sliding-window. Fine at 30/min; next is a
  Redis sorted-set sliding window if burst fairness matters.
- **Cancellation lets in-flight provider calls finish** (no cancellation tokens passed
  into `httpx`). Money is still exact; the only cost is a little wasted work.

## Repo map

```
app/
  main.py      routes, quote/reserve, cancel/retry, UI, /metrics route
  worker.py    claim -> heartbeat -> stage handler -> charge -> complete
  queue.py     claim / heartbeat / sweep / bury
  credits.py   reserve / charge / release_remaining
  finalize.py  terminal-and-quiet settlement
  cache.py     keys, get/put, single-flight, provider cooldown
  providers.py timeouts, retry+backoff, 402, fallback
  limits.py    admission control + leased Redis semaphores
  metrics.py   Postgres/Redis-backed Prometheus gauges
  events.py    durable event log + SSE
  schema.sql   tables, prices, demo account
mocks/main.py  planner, image, video A/B/C, check, compose, chaos
templates/     index + live job page
tests/         black-box invariants (money, cache, fallback, cancel, limits, metrics)
tools/         calibrate.py -> docs/calibration.md
compose.yaml   postgres, redis, mocks, api, worker1, worker2
run_local.py   same topology without Docker (bundled Postgres/Redis)
```

More detail on trade-offs and the exact cuts: [`docs/DECISIONS.md`](docs/DECISIONS.md),
and the component/topology/failure-mode picture: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
A 20-minute demo script with the commands and numbers to show: [`WALKTHROUGH.md`](WALKTHROUGH.md).

## Environment note

This was built on a machine without Docker, so the tests and demo numbers above were
produced with `run_local.py` (real Postgres 16 + Redis 7 via their bundled binaries, same
schema and same code paths). `compose.yaml` is the shipped topology and keeps production
shapes explicit: two named workers, healthchecks, and no `--reload`.
