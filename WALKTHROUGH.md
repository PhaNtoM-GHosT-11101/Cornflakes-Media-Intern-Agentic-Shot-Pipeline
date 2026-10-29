# Walkthrough script (20 minutes)

This is the demo script that exercises every graded behaviour, in order, with the
exact commands and the numbers you should expect to see. Each section says *what to
show* and *what to say*. Practise it once against a fresh stack.

**Before you start**

```bash
make clean            # wipe local data (fresh demo account: 100000 cents)
make local            # postgres + redis + mocks + api + 2 workers
```

`make local` runs the same code as `docker compose up`; on a Docker machine use
`docker compose up --build` instead. Open http://127.0.0.1:8000 in a browser.

---

## 0:00–2:00 — What this is (one breath each)

- **Ticket**: the unit of work is one row per `(job, stage, shot)`. Workers claim
  tickets from Postgres with `FOR UPDATE OF c SKIP LOCKED`. A crash resumes at the
  ticket level, never re-runs a job.
- **Money**: submit reserves `quote + 2 × shots × image_price` (headroom for up to two
  consistency regenerations per shot) in one conditional `UPDATE`. Every charge writes
  an append-only ledger row keyed `(job, stage, shot[, regen])` with a `UNIQUE`
  `idem_key` — crash and retry cannot double-charge. Unused reservation is released
  only when the job is terminal *and* no ticket is in flight.
- **Cache**: SHA-256 of `(provider, model, prompt, seed, reference)`; Redis hot,
  Postgres durable; `SET NX` single-flight means 20 identical requests make one
  provider call.
- **Providers**: three mock video providers; 402 marks a provider down for 120 s,
  emits one event, falls back. The customer still pays the quoted price.
- **Progress**: everything appends to a durable `events` table; the browser watches it
  over SSE with `Last-Event-ID` resume.

## 2:00–4:00 — Live job in the browser

Submit a story from the UI (or `python3 demo.py` in a second terminal) and narrate the
job page as it moves: `plan → images → clips → check → compose`. Point out:

- each stage is a ticket row with a `done` time,
- the ledger rows appearing as stages charge,
- the consistency column (`0.991`) vs the drifted shot that got regenerated.

If no regen happened, mention the mock drifts ~30% of shots and rerun with a different
story.

## 4:00–8:00 — The four money/durability demos

### a) Idempotency (T1)

```bash
curl -X POST localhost:8000/v1/jobs \
  -H 'Idempotency-Key: same-key' -H 'content-type: application/json' \
  -d '{"story":"a cat sails a tiny boat","shots":3}'
# run the SAME command again
```

Second response: same `job_id`, `"replayed": true`. Say: *the account page shows one
reservation and the ledger shows one charge per node — the retry did not create a
second job or a second charge.*

### b) Kill a worker mid-job (T2)

```bash
docker kill <worker1 container>          # or: pkill -f 'app.worker' once, locally
```

Narrate: the killed worker's lease expires after 30 s, the other worker's sweeper
requeues the ticket, the job completes. Open the job page and show the ticket's
`attempt: 2`, and the ledger with no duplicate rows. Say: *done shots are never
redone; charged shots are never recharged because the charge key already exists.*

### c) Cache rerun (T4)

```bash
python3 demo.py --twice
```

First run: `charged=13.50 vs quote 10.50` (three regenerations billed). Second run,
identical story: `3/3 cache hits`, `charged=1.50` — plan 0.50 + check 0.20 + compose
0.80. Say: *the renders and even the accepted regenerations were replayed from cache,
for free.*

### d) Provider outage (T5)

```bash
curl -X POST localhost:9000/chaos/providers/A/down
# submit a new job, watch the job page
```

The clip renders say provider B. Say: *A returned 402, we emitted exactly one
`provider.unavailable` event, and the clip still cost 2.00 — the fallback never changes
the customer's price.* Then `make chaos-reset`.

### e) Cancel mid-run (T10)

Submit a 6-shot job, cancel after ~3 s from the job page. Show: status `cancelled`,
pending tickets cancelled, the release row, and `reserve == charged + released`.

## 8:00–12:00 — The invariants as tests (2 minutes, not 4, if time is short)

```bash
.venv/bin/pytest -v tests        # 10 passed, 1 skipped
```

Read the one-line assertion from each file — these *are* the guarantees:

- `test_money.py` — replay reserves once; ledger keys unique; account debited exactly
  by charges.
- `test_cache.py` — identical rerun: images/clips zero cost, only plan+check+compose
  charged.
- `test_resilience.py` — A down ⇒ all renders from B/C, clip charge unchanged, spend =
  quote + 100 × regens.
- `test_cancel.py` — reserve == charged + released, released > 0, nothing pending.
- `test_sse.py` — reconnect with `Last-Event-ID` replays the log gap-free and
  duplicate-free.
- `test_limits.py` — third concurrent job gets 429 + `Retry-After`; idempotent replay
  still succeeds at the cap.
- `test_takeover.py` — opt-in: `RUN_TAKEOVER=1` kills a real worker; measured 41.5 s to
  finish an interrupted 5-shot job, no double charges.

## 12:00–16:00 — Code tour (4 files, 60 s each)

1. `app/queue.py` — the claim statement. One SQL query does "ready + deps done + not
   leased + lock + set lease". Heartbeat and sweep right below.
2. `app/credits.py` — `reserve` (conditional `UPDATE`, 402 if short), `charge`
   (`INSERT ... ON CONFLICT (idem_key) DO NOTHING`), `release_remaining`.
3. `app/cache.py` — key construction, `SET NX` single-flight, failures never cached,
   provider cooldown keys.
4. `app/limits.py` — the ~10-line Lua acquire: prune expired slot holders, take a slot
   if under the limit. The same "lease" idea as tickets, one layer down.

## 16:00–20:00 — Numbers you can defend

- **Threshold 0.75** — `tools/calibrate.py` sweeps 4000 labelled mock shots:
  consistent scores are 0.991–0.995, drifted are ≤ 0.504; the zero-cost band is
  0.60–0.98 with midpoint **0.74**. The shipped 0.75 was picked before the sweep and
  the sweep independently lands next to it. Say the honest part out loud: mock
  embeddings are bimodal; the *method* transfers, the exact number must be
  recalibrated on real embeddings.
- **Reservation math** — reserve 1650 = quote 1050 + 2×3×100; charged 1350 + released
  300. The invariant held on every job in the test suite.
- **Cache savings** — rerun cost 150¢ vs 1050¢ (86% cheaper).
- **Takeover** — 41.5 s wall clock (30 s lease + sweep), 0 lost shots, 0 double
  charges.
- **Backpressure** — 30/min, 2 active jobs, io 8 / cpu 2 global slots with 60 s leases.
  All env-configurable.
- **Metrics** — `/metrics` scrapes Postgres so one endpoint describes api + workers;
  worker restarts cannot reset counters.

## Likely questions, with answers

**"Why is the queue in Postgres instead of Kafka/RabbitMQ/Redis Streams?"**
Because money lives in the same transaction as the ticket: completing a ticket and
charging it must be atomic, and Postgres gives us that for free with `SKIP LOCKED`
claims. It also removes a whole failure domain. The cost is ~0.4 s polling latency,
irrelevant at this scale. (docs/DECISIONS.md #1)

**"How do you know a crash can't double-charge?"**
The ledger's `idem_key` is `UNIQUE` and the charge is `INSERT ... ON CONFLICT DO
NOTHING`. `test_money` asserts unique keys across every job, and the takeover test
kills a real worker and asserts no duplicates.

**"What if the API dies between quoting and inserting the job?"**
Nothing happened yet — the reservation and insert are one transaction, so either the
job exists with its reservation or nothing exists at all.

**"What if a worker dies after charging but before marking the ticket done?"**
The ticket gets requeued, the work is redone, the charge row is skipped (key exists),
and completion is idempotent.

**"What if the provider is down when the customer submits?"**
They get a degraded shot from a fallback provider at the quoted price. The resilience
test asserts exactly that.

**"What happens on the third failed consistency regeneration?"**
The loop caps at two (reserved headroom covers exactly that); the shot completes
flagged `inconsistent` and the customer can `POST /v1/jobs/{id}/shots/{n}/retry`.

**"What did you deliberately cut?"**
Regen runs inline in the check handler (not new tickets); clip cache key uses the
logical router; customers pay quoted prices; no auth; mock embeddings are synthetic.
All five are stated in README "Deliberately simplified" and docs/DECISIONS.md — say
them before you're asked.

## If asked to improve one thing

Pick the regeneration-into-queue change (docs/DECISIONS.md cut #1): the reservation
headroom already exists, so enqueueing regen tickets with a per-job cap is a
contained change that removes the "one ticket holds the worker through 3 calls"
behaviour.
