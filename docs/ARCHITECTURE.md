# Architecture

A single diagram first, then the tables, flows and failure modes. Everything is
mock-provider based; nothing here requires a real AI API.

## Topology

```
                        ┌─────────────┐   SSE (Last-Event-ID)   ┌──────────┐
                        │   browser   │◄────────────────────────│   api    │
                        └─────────────┘                         │ (uvicorn)│
                                                      ┌───────► │  :8000   │
   client ──HTTP 202/429──► api ──SQL──► PostgreSQL 16 │         └────┬─────┘
                                                      │              │ SQL
                                        jobs          │         ┌────▼─────┐
                                        tickets       │         │ worker1  │  poll every
                                        shots         │         │ worker2  │  0.4 s,
                                        ledger        └─────────│          │  claim via
                                        events                  └────┬─────┘  SKIP LOCKED
                                        content_cache               │ HTTP (io pool ≤ 8)
                                        accounts                   ┌▼──────────┐
                                        price_table                │   mocks   │
                                                      Redis 7 ◄────│  :9000    │
                                                      cache / cooldowns /      │
                                                      rate / semaphores        └──────────┘
```

- **Postgres** is the system of record: queue, money, events, durable cache.
- **Redis** is coordination and speed: hot cache, provider cooldowns, rate counters,
  cross-process semaphores. Everything in Redis can be rebuilt from Postgres or
  time.
- **Two workers** (`worker1`, `worker2`) poll the same ticket table; the claim query
  guarantees no two workers ever hold the same ticket.

## Data model (the important columns)

| Table | Why it exists | Key columns |
|---|---|---|
| `jobs` | one row per submitted film | `id`, `user_id`, `idem_key UNIQUE`, `status`, `reserved_cents`, `finalize_ok` |
| `tickets` | the real unit of work | `id`, `job_id`, `stage`, `shot`, `deps[]`, `state`, `attempt`, `owner`, `lease_expires_at`, `result` |
| `shots` | per-shot output and quality | `job_id`, `idx`, `provider`, `consistency`, `regen_count`, `cache_hit`, `degraded` |
| `ledger` | append-only money | `job_id`, `kind` (reserve/charge/cache_hit/release), `amount_cents`, `idem_key UNIQUE` |
| `events` | durable progress log | `id`, `job_id`, `type`, `payload jsonb` — SSE and the UI read only this |
| `content_cache` | durable content layer | `key` (SHA-256), `payload`, `created_at` |
| `accounts` | balances | `user_id`, `balance_cents`, `reserved_cents` |
| `price_table` | quoted prices | `stage`, `model`, `unit_cents`, `version` |

`tickets.deps` points at ticket ids that must be `done` first, so the pipeline is a
persisted graph: plan → images → clips → check → compose.

## Flows

### Submit (api)

```
1. same idem key?             → return the existing job (202, replayed: true)
2. rate window < 30/min?      → else 429 + Retry-After
3. active jobs < 2?           → else 429 + Retry-After
4. quote = price_table; reserve = quote + 2 × shots × image_price
5. one transaction:
     UPDATE accounts SET reserved_cents = reserved_cents + $reserve
       WHERE balance_cents - reserved_cents >= $reserve   -- else 402
     INSERT jobs, shots, tickets (with deps), ledger reserve row
```

The reservation and the job exist or neither does. Idempotency is checked *before*
admission so a client retry is never throttled.

### Worker loop

```
loop:
  claim one ready ticket      → FOR UPDATE OF c SKIP LOCKED, set 30 s lease
  heartbeat every 10 s
  run the stage (io pool for provider calls, cpu pool for compose)
  charge the ticket           → INSERT ledger ON CONFLICT (idem_key) DO NOTHING
  complete the ticket
  maybe_finalize the job
```

The charge is keyed `charge:{job}:{stage}:{shot}` (regens add `regen:{job}:{shot}:{n}`),
so requeued work never bills twice. `maybe_finalize` releases `reserved − charged`
only when the job is terminal and nothing is in flight.

### Consistency check (inside `check`)

```
score = cosine(character reference, generated embedding)
if score < 0.75 and regen_count < 2:
    regenerate with deterministic seed 1000 + idx + 100 × (attempt + 1)
    bill the regeneration only if the fresh image was not already cached
```

Two regens per shot is exactly what the reservation headroom covers.

### Cancel

```
transaction: job → cancelled; pending tickets → cancelled
in-flight tickets finish normally (and still charge)
finalize: releases the unused reservation (reserve == charged + released)
```

### SSE / progress

Every state change appends to `events`. The browser opens
`GET /v1/jobs/{id}/events` (SSE), and on reconnect sends `Last-Event-ID`; the stream
replays `WHERE id > last`. A page refresh rebuilds history from the same table.

## Failure modes and the answer to each

| Failure | Behaviour |
|---|---|
| Worker killed mid-ticket | lease expires (30 s), sweeper requeues, another worker resumes; done shots skipped, charges idempotent |
| API killed mid-submit | the reservation+insert transaction never committed; nothing exists |
| Worker killed between charge and complete | ticket requeued, work redone, charge skipped by `idem_key` |
| Provider 402 | provider marked down 120 s in Redis, one event, fallback A→B→C; customer pays quoted price |
| Provider timeout/500 | retry with backoff+jitter, then fallback, shot marked `degraded` |
| Redis lost | hot cache misses re-fill from Postgres `content_cache`; cooldowns/rate counters reset (safe defaults) |
| Postgres lost | everything is gone — but the whole system state was there, which is the point of a system of record; backups are the next layer (out of scope) |
| Duplicate submit (same key) | same job, `replayed: true`, no second reservation |
| Over-budget account | reserve fails closed with 402 before any work starts |
| Third concurrent job | 429 + `Retry-After`; idempotent replays still allowed |

## Concurrency budget

| Pool | Limit | Held during | Mechanism |
|---|---|---|---|
| active jobs / user | 2 | whole job | DB count, checked at submit |
| submissions / user | 30/min | — | Redis fixed window |
| `io` slots | 8 (global) | provider HTTP calls | Redis sorted-set semaphore, 60 s lease |
| `cpu` slots | 2 (global) | compose/transcode | same mechanism |

All limits are env-configurable and visible in `/metrics` (`pipeline_slots_in_use`,
`pipeline_queue_depth`, …).
