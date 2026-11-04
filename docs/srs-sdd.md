# Hookgate — SRS and SDD

Version 0.2 · 2026-11-02 · Status: decisions accepted

**Hookgate receives payment-provider webhooks without losing events and without processing any of them twice.**

## 0. How to read this document

Part I (sections 1–5) is the SRS: what the system must do, written so each item can be verified. Part II (sections 6–17) is the SDD: how it is built. Every requirement has an ID (FR-, NFR-, VR-) that tests, commits and the README evidence table will reference. Items tagged **\[DECISION\]** were open in v0.1 and are now resolved; section 18 records each outcome.

---

# Part I — Software Requirements Specification (SRS)

## 1. Introduction

### 1.1 Problem

Payment providers deliver webhooks at-least-once. In practice that means duplicates, delayed deliveries, reordering, and retries after our own timeouts. A naive receiver fails in two ways: it drops events (crash between receiving and processing) or it applies an effect twice (a retry races the original). Hookgate guarantees three things: **durable receipt before acknowledging**, **effectively-once effects**, and **order-safe resource state**.

### 1.2 In scope

- Signed ingestion endpoint with replay protection; raw event stored; fast 202.
- Two-layer idempotency (PostgreSQL unique key + Redis fast path) that survives a Redis outage.
- Asynchronous processing (Celery) with an explicit state machine, bounded retries with backoff and jitter, idempotent handlers.
- Out-of-order handling per resource.
- Dead-letter queue with list and replay endpoints.
- Simulated provider script (signed, duplicated, delayed, reordered, invalid, poison events).
- Observability: structured logs with trace id, `/health`, Prometheus metrics.

### 1.3 Out of scope

Web UI, real provider integrations, authentication beyond a static API key, Kubernetes, multi-tenancy, secret rotation, rate limiting.

### 1.4 Demonstration criteria (non-negotiable)

| # | Criterion | Where it is satisfied |
| --- | --- | --- |
| 1 | Solves a hard problem and says so in the first line | README first line; this document's first line |
| 2 | Tests failures on purpose | VR-3 to VR-7, VR-9 |
| 3 | Starts with one command | NFR-5 (`docker compose up`) |
| 4 | CI is green | NFR-4, section 16 |
| 5 | States honestly what it does not do | Section 17 (limitations) |

### 1.5 Glossary

- **Event**: a message from the provider with a unique `id`, a type, a target resource and an `occurred_at` timestamp.
- **Resource**: the business object an event refers to (here: a payment).
- **Delivery**: one HTTP attempt carrying an event; the same event may arrive in many deliveries.
- **Effect**: the change applied to our own state because of an event.
- **Dead event**: an event that exhausted retries or failed permanently; waits for operator action.
- **Replay**: operator-triggered reprocessing of a dead event.

## 2. Overall description

### 2.1 Context

```
Simulated provider --HTTPS+HMAC--> Hookgate API --> PostgreSQL (source of truth)
                                      |   \                ^
                                      |    +--> Redis (dedupe fast path)
                                      v                    |
                                   Broker --> Celery worker +--> payments projection
                                   Celery beat (sweeper)
Operator --X-API-Key--> /admin/events (list, inspect, replay)
```

### 2.2 Actors

- **Provider** (simulated): sends signed webhooks, at-least-once, no ordering guarantee.
- **Operator**: inspects and replays failed events using the API key.
- **Monitoring**: scrapes `/metrics`, probes `/health`.

### 2.3 Provider contract (assumed, defined by the simulator)

Header: `Hookgate-Signature: t=<unix_seconds>,v1=<hex hmac-sha256>` where the signed payload is `<t>.<raw body bytes>`. `t` is the **delivery** time, signed at each send attempt, so a legitimately delayed delivery still carries a fresh timestamp.

Body (JSON):

```
{ 'id': 'evt_123', 'type': 'payment.succeeded',
  'occurred_at': '2026-11-02T10:15:30.123456Z',
  'resource': { 'type': 'payment', 'id': 'pay_789' },
  'data': { ...full snapshot of the resource... } }
```

The provider gives no ordering guarantee and may deliver the same `id` many times. Events carry a **full snapshot** of the resource (this assumption makes ordering safe; see 13).

The snapshot carries `amount` (integer, minor units, >= 0) and `currency` (three uppercase letters). Both are validated by the handler, not at ingestion.

### 2.4 Delivery guarantee

At-least-once receipt, effectively-once effect. Hookgate does not claim exactly-once delivery; it makes repeated delivery harmless.

### 2.5 Constraints and assumptions

Python 3.12; single PostgreSQL primary; Docker Compose for local run; provider clocks are roughly correct (see limitations).

## 3. Functional requirements

### 3.1 Ingestion (FR-ING)

| ID | Requirement |
| --- | --- |
| FR-ING-1 | `POST /webhooks/payments` reads the raw body and verifies HMAC-SHA256 over `<t>.<raw body>` with the shared secret, using constant-time comparison. |
| FR-ING-2 | Missing, malformed or invalid signature → 401; nothing is persisted; a rejection metric and log are emitted. |
| FR-ING-3 | Timestamp `t` outside the tolerance window (default 300 s, configurable) → 401 `timestamp_out_of_tolerance`, same handling as FR-ING-2. |
| FR-ING-4 | Body that is not valid JSON or lacks required fields → 400; body over 1 MiB → 413. |
| FR-ING-5 | The raw body bytes, relevant headers, trace id and receive time are persisted before responding. |
| FR-ING-6 | Success responds `202 Accepted` without waiting for processing. |
| FR-ING-7 | If publishing to the broker fails, the endpoint still responds 202; the stored event is recovered by the sweeper (FR-PRO-7). |

### 3.2 Idempotency (FR-IDE)

| ID | Requirement |
| --- | --- |
| FR-IDE-1 | An event's identity is `(provider, event_id)`. |
| FR-IDE-2 | PostgreSQL enforces a unique constraint on that identity and is the source of truth. |
| FR-IDE-3 | Redis holds a short-lived marker for known events; a hit short-circuits the database write. **\[DECISION\]** TTL 24 h. |
| FR-IDE-4 | If Redis is down, slow or errors, ingestion degrades to PostgreSQL only; the provider sees no error; a metric and log record the degradation. |
| FR-IDE-5 | A duplicate delivery responds `202` with body `{'status': 'duplicate'}` and does not enqueue a second processing task. |
| FR-IDE-6 | A duplicate id with a different body keeps the original, logs a warning and increments a `payload_mismatch` counter. |

### 3.3 Processing (FR-PRO)

| ID | Requirement |
| --- | --- |
| FR-PRO-1 | Events move through `received → processing → processed / failed / dead` exactly as defined in section 9. |
| FR-PRO-2 | Claiming an event for processing is an atomic compare-and-set; two workers can never process the same event concurrently. |
| FR-PRO-3 | Handlers are idempotent: the effect and the state transition commit in one database transaction. |
| FR-PRO-4 | Retries are bounded (default 5 attempts) with exponential backoff and full jitter (base 2 s, cap 300 s). |
| FR-PRO-5 | Errors are classified: transient errors retry; permanent errors (validation, unknown type) go to `dead` immediately. |
| FR-PRO-6 | The attempt counter increments at claim time, so a payload that crashes the worker still ends in `dead`. |
| FR-PRO-7 | A sweeper re-enqueues: `received` events older than 30 s, `failed` events past `next_attempt_at`, and `processing` events whose lease expired. |
| FR-PRO-8 | Every state change is recorded in an audit table with reason and trace id. |

### 3.4 Ordering (FR-ORD)

| ID | Requirement |
| --- | --- |
| FR-ORD-1 | Ordering key per resource is `(occurred_at, event_id)`. **\[DECISION\]** |
| FR-ORD-2 | An event older than the last applied one for its resource ends `processed` with outcome `ignored_stale` and no effect. |
| FR-ORD-3 | Events of the same resource are serialized with a row lock; events of different resources run in parallel. |
| FR-ORD-4 | Final resource state does not depend on arrival order. |

### 3.5 Dead-letter queue (FR-DLQ)

| ID | Requirement |
| --- | --- |
| FR-DLQ-1 | `GET /admin/events?status=dead\|failed` lists events with cursor pagination. |
| FR-DLQ-2 | `GET /admin/events/{id}` returns the stored raw payload, state, error and attempt history. |
| FR-DLQ-3 | `POST /admin/events/{id}/replay` moves a `dead` event back to `received`, resets attempts and clears the retry schedule and lease (history is kept) and enqueues it; non-dead events → 409. |
| FR-DLQ-4 | Concurrent replay calls for one event enqueue it once. |
| FR-DLQ-5 | (Backlog, not in the MVP) Bulk replay by filter. |

### 3.5b Simulator (FR-SIM)

| ID | Requirement |
| --- | --- |
| FR-SIM-1 | `python -m simulator run --scenario <name> --seed <n>` sends events deterministically. |
| FR-SIM-2 | Scenarios cover: happy path, duplicates, delayed, out-of-order, burst of simultaneous duplicates, bad signature, expired timestamp, poison payload; plus chaos, a composite that runs every scenario above in one seeded sequence. |
| FR-SIM-3 | The simulator knows the expected final state and offers `verify` to compare it against the API. |

### 3.6 Observability (FR-OBS)

| ID | Requirement |
| --- | --- |
| FR-OBS-1 | JSON logs include `trace_id`, `event_id`, `state`. Trace id comes from `X-Trace-Id` or is generated, is stored on the event and propagated to the worker. |
| FR-OBS-2 | `GET /health`: liveness always 200 if the process runs; readiness reports `db`, `broker`, `redis`. Redis down → `degraded` (200); database down → 503. |
| FR-OBS-3 | `GET /metrics` exposes the counters in section 15. |

### 3.7 Security (FR-SEC)

| ID | Requirement |
| --- | --- |
| FR-SEC-1 | `/admin/*` requires `X-API-Key` (constant-time comparison); ingestion relies only on the HMAC. |
| FR-SEC-2 | Secrets come from environment variables and are never logged; raw bodies are not logged. |

## 4. Non-functional requirements

| ID | Requirement |
| --- | --- |
| NFR-1 | Reliability: no acknowledged event is lost under API crash after commit, worker SIGKILL, broker outage, or Redis outage. |
| NFR-2 | Latency: ingest p95 under 100 ms with Redis up and under 150 ms with Redis down, measured on a dev laptop and reported (target, not a guarantee). |
| NFR-3 | Throughput: measured and reported in the README; no promise beyond the measurement. |
| NFR-4 | Quality gates in CI: `ruff check`, `ruff format --check`, `mypy --strict`, `pytest` with coverage of at least 80% on the domain package. |
| NFR-5 | `docker compose up --build` starts every service, applies migrations and exposes the API. |
| NFR-6 | Python 3.12, SQLAlchemy 2.0 style, Alembic migrations. |
| NFR-7 | The domain layer has no I/O imports (checked in CI). |
| NFR-8 | README opens with the problem, shows an evidence table, and lists limitations. |

## 5. Verification matrix

| ID | Test | Covers |
| --- | --- | --- |
| VR-1 | Hypothesis: the same event delivered N times, in any order, produces exactly one effect and one stored row. | FR-IDE-1/2/5, FR-PRO-3 |
| VR-2 | Hypothesis: any permutation of a resource's events yields the same final state (that of the latest event). | FR-ORD-1/2/4 |
| VR-3 | Redis taken down in the middle of a delivery sequence: no errors to the provider, no duplicate effect, metric incremented. | FR-IDE-4, NFR-1 |
| VR-4 | Worker killed with SIGKILL mid-task: event recovered by the sweeper, effect applied once. | FR-PRO-2/6/7, NFR-1 |
| VR-5 | Handler fails transiently N times then succeeds: ends `processed`, attempts recorded, backoff respected. | FR-PRO-4/5 |
| VR-6 | Poison payload: ends `dead`, visible in the list endpoint, replayable. | FR-PRO-5/6, FR-DLQ-1 to 3 |
| VR-7 | 32 simultaneous deliveries of one event: one row, one enqueue, one effect. | FR-IDE-2, FR-PRO-2 |
| VR-8 | Signature and timestamp: unit and property tests (tampered body, wrong secret, skew edges). | FR-ING-1 to 3 |
| VR-9 | Broker down at ingestion time: 202 returned, event processed after the broker returns. | FR-ING-7, FR-PRO-7 |
| VR-10 | Replay endpoint: state, 409 cases, concurrent replay. | FR-DLQ-3/4 |
| VR-11 | Retry policy property: delay always within `[0, min(cap, base*2^(n-1))]`. | FR-PRO-4 |
| VR-12 | State machine property: random trigger sequences never reach an illegal transition. | FR-PRO-1 |

---

# Part II — Software Design Description (SDD)

## 6. Architecture

### 6.1 Layers

- **domain** (pure, no I/O): state machine, retry policy, ordering rule, signature verification, error taxonomy, event and payment models.
- **application**: use cases `ingest_event`, `process_event`, `sweep`, `replay_event`; ports (repository, cache, queue, clock, rng).
- **infrastructure**: SQLAlchemy repositories, Redis cache, Celery tasks, structlog and Prometheus wiring.
- **api**: FastAPI routers, dependencies, error mapping.

The domain package is the 80% coverage target; keeping it pure makes that target meaningful.

### 6.2 Ingestion flow

```
1. read raw body  2. verify signature + timestamp  3. parse minimal fields
4. Redis GET key -> hit? return 202 duplicate   (errors ignored)
5. INSERT ... ON CONFLICT (provider, event_id) DO NOTHING RETURNING id
6. conflict? -> 202 duplicate     inserted? -> COMMIT
7. Redis SET key (after commit, best effort)
8. broker publish (best effort; failure is logged, sweeper recovers)
9. 202 accepted
```

### 6.3 Processing flow

```
worker receives task(event_id)
  claim: UPDATE ... SET status='processing', attempts=attempts+1, locked_until=now()+lease
         WHERE id=:id AND status IN ('received','failed') AND (next_attempt_at IS NULL OR next_attempt_at<=now())
         RETURNING *      -- 0 rows -> someone else owns it or it is done: ack and exit
  BEGIN
    upsert resource row; SELECT ... FOR UPDATE
    compare ordering key -> apply or ignore_stale
    insert applied_effects(event_id)
    set status='processed', outcome
  COMMIT
  on TransientError: status='failed', next_attempt_at=now()+jittered backoff (or 'dead' if attempts>=max)
  on PermanentError: status='dead'
```

## 7. Technology and decisions (ADR index)

| ADR | Decision | Rationale |
| --- | --- | --- |
| 001 | PostgreSQL is the source of truth; the events table doubles as the outbox. | Removes the dual-write problem: an event is durable before any queue is touched. |
| 002 | **\[DECISION\]** Broker: RabbitMQ chosen over Redis (accepted). | With Redis as both cache and broker, a Redis outage also stops enqueueing, which blurs the failure test. RabbitMQ isolates the cache failure; cost is one more compose service. |
| 003 | Redis is only a dedupe fast path, written after commit. | A marker can never exist for an event that was not persisted. |
| 004 | DLQ is a status on the same table, not a separate table. | Replay becomes a state transition with an audit row; no copying. |
| 005 | **\[DECISION\]** Sync SQLAlchemy 2.0 with psycopg 3 everywhere (accepted); async API left as a follow-up. | Celery is synchronous; one session style halves the surface. FastAPI runs `def` endpoints in a threadpool. Async can be a documented follow-up. |
| 006 | Ordering key `(occurred_at, event_id)`. | Total order even when timestamps tie; deterministic. |
| 007 | Lease plus sweeper instead of relying on broker redelivery. | Recovery logic lives in our database and is testable without a specific broker behavior. |
| 008 | Attempts increment at claim, not at failure. | A crash-looping poison payload cannot retry forever. |
| 009 | Layered structure with a pure domain. | Meaningful coverage threshold; fast Hypothesis tests. |
| 010 | Celery config: `acks_late=True`, `reject_on_worker_lost=True`, `prefetch_multiplier=1`. | Tasks are re-delivered after worker death; duplicates are harmless because of the claim CAS. |
| 011 | The pure transition() function validates; SQL applies. | transition() decides the target state and which columns are cleared; the repository applies it with a compare-and-set on the current status. Entering processed or dead clears next\_attempt\_at and locked\_until. The next\_attempt\_at guard lives only in the claim SQL because it needs the database clock. Rules are not duplicated. |

## 8. Data model

**webhook\_events**

| Column | Type | Notes |
| --- | --- | --- |
| id | uuid PK | internal id |
| provider | text | `UNIQUE (provider, event_id)` |
| event\_id | text | provider's id |
| event\_type | text |  |
| resource\_type, resource\_id | text | indexed together |
| occurred\_at | timestamptz | ordering key part 1 |
| received\_at | timestamptz |  |
| raw\_body | bytea | exactly as received |
| headers | jsonb | allow-listed headers only |
| status | enum | received, processing, processed, failed, dead |
| outcome | enum null | applied, ignored\_stale |
| attempts | int | incremented at claim |
| next\_attempt\_at | timestamptz null |  |
| locked\_until | timestamptz null | lease |
| last\_error | text null |  |
| trace\_id | text |  |
| processed\_at | timestamptz null |  |

Indexes: `(status, next_attempt_at)` for the sweeper, `(status, received_at)` for listing.

**event\_transitions** (audit): `id, event_id FK, from_status, to_status, reason, trace_id, at`.

**event\_attempts**: `id, event_id FK, attempt_no, started_at, finished_at, result, error`.

**payments** (projection): `resource_id PK, status, amount, currency, last_occurred_at, last_event_id, updated_at`.

**applied\_effects**: `event_id PK FK` — written in the same transaction as the projection; a second insert for the same event fails, which makes a double effect structurally impossible.

Alembic owns the schema; migrations run in a one-shot compose service before the API starts.

## 9. State machine

| From | To | Trigger | Guard / action |
| --- | --- | --- | --- |
| (new) | received | ingestion commit | unique identity |
| received | processing | worker claim | CAS; attempts+1; lease set |
| failed | processing | worker claim | `next_attempt_at <= now`; attempts+1 |
| processing | processed | handler success | outcome = applied or ignored\_stale |
| processing | failed | transient error or lease expiry | attempts < max; set `next_attempt_at` |
| processing | dead | permanent error; transient error or lease expiry with attempts >= max | store `last_error` |
| dead | received | operator replay | attempts reset to 0; history kept |

Terminal: `processed`. Forbidden: any transition out of `processed`; `dead → processing` (must pass through `received`).

The transition table is a pure domain function `transition(status, trigger, *, attempts, max_attempts) -> TransitionResult` that raises `InvalidTransition`. The result names the target status and what happens to `attempts`, `locked_until` and `next_attempt_at` (ADR-011). The application layer persists the result and writes the audit row in the same transaction.

## 10. Ingestion design

**Signature.** `expected = HMAC_SHA256(secret, f'{t}.'.encode() + raw_body)`; compare with `hmac.compare_digest`. The raw body is read before any JSON parsing, so re-serialization can never change the signed bytes. Timestamp check: `abs(now - t) <= tolerance`. Clock is injected for tests.

**Parsing.** Only the fields needed for identity and ordering are validated at ingestion (`id`, `type`, `occurred_at`, `resource.type`, `resource.id`). Business validation happens in the handler, so a semantically bad event is still stored and can reach the dead-letter queue instead of being rejected and lost.

**Insert.**

```sql
INSERT INTO webhook_events (...) VALUES (...)
ON CONFLICT (provider, event_id) DO NOTHING
RETURNING id;
```

No row returned means duplicate. On duplicate, the stored raw body is compared byte for byte with the incoming one to detect FR-IDE-6 (no hash column is needed).

**HTTP contract (phase F3).** Checks run in this order: body size (413 above 1 MiB), signature and timestamp (401 with `missing_signature`, `malformed_signature`, `invalid_signature` or `timestamp_out_of_tolerance`), minimal envelope (400 `invalid_payload`, listing field paths but never echoing values). `occurred_at` must carry a timezone. Stored headers are limited to `content-type`, `user-agent`, `hookgate-signature` and `x-trace-id`. The trace id is taken from `X-Trace-Id` when it matches `[A-Za-z0-9._:-]{1,128}`, otherwise generated, and is echoed in the response. The single provider is identified as `sim`. Publishing happens only after the insert commits; a publish failure is logged and the response is still 202.

## 11. Idempotency design

Two layers with distinct roles: **PostgreSQL decides, Redis only saves work.**

| Situation | Behavior |
| --- | --- |
| Redis healthy, event known | GET hits, return duplicate without touching PostgreSQL. |
| Redis healthy, event new | GET misses, INSERT, commit, SET marker. |
| Redis down or timing out | GET/SET raise; caught, metric `cache_errors_total` incremented, flow continues with PostgreSQL. Socket timeouts are short (50 ms) so an outage does not add latency. |
| Marker evicted or expired | GET misses, INSERT conflicts, duplicate detected by PostgreSQL. |
| Crash after commit, before SET | Marker missing; next duplicate is caught by PostgreSQL. |
| Marker exists but row missing | Cannot happen by construction (SET after commit); only after a database restore, where a redelivered event is treated as a duplicate and never re-inserted until its marker expires, a lost event that the provider's next retry cannot fix. Runbook: flush the Redis dedupe keys after any restore. Documented as a limitation. |

**Implementation notes (phase F4).** Marker key `hookgate:dedupe:<provider>:<event_id>`, value `1`, TTL `DEDUPE_TTL_S`. The Redis client uses `REDIS_SOCKET_TIMEOUT_S` for connect and read and has client retries disabled: redis-py retries connection and timeout errors by default with backoff, which would add seconds per delivery during an outage. The marker is written after the commit for accepted events and also when PostgreSQL detects a duplicate (the row exists, so re-marking is safe). Limitation: a duplicate short-circuited by a Redis hit is not compared with the stored body, so FR-IDE-6 is detected only on the PostgreSQL path. Until the Prometheus adapter (phase F9), cache errors are counted through the `IngestionMetrics` port in tests and logged as `cache_error` in the running service.

**Effect idempotency** is separate from receipt idempotency: even if two deliveries somehow reached processing, `applied_effects.event_id` is a primary key written in the same transaction as the projection, and the claim CAS prevents concurrent processing.

## 12. Processing design

**Retry policy** (pure, injectable RNG and clock):

```
delay(n) = rng.uniform(0, min(cap, base * 2 ** (n - 1)))   # full jitter
```

Defaults: `max_attempts=5`, `base=2 s`, `cap=300 s`. The next attempt time is stored in the database and also passed as the Celery `eta`; the database value is authoritative.

**Error taxonomy.** `TransientError` (timeouts, connection errors, deadlocks, simulated flaky dependency) vs `PermanentError` (schema violation, unknown event type, business rule failure). Any unclassified exception is treated as transient until attempts run out.

**Lease and sweeper.** The claim sets `locked_until = now + lease` (default 60 s). Celery beat runs the sweeper every 15 s. It selects with `FOR UPDATE SKIP LOCKED`: `received` older than 30 s; `failed` with `next_attempt_at <= now`; `processing` with `locked_until < now` (treated as a transient failure through the same state machine, so attempts and backoff still apply). A worker killed mid-task therefore leaves the event in `processing`, and the sweeper recovers it after the lease expires.

**Duplicate task messages are expected** (acks\_late redelivery, sweeper overlap). The claim CAS makes them no-ops. The sweeper changes state inside its transaction and publishes to the broker only after commit; a failed publish is retried on the next sweep.

**Implementation notes (phase F5).** Processing uses three short transactions: (1) claim plus an open `event_attempts` row, committed before any work; (2) the effect: the `processing -> processed` transition first (compare-and-set on status *and* `attempts`, which locks the row and proves the claim is still ours), then the projection and `applied_effects`, all committed together; (3) only if (2) raised, the failure: a separate transaction records `failed` with `next_attempt_at = now() + delay` or `dead`, and closes the attempt row. A worker whose lease expired and whose event was claimed again sees its compare-and-set fail and writes nothing (outcome `lost`). Time decision (P3, confirmed): PostgreSQL's `now()` is the only clock for leases and retry times; the jittered delay is drawn in Python and added in SQL. Tests move time by back-dating `next_attempt_at` and `locked_until`; chaos tests will use `LEASE_S=2`.

**Implementation notes (phase F7).** The Celery task only delegates to `process_event`. When a retry was scheduled, the task also sends a delayed message (`countdown` = backoff + 0.5 s) instead of an absolute `eta`; `next_attempt_at` in the database stays authoritative and an early message is a skipped claim. The sweeper (ADR-007) selects with `FOR UPDATE SKIP LOCKED` in batches of 500, uses a 30 s grace period for `received`, closes attempt rows left open by a dead worker as `lease_expired`, and publishes after commit. Publishing is bounded: one connection attempt, plus one immediate reconnect when a previously working connection is dead, so producers heal after a broker restart (ADR-002). Worker pool: threads (ADR-012). ADR-010 settings are in `infrastructure/celery_app.py`.

## 13. Ordering design

Inside the processing transaction:

1. `INSERT INTO payments (resource_id, ...) ON CONFLICT DO NOTHING` (ensures the row exists).
2. `SELECT ... FROM payments WHERE resource_id = :r FOR UPDATE` (serializes same-resource events).
3. If `(event.occurred_at, event.event_id) <= (last_occurred_at, last_event_id)` → outcome `ignored_stale`, no write.
4. Else apply the snapshot from the event, update `last_*`, outcome `applied`.

Because every event carries a full snapshot, applying only the latest one gives the same final state as applying all of them in order, so arrival order cannot change the result (VR-2). If events were deltas this would not hold; that is recorded as a design assumption and limitation.

**Implementation notes (phase F6).** A projection row created by step 1 has NULL `last_*` columns; the domain rule treats NULL as "no event applied yet", so the first event of a resource is always applied (P5). The comparison runs in Python with the pure `is_stale` rule, so it never depends on the database collation. Every transaction locks the payment row first and the event row second, a fixed order that rules out lock cycles between workers. A stale event still has its snapshot validated (an invalid one ends `dead`); a valid stale event ends `processed` with outcome `ignored_stale` and writes no `applied_effects` row.

Event types for the demo domain: `payment.created`, `payment.authorized`, `payment.succeeded`, `payment.failed`, `payment.refunded`. The handler maps each to a `payments.status`; an unknown type is a `PermanentError`.

## 14. Dead-letter and replay design

| Method and path | Auth | Behavior |
| --- | --- | --- |
| `GET /admin/events` | API key | Filters: `status`, `resource_id`, `event_type`; cursor pagination on `(received_at, id)`. |
| `GET /admin/events/{id}` | API key | Event detail with raw payload, transitions and attempts. |
| `POST /admin/events/{id}/replay` | API key | In one transaction: CAS `dead → received`, reset attempts, set next\_attempt\_at and locked\_until to NULL, audit row with reason `operator_replay`. Enqueue after commit. 409 if not `dead`. |

Replay is safe to repeat: the CAS succeeds once, so concurrent calls enqueue once.

## 15. Simulator and observability design

### 15.1 Simulator scenarios

| Scenario | What it sends | Expected result |
| --- | --- | --- |
| happy | ordered, unique events | all processed |
| duplicates | each event 1–5 times | one row per event; duplicates counted |
| delayed | events held and sent later with a fresh signature | all processed |
| out\_of\_order | per-resource events shuffled | final state equals latest event |
| burst | one event, 32 concurrent deliveries | one row, one effect |
| bad\_signature / expired | tampered or old timestamp | 401, nothing stored |
| poison | payload with unknown type | `dead`, listed, replayable |

Built with `httpx`, seeded RNG, and an expected-state oracle used by the verify command. The chaos scenario is the composite: every scenario above, interleaved by seed, then `verify`.

### 15.2 Metrics (Prometheus)

| Metric | Type | Labels |
| --- | --- | --- |
| hookgate\_events\_received\_total | counter | outcome=accepted\|duplicate\|rejected |
| hookgate\_duplicates\_total | counter | layer=redis\|postgres |
| hookgate\_events\_processed\_total | counter | outcome=applied\|ignored\_stale |
| hookgate\_events\_retried\_total | counter |  |
| hookgate\_events\_dead\_total | counter | reason |
| hookgate\_cache\_errors\_total | counter | op=get\|set |
| hookgate\_processing\_seconds | histogram |  |
| hookgate\_events\_by\_status | gauge | status (computed from the database at scrape time) |

The API serves `/metrics`; the worker exposes its own metrics port. Counters that change in the worker are incremented there.

### 15.3 Logging

`structlog` JSON with `trace_id`, `event_id`, `resource_id`, `status`, `attempt`. The trace id is stored on the event row and sent as a Celery header so API and worker lines correlate. Raw bodies and secrets are never logged.

### 15.4 Health response

```
{ 'status': 'ok' | 'degraded' | 'down',
  'checks': { 'db': 'ok', 'broker': 'ok', 'redis': 'down' } }
```

## 16. Project structure, run and CI

```
hookgate/
  src/hookgate/{domain,application,infrastructure,api}/
  simulator/
  migrations/            # Alembic
  tests/{unit,property,integration,chaos}/
  docs/                  # this document, ADRs
  docker-compose.yml  Makefile  pyproject.toml
  .github/workflows/ci.yml
```

**Run.** `docker compose up --build` starts postgres, redis, broker, a one-shot migration job, api, worker and beat. `docker compose --profile demo run simulator --scenario chaos` runs the demo. `make test`, `make lint`, `make chaos` wrap the local commands.

**CI (GitHub Actions).**

| Job | Steps |
| --- | --- |
| lint | `ruff check`, `ruff format --check` |
| types | `mypy --strict` |
| test | service containers for postgres, redis, broker; `pytest --cov=hookgate.domain --cov-fail-under=80` |
| chaos | separate job for tests that kill processes or stop services |
| smoke | `docker compose up -d`, run simulator scenarios, `verify` |

Import rule check (domain must not import infrastructure or framework code) runs in the lint job.

## 17. Test strategy and honest limitations

### 17.1 How the hard tests work

- **Hypothesis (VR-1, VR-2).** Strategy draws events from a small id pool to force duplicates, uses `st.permutations` and repeat counts, runs through the application use cases against a real database, truncating the tables at the start of each example (the claim and the transitions need real commits, so rolled-back transactions cannot be used), with processing invoked directly (no broker) for speed. Assertions: row count equals distinct ids, `applied_effects` count equals distinct applied events, final projection equals the oracle (latest event per resource).
- **Redis outage (VR-3).** Unit level with a `FaultyCache` that raises after N calls; integration level stops the Redis container mid-sequence and restarts it.
- **Worker kill (VR-4).** Start a real worker subprocess; a test-only handler hook blocks and signals through a file; the test sends SIGKILL, starts a new worker, advances past the lease, and asserts a single effect.
- **Transient and poison (VR-5, VR-6).** A configurable failing handler with an injected clock and RNG; assertions on delays, attempts, audit rows and final status.
- **Concurrency (VR-7).** 32 threads released by a barrier hit the endpoint with the same signed event; assertions on row count, enqueue count and effect count.

### 17.2 Limitations (to be copied into the README)

- Not exactly-once delivery; effectively-once effects under the stated assumptions.
- Ordering relies on provider-supplied `occurred_at` and full-snapshot events; skewed provider clocks or delta events break the guarantee.
- Single PostgreSQL primary; no sharding or failover handling.
- A restored database backup can leave Redis markers for events the restored database no longer has, so redelivered events are dropped as duplicates until the markers expire; flush the Redis dedupe keys after any restore.
- One shared secret; no rotation. Static API key only.
- No rate limiting, backpressure or retention policy for stored raw bodies.
- The provider is simulated; real provider quirks are not covered.
- Throughput and latency numbers come from a single laptop and are indicative only.
- Dead events require a human to replay them; there is no alerting.

## 18. Decisions and next steps

| # | Decision | Outcome (all accepted 2026-11-02) |
| --- | --- | --- |
| D1 | Broker: RabbitMQ or Redis | RabbitMQ (ADR-002) |
| D2 | Sync or async SQLAlchemy | Sync with psycopg 3 (ADR-005) |
| D3 | Ordering key | `(occurred_at, event_id)` (FR-ORD-1) |
| D4 | Redis marker TTL | 24 h (FR-IDE-3) |
| D5 | Timestamp tolerance | 300 s |
| D6 | Event types in the demo domain | The five listed in section 13 |
| D7 | Language of the docs and README | English, to match code and commits |

**Suggested milestones.** M0 skeleton, CI and compose; M1 signed ingestion; M2 two-layer idempotency; M3 processing and state machine; M4 ordering; M5 dead-letter and replay; M6 simulator and observability; M7 failure and concurrency suites; M8 README with evidence table and limitations. Each milestone ends with its VR tests green in CI. Within each milestone, the Hypothesis, failure and concurrency tests (VR-1 to VR-7 and VR-9) are written first. Backlog: bulk replay (FR-DLQ-5); the events-by-status gauge is the last observability task.
