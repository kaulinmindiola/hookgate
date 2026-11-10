# ADR-007: Lease plus sweeper instead of relying on broker redelivery

Status: accepted (SDD v0.2) · Implemented in phases F5 (lease) and F7 (sweeper)

## Context

A worker can die after claiming an event (SIGKILL, OOM, node loss). `acks_late` makes the broker redeliver the message, but that depends on one broker's behavior and does not cover messages that were never published (broker down at ingestion, FR-ING-7).

## Decision

Recovery lives in the database. The claim sets `locked_until = now() + LEASE_S`. A sweeper, scheduled by Celery beat every `SWEEP_INTERVAL_S`, runs one short transaction with `FOR UPDATE SKIP LOCKED`:

- `processing` with an expired lease goes through the state machine as `lease_expired` (`failed` with backoff, or `dead` once `attempts` reached the maximum) and the attempt row the worker left open is closed as `lease_expired`;
- `received` older than 30 s and `failed` whose `next_attempt_at` has passed are selected for publishing.

It publishes only after the commit. A failed publish is retried on the next sweep; a duplicate message is a no-op because the claim is a compare-and-set.

## Time (decision P3)

PostgreSQL's `now()` is the only clock for leases and retry times. Tests never move a clock: they back-date `locked_until`, `next_attempt_at` or `received_at`. The chaos suite (F11) uses `LEASE_S=2` to make real expiry fast.

## Consequences

- Recovery is testable without a specific broker: `tests/integration/test_sweep.py` covers a crashed worker (effect applied once, attempts `[lease_expired, processed]`) and a broker outage at ingestion (VR-9 at application level).
- The chaos suite (F11) checks the same recovery with real processes: a worker killed with SIGKILL while it holds a claim (`tests/chaos/test_worker_kill.py`, `LEASE_S=2`) and a stopped RabbitMQ (`tests/chaos/test_broker_outage.py`).
- A crashed event is retried after its lease plus its backoff, not immediately.
- Concurrent sweepers split the work instead of waiting on each other (`SKIP LOCKED`).
- Retries also get a delayed task message (`countdown` = backoff + 0.5 s) so they run on time; the database value stays authoritative, and an early message is just a skipped claim.

## Alternatives considered

- Broker redelivery only: no recovery for never-published events; behavior differs between brokers.
- Long-running transaction holding the row lock while the handler runs: a crash releases the lock, but long transactions block vacuum and hold connections.