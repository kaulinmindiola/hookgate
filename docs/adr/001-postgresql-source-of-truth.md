# ADR-001: PostgreSQL is the source of truth; the events table is the outbox

Status: accepted (SDD v0.2) · Implemented in phase F2

## Context

Publishing to a queue and writing to a database cannot be one atomic operation (the dual-write problem). If the process dies between the two, an acknowledged event is lost.

## Decision

An event is durable in PostgreSQL before any queue or cache is touched. The `webhook_events` table doubles as the outbox: a sweeper re-publishes events that were stored but never processed (FR-PRO-7). Redis is only a dedupe fast path (ADR-003) and the broker only carries task messages.

## Consequences

- Receipt idempotency is a `UNIQUE (provider, event_id)` constraint with `INSERT ... ON CONFLICT DO NOTHING` (FR-IDE-2).
- Claiming an event is a compare-and-set `UPDATE` guarded by status and by the database clock (FR-PRO-2).
- Every state change writes an audit row in the same transaction (FR-PRO-8).
- Schema details fixed in F2: the `payments` projection row is created empty, so its non-key columns are nullable; `event_attempts` has no uniqueness on `attempt_no`, because replay resets the counter and keeps the history.

## Alternatives considered

- Publish inside the request, then persist: loses events on a crash between both steps.
- Broker as the source of truth: recovery would depend on one broker's redelivery behavior (see ADR-007).