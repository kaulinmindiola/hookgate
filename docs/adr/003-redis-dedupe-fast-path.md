# ADR-003: Redis is only a dedupe fast path, written after commit

Status: accepted (SDD v0.2) · Implemented in phase F4 · Cool-down added in phase F12

## Context

Most duplicate deliveries arrive within minutes of the original. Answering them from memory saves a database round trip, but a cache that disagrees with the database must never cause an event to be lost or applied twice.

## Decision

Redis holds a marker per `(provider, event_id)` with a TTL (`DEDUPE_TTL_S`, 24 h by default). The API reads it before the insert and writes it only **after** the event is committed in PostgreSQL. PostgreSQL's `UNIQUE (provider, event_id)` remains the authority. Every Redis error is caught, counted (`hookgate_cache_errors_total`) and logged, and the flow continues with PostgreSQL. The client has 50 ms socket timeouts and no retries. After a failed call the adapter skips Redis for a 5 s cool-down (`CacheCircuitOpen`), and then one call probes it again.

## Consequences

- A marker never exists for an event that was not stored, so a cache hit can never drop a new event.
- A Redis outage only removes the fast path: duplicates are still caught by the database (VR-3, `tests/chaos/test_redis_outage.py`). Readiness reports `degraded`, not `down`.
- The cool-down exists because of a finding of F12: in the compose network a stopped container's name takes about 3.5 s to fail to resolve, which socket timeouts do not bound. With the cool-down an outage costs one slow call per 5 s instead of two per delivery.
- A body mismatch on a duplicate (FR-IDE-6) is only detected on the PostgreSQL path, not on a Redis hit (decision P6).
- Restoring a database backup can leave markers for events the database no longer has; the Redis keys must be flushed after a restore (README, Limitations).

## Alternatives considered

- Redis as the source of truth for idempotency: an outage or an eviction would let duplicates through.
- No cache at all: correct, but every duplicate costs a database insert attempt.