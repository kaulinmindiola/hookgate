# ADR-002: RabbitMQ as the Celery broker, separate from the Redis cache

Status: accepted (SDD v0.2) · Implemented in phase F7

## Context

Redis already serves as the dedupe fast path (ADR-003). Celery can also use Redis as its broker, which would save one service.

## Decision

Use RabbitMQ as the broker. Redis stays a cache only.

## Consequences

- A Redis outage only disables the fast path; enqueueing keeps working. The Redis failure tests (VR-3) and the broker failure tests (VR-9) stay independent, so each one proves one thing.
- One more compose service and one more CI service in the chaos job.
- Publishing is bounded by design. Opening a connection is tried once (kombu would otherwise retry a refused connection until its timeout, making the provider wait), and a connection found dead is reconnected once, immediately (`task_publish_retry_policy` with `max_retries=1` and no sleeping). Measured against RabbitMQ 3.13: a stopped broker fails the publish in 0 to 2 ms; the event is already durable and the sweeper republishes it (ADR-007).
- The single reconnect is required, not an optimization. Without it a producer keeps its dead channel after a broker restart: Celery beat (one cached producer) never sent another sweep and events stayed `received` forever, found during the manual VR-9 check in phase F7 and reproduced against a real RabbitMQ; the API also lost its first publish after each restart.

## Alternatives considered

- Redis as both cache and broker: one service less, but a single outage would take down both layers and blur the failure tests.
- No broker (workers poll the table): simpler, but polling latency and load grow with the number of workers; the sweeper already covers the polling role for recovery only.