# ADR-010: Celery delivery settings: late ack, reject on worker loss, prefetch 1

Status: accepted (SDD v0.2) · Implemented in phase F7

## Context

By default Celery acknowledges a message when a worker receives it and prefetches several messages per process. A worker that dies would then lose the messages it held, and recovery would depend entirely on the sweeper's interval.

## Decision

`task_acks_late=True`, `task_reject_on_worker_lost=True` and `worker_prefetch_multiplier=1` (`infrastructure/celery_app.py`): a message is acknowledged only after its task ran, a worker holds one message at a time, and a message whose worker died goes back to the queue. Publishing is bounded instead of blocking: one connection attempt with a 1 s timeout, and one immediate reconnect on a stale connection (`task_publish_retry` with a one-retry policy, finding of F7).

## Consequences

- A redelivered message is harmless: the claim compare-and-set lets only one worker process an event (ADR-011), and the effect is recorded once in `applied_effects`.
- Redelivery is a speed-up, not the guarantee: if the broker loses a message, the sweeper still recovers the event from the database (ADR-007).
- With the thread pool (ADR-012) `reject_on_worker_lost` has little to do, since a killed worker takes all its threads with it; it is kept because it is correct for any pool.
- After a broker restart the API and Celery beat publish again at once instead of using a dead connection (regression tests in `tests/chaos/test_broker_outage.py`).

## Alternatives considered

- Early acknowledgement: simpler, but every worker crash loses messages until the sweeper runs.
- Unlimited publish retries: an outage would block ingestion requests instead of failing fast and letting the sweeper recover.