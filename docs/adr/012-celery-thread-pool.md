# ADR-012: Celery worker uses the thread pool

Status: accepted (decision P4) · Implemented in phase F7

## Context

Celery's default `prefork` pool runs tasks in child processes. Prometheus counters incremented in a child are not visible to the HTTP server of the parent unless the multiprocess mode (`PROMETHEUS_MULTIPROC_DIR`) is configured. The chaos test for VR-4 kills a worker with SIGKILL and must kill every process that could be running a task.

## Decision

Run the worker with `--pool threads` (also set as `worker_pool` in the app configuration). The ADR-010 delivery settings stay: `acks_late=True`, `reject_on_worker_lost=True`, `prefetch_multiplier=1`.

## Consequences

- One process: one metrics registry (phase F9) and one PID for SIGKILL (phase F11).
- The tasks are I/O bound (PostgreSQL), so threads give real concurrency despite the GIL. CPU-heavy handlers would need another decision.
- The thread pool also works on Windows, where `prefork` is not supported, so the worker runs natively for local development (`make worker`).
- `reject_on_worker_lost` adds nothing with threads (there is no child process to lose), but it is harmless and keeps the configuration valid if the pool changes.
- Tasks are registered with `shared=False`, so each Celery app has its own tasks (relevant for tests that build several apps).

## Alternatives considered

- `prefork` + Prometheus multiprocess mode: more moving parts (a shared directory, cleanup of dead children's files) for no benefit at this load.
- `solo`: one task at a time per worker; simpler, but the burst and recovery tests would be slower and less realistic.