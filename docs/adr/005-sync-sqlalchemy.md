# ADR-005: Synchronous SQLAlchemy 2.0 with psycopg 3 everywhere

Status: accepted (decision D2, SDD v0.2) · Implemented from phase F2 (repositories) and F3 (API)

## Context

The API could be asynchronous (FastAPI with `async def` and an async driver), but the worker cannot: Celery runs plain functions. Two session styles would mean two versions of every repository and two sets of tests for the same SQL, and the risky part of the system (the claim, the compare-and-set transitions, `ON CONFLICT`) lives exactly in that SQL.

## Decision

Use synchronous SQLAlchemy 2.0 (2.0-style `select`/`update`, typed ORM models) with psycopg 3 in the API, the worker, the sweeper, the migrations and the tests. API endpoints are plain `def` functions, which FastAPI runs in its thread pool. Every unit of work is one short transaction (`session_scope`: commit on success, rollback on any exception); no transaction is held while waiting on the network.

## Consequences

- One repository layer, used by the API, the worker and the integration tests alike.
- Concurrency comes from threads: FastAPI's thread pool for requests and Celery's thread pool for tasks (ADR-012). The database work is I/O bound, so the GIL is not the limit; each process uses a connection pool of 10 plus 10 overflow, with `pool_pre_ping` so a restarted PostgreSQL does not break the first request.
- VR-7 (32 simultaneous deliveries of one event over real HTTP) and the benchmark (about 190 accepted events per second on one laptop, see the README) run on this model; no request ever waited on a busy event loop.
- Moving the API to async later only touches the API layer and the repositories it calls; it is listed as a follow-up, not needed for the stated requirements.

## Alternatives considered

- Async API with an async driver and a sync worker: duplicate repositories and two transaction models for the same rules.
- Async everywhere with an async task runner instead of Celery: a larger stack change for no requirement that needs it.