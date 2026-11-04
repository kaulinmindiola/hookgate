# Hookgate

**Hookgate receives payment-provider webhooks without losing events and without processing any of them twice.**

Payment providers deliver webhooks at-least-once: duplicates, delays, reordering and retries after timeouts are normal. A naive receiver either drops events (crash between receiving and processing) or applies an effect twice (a retry races the original). Hookgate guarantees durable receipt before acknowledging, effectively-once effects and order-safe resource state.

> **Status:** work in progress. Nothing below is claimed as implemented until it has test evidence.
> Design: [`docs/srs-sdd.md`](docs/srs-sdd.md).

## Development

Requirements: Python 3.12, [uv](https://docs.astral.sh/uv/) and Docker with Compose.

```bash
make install                  # create the virtualenv and install dependencies
cp .env.example .env          # local development values (PowerShell: Copy-Item .env.example .env)
make up                       # start PostgreSQL, Redis and RabbitMQ and wait until healthy
make migrate                  # apply the database migrations
make run                      # start the API on http://127.0.0.1:8000
make check                    # lint + types + unit/property tests + integration tests
```

Other targets: `make format`, `make lint`, `make types`, `make test` (no database needed), `make test-integration` (needs `make up`), `make worker` (Celery worker against the local infrastructure), `make down`, `make chaos`.

Integration tests use their own databases (`hookgate_test`, created on demand); set `TEST_DATABASE_URL` to point them elsewhere.

## Running the whole system

```bash
docker compose up --build       # or: make stack (detached), then make logs
```

This builds one image and starts PostgreSQL, Redis, RabbitMQ, a one-shot `migrate` job, the `api` on http://127.0.0.1:8000, the Celery `worker` (thread pool) and `beat` (runs the sweeper every 15 s). Stop `make run` first if it is using port 8000. A signed webhook sent as shown below is then processed by the worker; `docker compose logs -f worker` shows its JSON log lines with the `trace_id`.

## Sending a signed webhook

`POST /webhooks/payments` accepts a delivery only if `Hookgate-Signature: t=<unix seconds>,v1=<hex>` is an HMAC-SHA256 of `<t>.<raw body>` with `WEBHOOK_SECRET`, and `t` is within 300 s of the server clock. With the API running (`make run`), this sends one event using the development secret from `.env.example`:

```python
import json, time

import httpx

from hookgate.domain.signature import build_signature_header

secret = "dev-only-webhook-secret-change-me"  # WEBHOOK_SECRET in your .env
body = json.dumps(
    {
        "id": "evt_demo_1",
        "type": "payment.succeeded",
        "occurred_at": "2026-11-03T10:15:30.123456Z",
        "resource": {"type": "payment", "id": "pay_demo_1"},
        "data": {"amount": 1500, "currency": "USD"},
    }
).encode()
headers = {
    "Content-Type": "application/json",
    "Hookgate-Signature": build_signature_header(secret, int(time.time()), body),
}
response = httpx.post("http://127.0.0.1:8000/webhooks/payments", content=body, headers=headers)
print(response.status_code, response.json())  # 202 {'status': 'accepted'}; again: 'duplicate'
```

Responses: `202 accepted` or `202 duplicate`; `401` with `missing_signature`, `malformed_signature`, `invalid_signature` or `timestamp_out_of_tolerance`; `400 invalid_payload` when the body is not JSON or lacks `id`, `type`, `occurred_at` (with timezone), `resource.type` or `resource.id`; `413` above 1 MiB. Every response carries `X-Trace-Id` (the caller's value if safe, otherwise a generated one). Logs are JSON lines and never contain bodies or secrets.

## Inspecting and replaying events (admin API)

Every `/admin/*` route requires `X-API-Key` with the value of `ADMIN_API_KEY` (compared in constant time); without it the answer is `401 invalid_api_key`. With the development key from `.env.example`:

```bash
KEY="X-API-Key: dev-only-admin-key-change-me"
curl -H "$KEY" "http://127.0.0.1:8000/admin/events?status=dead&limit=20"   # oldest first
curl -H "$KEY" "http://127.0.0.1:8000/admin/events?cursor=<next_cursor>"   # next page
curl -H "$KEY" "http://127.0.0.1:8000/admin/events/<id>"                   # raw body, transitions, attempts
curl -X POST -H "$KEY" "http://127.0.0.1:8000/admin/events/<id>/replay"    # dead -> received, enqueued
```

- `GET /admin/events`: filters `status` (repeatable), `resource_id`, `event_type`; `limit` 1 to 200 (default 50). Items are ordered by `(received_at, id)`; `next_cursor` is `null` on the last page, and pages never repeat or skip an event. A cursor not produced by the API is `400 invalid_cursor`.
- `GET /admin/events/{id}`: the stored raw body (exact bytes in `raw_body_base64`, readable text in `raw_body`), headers, state, last error, transitions and attempts. `404 event_not_found` if unknown.
- `POST /admin/events/{id}/replay`: only for `dead` events; resets `attempts`, clears the retry time and the lease, keeps the history, writes an `operator_replay` audit row and enqueues the event. `202 {"status": "received", "enqueued": true}`; `enqueued` is `false` if the broker was down (the sweeper enqueues it later). Not dead: `409 event_not_dead`. Concurrent replays enqueue the event once. A poison payload replayed without a fix ends `dead` again, with the full history.

## Decisions

- [ADR-001: PostgreSQL is the source of truth](docs/adr/001-postgresql-source-of-truth.md)
- [ADR-002: RabbitMQ as the broker, separate from the Redis cache](docs/adr/002-rabbitmq-broker.md)
- [ADR-007: Lease plus sweeper instead of broker redelivery](docs/adr/007-lease-and-sweeper.md)
- [ADR-012: Celery worker uses the thread pool](docs/adr/012-celery-thread-pool.md)

## License

MIT