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

Other targets: `make format`, `make lint`, `make types`, `make test` (no database needed), `make test-integration` (needs `make up`), `make down`, `make chaos`.

Integration tests use their own databases (`hookgate_test`, created on demand); set `TEST_DATABASE_URL` to point them elsewhere.

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

## Decisions

- [ADR-001: PostgreSQL is the source of truth](docs/adr/001-postgresql-source-of-truth.md)

## License

MIT