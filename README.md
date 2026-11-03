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
make check                    # lint + types + unit/property tests + integration tests
```

Other targets: `make format`, `make lint`, `make types`, `make test` (no database needed), `make test-integration` (needs `make up`), `make down`, `make chaos`.

Integration tests use their own databases (`hookgate_test`, created on demand); set `TEST_DATABASE_URL` to point them elsewhere.

## Decisions

- [ADR-001: PostgreSQL is the source of truth](docs/adr/001-postgresql-source-of-truth.md)

## License

MIT