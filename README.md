# Hookgate

**Hookgate receives payment-provider webhooks without losing events and without processing any of them twice.**

Payment providers deliver webhooks at-least-once: duplicates, delays, reordering and retries after timeouts are normal. A naive receiver either drops events (crash between receiving and processing) or applies an effect twice (a retry races the original). Hookgate guarantees durable receipt before acknowledging, effectively-once effects and order-safe resource state.

> **Status:** work in progress. Nothing below is claimed as implemented until it has test evidence.
> Design: [`docs/srs-sdd.md`](docs/srs-sdd.md).

## Development

Requirements: Python 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
make install   # create the virtualenv and install dependencies
make check     # lint + types + tests
```

Other targets: `make format`, `make lint`, `make types`, `make test`, `make chaos`.

## License

MIT