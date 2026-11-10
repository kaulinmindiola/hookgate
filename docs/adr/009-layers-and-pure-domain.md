# ADR-009: Layered structure with a pure domain

Status: accepted (SDD v0.2) · Implemented from phase F0 (structure) and F1 (domain)

## Context

The rules that decide correctness (state machine, retry policy, ordering, signature verification) must be easy to test exhaustively. Mixed with database and network code, they would need services to test and a coverage number would mean little.

## Decision

Four layers, with dependencies pointing inwards:

- `domain`: pure rules and models, no I/O;
- `application`: use cases and the ports they need (repositories, cache, publisher, clock);
- `infrastructure`: PostgreSQL, Redis, Celery, Prometheus;
- `api`: FastAPI routers, dependencies and error mapping.

The domain's purity is checked in CI by `scripts/check_domain_purity.py` (a blocked-import list, decision P2), and the 80 % coverage gate applies to `hookgate.domain` (NFR-4, NFR-7).

## Consequences

- The domain is tested in milliseconds, including Hypothesis properties (VR-8, VR-11, VR-12); coverage is 100 % with branches.
- Use cases are unit-tested with in-memory fakes of the ports; the same code runs against the real adapters in the integration and chaos suites.
- A new adapter (another cache, another broker) only touches `infrastructure` and the wiring.

## Alternatives considered

- A flat module per feature: less code, but rules and I/O mixed, and slow tests for every rule.
- import-linter contracts: more expressive than the script, at the cost of one more dependency.