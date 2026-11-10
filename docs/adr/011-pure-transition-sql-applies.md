# ADR-011: The pure `transition()` function decides; SQL applies with a compare-and-set

Status: accepted (SDD v0.2) · Implemented in phases F1 (domain) and F5 (repository)

## Context

The event state machine (SDD section 9) is enforced in two places by nature: the rules (which trigger moves which status where, what happens to `attempts`, the lease and the retry time) and the database writes, which must stay correct when several workers, the sweeper and an operator act on the same row at once. Writing the rules again in SQL would let the two copies drift apart.

## Decision

`hookgate.domain.states.transition(status, trigger, attempts=..., max_attempts=...)` is pure. It returns a `TransitionResult`: the source and target status, the audit reason, and what to do with `attempts` (keep, increment, reset), `locked_until` and `next_attempt_at` (keep, set, clear). Any pair not in the table raises `InvalidTransition`.

The repository applies a result with one `UPDATE ... WHERE id = :id AND status = :from_status` (and `AND attempts = :expected` when a worker finishes its own attempt), writes the audit row in the same transaction, and reports whether a row matched. No rule is written in SQL:

- the claim's source statuses are derived from `transition()` when the repository module loads;
- entering `processed` or `dead` clears `next_attempt_at` and `locked_until` because the result says so;
- the only condition that lives in SQL alone is `next_attempt_at <= now()` in the claim, because it needs the database clock (decision P3, ADR-007).

## Consequences

- Concurrency safety comes from the compare-and-set: a second claim, a worker whose lease expired and whose event was claimed again, or a concurrent replay matches no row and writes nothing (outcome `lost`, or `409` for a replay).
- The rules are tested without a database: a property test drives random trigger sequences and never reaches an illegal transition (VR-12), and the domain package has 100 % coverage.
- The repository refuses results it must not apply on its own (a lease set outside the claim, a retry without a time), so a misuse fails loudly in tests.

## Alternatives considered

- Rules in SQL only (a `CASE` per transition or a trigger in PostgreSQL): hard to unit test and invisible to the domain model.
- Rules in Python with a read-modify-write: a race between the read and the write would need row locks for every transition.