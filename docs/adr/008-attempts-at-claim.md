# ADR-008: Attempts increment at claim, not at failure

Status: accepted (SDD v0.2) · Implemented in phase F5

## Context

If `attempts` were incremented when a handler fails, a payload that crashes the worker (killed process, out-of-memory) would never record a failure: the lease would expire, the sweeper would hand it out again, and it would crash workers forever.

## Decision

The claim (`UPDATE ... SET status = 'processing', attempts = attempts + 1, locked_until = now() + lease`) counts the attempt before any processing starts, and opens an `event_attempts` row. The pure `transition()` turns both a transient error and an expired lease into `dead` once `attempts >= MAX_ATTEMPTS`.

## Consequences

- A crash-looping poison payload ends `dead` after `MAX_ATTEMPTS` claims, even if no failure was ever recorded by the worker.
- An attempt row without an end is evidence of a crash; the sweeper closes it with `lease_expired` (VR-4).
- A replay resets `attempts` to zero but keeps the attempt rows, so the history stays complete (ADR-004).

## Alternatives considered

- Increment at failure: unbounded retries for payloads that kill the worker.
- Count crashes separately: two counters for one limit, with no benefit.