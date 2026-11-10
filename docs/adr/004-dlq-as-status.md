# ADR-004: The dead-letter queue is a status on the events table

Status: accepted (SDD v0.2) · Implemented in phases F5 (dead) and F8 (admin API, replay)

## Context

Events that cannot be processed (poison payloads, exhausted retries) must stay visible, inspectable and replayable. A separate dead-letter table or broker queue means copying the event, and copies drift from the original and its history.

## Decision

`dead` is one more status of `webhook_events`. The admin API lists events by status with keyset pagination (`GET /admin/events?status=dead`), shows the stored raw body, the transitions and the attempts (`GET /admin/events/{id}`), and replays a dead event (`POST /admin/events/{id}/replay`). The replay is a state transition, `dead -> received` with reason `operator_replay`: it resets `attempts`, clears the retry time and the lease, keeps the history and enqueues the event.

## Consequences

- No copying: the exact bytes received are what an operator inspects and what a replay processes.
- The replay goes through the same compare-and-set as every transition (ADR-011): only a `dead` event can be replayed (`409 event_not_dead` otherwise), and concurrent replays enqueue it once (VR-10).
- The full history survives replays: attempt rows are never deleted, so a poison payload replayed without a fix ends `dead` again with every attempt visible.
- Bulk replay by filter is left for the backlog (FR-DLQ-5).

## Alternatives considered

- A separate dead-letter table: needs a copy and a way back, and splits the audit trail.
- The broker's dead-letter exchange: not inspectable through our API, and the database would not know the event was given up on.