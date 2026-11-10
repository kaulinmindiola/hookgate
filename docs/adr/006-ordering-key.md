# ADR-006: Ordering key `(occurred_at, event_id)` per resource

Status: accepted (decision FR-ORD-1, SDD v0.2) · Implemented in phase F6

## Context

Providers do not deliver in order: an old `payment.created` can arrive after a newer `payment.succeeded`. Applying events in arrival order would leave a resource in a stale state. Timestamps alone can tie.

## Decision

Each event's ordering key is `(occurred_at, event_id)`. The payment row stores the key of the last applied event. Under a row lock (`SELECT ... FOR UPDATE` on the payment), an event whose key is not newer than the stored one ends `processed` with outcome `ignored_stale` and no effect; a newer one is applied and becomes the new key. A payment row without an applied event (`last_occurred_at` is NULL) means "no previous event", handled explicitly by the pure rule `ordering.is_stale` (decision P5).

## Consequences

- The order is total even when timestamps tie, and deterministic: the same set of events always gives the same final state, whatever the arrival order (VR-2, `tests/integration/test_ordering_property.py`).
- Events of the same payment are serialized by the row lock; events of different payments run in parallel (FR-ORD-3).
- The guarantee relies on the provider's `occurred_at` and on events that carry a full snapshot of the resource; delta events or skewed provider clocks break it (README, Limitations).

## Alternatives considered

- Arrival order: wrong under reordering, which providers do.
- A provider sequence number: better if the provider offers one, but not part of the simulated provider's contract.