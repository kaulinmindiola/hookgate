"""VR-1 (ingestion part): repeated delivery is harmless (FR-IDE-1/2/5, FR-IDE-4).

Several events, each delivered one or more times, in any order, sometimes with a
changed body, while Redis fails at random: every event ends as exactly one stored
row holding the body of its first delivery, is published exactly once, and only the
first delivery of each event is acknowledged as accepted. The effect part of VR-1
(one ``applied_effects`` row) is added in phase F5.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from hypothesis import given, settings
from hypothesis import strategies as st
from tests.fakes import FixedClock, InMemoryCache, InMemoryEvents, RecordingPublisher

from hookgate.application.ingest_event import IngestCommand, IngestEvent, IngestStatus
from hookgate.domain.events import EventIdentity
from hookgate.domain.signature import build_signature_header

SECRET = "property-test-secret"
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)


@dataclass(frozen=True)
class Delivery:
    event_index: int
    changed_body: bool


@dataclass(frozen=True)
class DeliveryPlan:
    event_count: int
    deliveries: list[Delivery]
    cache_faults: list[bool]  # cache call k raises when cache_faults[k] is True


class ScriptedCache(InMemoryCache):
    """Fails exactly on the calls the plan marks, whatever the operation."""

    def __init__(self, faults: list[bool]) -> None:
        super().__init__()
        self.faults = faults
        self.calls = 0

    def _maybe_fail(self) -> None:
        call, self.calls = self.calls, self.calls + 1
        if call < len(self.faults) and self.faults[call]:
            raise ConnectionError("redis unavailable")

    def seen(self, identity: EventIdentity) -> bool:
        self._maybe_fail()
        return super().seen(identity)

    def remember(self, identity: EventIdentity) -> None:
        self._maybe_fail()
        super().remember(identity)


@st.composite
def delivery_plans(draw: st.DrawFn) -> DeliveryPlan:
    event_count = draw(st.integers(min_value=1, max_value=4))
    repeats = draw(st.lists(st.integers(min_value=0, max_value=event_count - 1), max_size=12))
    order = draw(st.permutations([*range(event_count), *repeats]))
    changed = draw(st.lists(st.booleans(), min_size=len(order), max_size=len(order)))
    faults = draw(st.lists(st.booleans(), max_size=2 * len(order)))
    deliveries = [Delivery(index, flag) for index, flag in zip(order, changed, strict=True)]
    return DeliveryPlan(event_count, deliveries, faults)


def raw_body(delivery: Delivery) -> bytes:
    amount = 9_999 if delivery.changed_body else 1_500
    return json.dumps(
        {
            "id": f"evt_{delivery.event_index}",
            "type": "payment.created",
            "occurred_at": "2026-11-03T11:59:00Z",
            "resource": {"type": "payment", "id": f"pay_{delivery.event_index}"},
            "data": {"amount": amount, "currency": "USD"},
        }
    ).encode()


@dataclass(frozen=True)
class Outcome:
    statuses: list[IngestStatus]
    events: InMemoryEvents
    publisher: RecordingPublisher


def run_plan(plan: DeliveryPlan) -> Outcome:
    events = InMemoryEvents()
    publisher = RecordingPublisher()
    ingest = IngestEvent(
        secret=SECRET,
        tolerance_s=300,
        clock=FixedClock(NOW),
        events=events.scope,
        publisher=publisher,
        cache=ScriptedCache(plan.cache_faults),
    )
    statuses = []
    for delivery in plan.deliveries:
        raw = raw_body(delivery)
        command = IngestCommand(
            raw_body=raw,
            signature_header=build_signature_header(SECRET, int(NOW.timestamp()), raw),
            headers={},
            trace_id="trace",
        )
        statuses.append(ingest(command).status)
    return Outcome(statuses, events, publisher)


def first_deliveries(plan: DeliveryPlan) -> dict[int, Delivery]:
    first: dict[int, Delivery] = {}
    for delivery in plan.deliveries:
        first.setdefault(delivery.event_index, delivery)
    return first


@settings(max_examples=200, deadline=None)
@given(plan=delivery_plans())
def test_each_event_is_stored_and_published_exactly_once(plan: DeliveryPlan) -> None:
    outcome = run_plan(plan)

    stored_ids = {identity.event_id for identity in outcome.events.rows}
    assert stored_ids == {f"evt_{index}" for index in range(plan.event_count)}
    published = [event_id for event_id, _ in outcome.publisher.published]
    assert len(published) == plan.event_count
    assert len(set(published)) == plan.event_count


@settings(max_examples=200, deadline=None)
@given(plan=delivery_plans())
def test_only_the_first_delivery_of_each_event_is_accepted(plan: DeliveryPlan) -> None:
    outcome = run_plan(plan)

    seen: set[int] = set()
    expected = []
    for delivery in plan.deliveries:
        is_first = delivery.event_index not in seen
        seen.add(delivery.event_index)
        expected.append(IngestStatus.ACCEPTED if is_first else IngestStatus.DUPLICATE)
    assert outcome.statuses == expected


@settings(max_examples=200, deadline=None)
@given(plan=delivery_plans())
def test_the_stored_body_is_the_one_from_the_first_delivery(plan: DeliveryPlan) -> None:
    outcome = run_plan(plan)

    for index, delivery in first_deliveries(plan).items():
        assert outcome.events.stored(f"evt_{index}").raw_body == raw_body(delivery)
