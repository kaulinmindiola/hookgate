"""VR-2: any arrival order of a resource's events ends in the state of the latest one
(FR-ORD-1/2/4), against a real PostgreSQL.

Occurred-at values come from a tiny pool so ties are frequent and the ``event_id``
tie-break is exercised. Each example truncates the tables first (P8). The expected
final state is computed independently: the event with the greatest key.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker
from tests.fakes import FixedRng

from hookgate.application.process_event import ProcessEvent, ProcessOutcome
from hookgate.application.records import NewEvent
from hookgate.domain.events import EventIdentity
from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.retry import RetryPolicy
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.payments import PaymentProjection
from hookgate.infrastructure.db.repositories import EventRepository
from hookgate.infrastructure.db.transactions import processing_scope

pytestmark = pytest.mark.integration

T0 = datetime(2026, 11, 3, 10, 0, 0, tzinfo=UTC)
TABLES = "applied_effects, event_attempts, event_transitions, payments, webhook_events"
TYPES = [
    "payment.created",
    "payment.authorized",
    "payment.succeeded",
    "payment.failed",
    "payment.refunded",
]


@dataclass(frozen=True)
class Spec:
    event_id: str
    seconds: int
    event_type: str
    amount: int

    @property
    def key(self) -> tuple[datetime, str]:
        return (T0 + timedelta(seconds=self.seconds), self.event_id)


@st.composite
def arrivals(draw: st.DrawFn) -> list[Spec]:
    count = draw(st.integers(min_value=1, max_value=5))
    specs = [
        Spec(
            event_id=f"evt_{index}",
            seconds=draw(st.integers(min_value=0, max_value=2)),  # frequent ties
            event_type=draw(st.sampled_from(TYPES)),
            amount=index * 100,
        )
        for index in range(count)
    ]
    return draw(st.permutations(specs))


def insert(session_factory: sessionmaker[Session], spec: Spec) -> UUID:
    occurred_at = spec.key[0]
    raw = json.dumps(
        {
            "id": spec.event_id,
            "type": spec.event_type,
            "occurred_at": occurred_at.isoformat(),
            "resource": {"type": "payment", "id": "pay_vr2"},
            "data": {"amount": spec.amount, "currency": "USD"},
        }
    ).encode()
    new = NewEvent(
        identity=EventIdentity("sim", spec.event_id),
        event_type=spec.event_type,
        resource_type="payment",
        resource_id="pay_vr2",
        occurred_at=occurred_at,
        received_at=T0,
        raw_body=raw,
        headers={},
        trace_id="vr2",
    )
    with session_scope(session_factory) as session:
        stored_id = EventRepository(session).insert_if_new(new)
    assert stored_id is not None
    return stored_id


@settings(
    max_examples=200,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(order=arrivals())
def test_final_state_is_the_latest_event_whatever_the_arrival_order(
    session_factory: sessionmaker[Session], order: list[Spec]
) -> None:
    with session_scope(session_factory) as session:
        session.execute(text(f"TRUNCATE TABLE {TABLES} RESTART IDENTITY CASCADE"))
    process = ProcessEvent(
        units=processing_scope(session_factory),
        lease_s=60,
        policy=RetryPolicy(),
        rng=FixedRng(0.5),
    )

    outcomes = []
    for spec in order:  # each event arrives and is processed in arrival order
        result = process(insert(session_factory, spec))
        assert result.outcome is ProcessOutcome.PROCESSED
        outcomes.append(result.event_outcome)

    latest = max(order, key=lambda spec: spec.key)
    with session_scope(session_factory) as session:
        record = PaymentProjection(session).get("pay_vr2")
        effects = int(session.execute(text("SELECT count(*) FROM applied_effects")).scalar_one())
    assert record is not None
    assert (record.last_occurred_at, record.last_event_id) == latest.key
    assert (record.status, record.amount) == (latest.event_type.split(".")[1], latest.amount)
    assert effects == outcomes.count(EventOutcome.APPLIED)

    # An event is applied exactly when it is newer than everything that arrived before it.
    best_so_far: tuple[datetime, str] | None = None
    for spec, outcome in zip(order, outcomes, strict=True):
        newer = best_so_far is None or spec.key > best_so_far
        assert outcome is (EventOutcome.APPLIED if newer else EventOutcome.IGNORED_STALE)
        if newer:
            best_so_far = spec.key
