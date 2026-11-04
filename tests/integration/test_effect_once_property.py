"""VR-1 (complete): N deliveries, in any order, interleaved with processing, against a
real PostgreSQL, produce exactly one stored row and one applied effect per event
(FR-IDE-1/2/5, FR-PRO-2/3).

Each Hypothesis example starts by truncating the tables inside the test body (P8):
function-scoped fixtures run once per test, not once per example. Processing is
invoked directly, without a broker.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker
from tests.fakes import FixedClock, FixedRng, RecordingPublisher

from hookgate.application.ingest_event import IngestCommand, IngestEvent, IngestStatus
from hookgate.application.process_event import ProcessEvent, ProcessOutcome
from hookgate.domain.retry import RetryPolicy
from hookgate.domain.signature import build_signature_header
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.transactions import event_store_scope, processing_scope

pytestmark = pytest.mark.integration

SECRET = "vr1-secret"
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)
TABLES = "applied_effects, event_attempts, event_transitions, payments, webhook_events"


@dataclass(frozen=True)
class Step:
    action: str  # "deliver" or "process"
    event_index: int


@st.composite
def scenarios(draw: st.DrawFn) -> tuple[int, list[Step]]:
    event_count = draw(st.integers(min_value=1, max_value=3))
    indexes = st.integers(min_value=0, max_value=event_count - 1)
    extra = draw(
        st.lists(st.builds(Step, st.sampled_from(["deliver", "process"]), indexes), max_size=10)
    )
    first_deliveries = [Step("deliver", index) for index in range(event_count)]
    steps = draw(st.permutations([*first_deliveries, *extra]))
    return event_count, steps


def command(event_index: int) -> IngestCommand:
    raw = json.dumps(
        {
            "id": f"evt_{event_index}",
            "type": "payment.succeeded",
            "occurred_at": "2026-11-03T11:59:00Z",
            "resource": {"type": "payment", "id": f"pay_{event_index}"},
            "data": {"amount": 100 + event_index, "currency": "USD"},
        }
    ).encode()
    return IngestCommand(
        raw_body=raw,
        signature_header=build_signature_header(SECRET, int(NOW.timestamp()), raw),
        headers={},
        trace_id=f"trace-{event_index}",
    )


def count(session: Session, sql: str) -> int:
    return int(session.execute(text(sql)).scalar_one())


@settings(
    max_examples=40,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(scenario=scenarios())
def test_every_event_is_stored_once_and_applied_once(
    session_factory: sessionmaker[Session], scenario: tuple[int, list[Step]]
) -> None:
    event_count, steps = scenario
    with session_scope(session_factory) as session:
        session.execute(text(f"TRUNCATE TABLE {TABLES} RESTART IDENTITY CASCADE"))

    ingest = IngestEvent(
        secret=SECRET,
        tolerance_s=300,
        clock=FixedClock(NOW),
        events=event_store_scope(session_factory),
        publisher=RecordingPublisher(),
    )
    process = ProcessEvent(
        units=processing_scope(session_factory),
        lease_s=60,
        policy=RetryPolicy(),
        rng=FixedRng(0.5),
    )
    stored: dict[int, UUID] = {}
    for step in steps:
        if step.action == "deliver":
            result = ingest(command(step.event_index))
            if result.status is IngestStatus.ACCEPTED:
                assert result.stored_id is not None
                stored[step.event_index] = result.stored_id
        elif step.event_index in stored:
            outcome = process(stored[step.event_index]).outcome
            assert outcome in {ProcessOutcome.PROCESSED, ProcessOutcome.SKIPPED}

    for stored_id in stored.values():  # drain: what the sweeper would eventually do
        process(stored_id)

    assert sorted(stored) == list(range(event_count))
    with session_scope(session_factory) as session:
        assert count(session, "SELECT count(*) FROM webhook_events") == event_count
        assert count(session, "SELECT count(*) FROM applied_effects") == event_count
        assert count(session, "SELECT count(*) FROM payments") == event_count
        processed = "SELECT count(*) FROM webhook_events WHERE status = 'processed'"
        assert count(session, processed) == event_count
        assert count(session, "SELECT count(*) FROM event_attempts") == event_count
