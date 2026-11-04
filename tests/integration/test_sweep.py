"""Sweeper against a real PostgreSQL (FR-PRO-7, FR-ING-7; VR-4 partial, VR-9 app level).

Time is moved by back-dating columns (decision P3).
"""

import json
from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker
from tests.fakes import FixedClock, FixedRng, RecordingPublisher

from hookgate.application.ingest_event import IngestCommand, IngestEvent, IngestStatus
from hookgate.application.process_event import ProcessEvent, ProcessOutcome
from hookgate.application.records import NewEvent, StoredEvent
from hookgate.application.sweep import LEASE_EXPIRED_ERROR, SweepEvents, SweepReport
from hookgate.domain.events import EventIdentity
from hookgate.domain.retry import RetryPolicy
from hookgate.domain.signature import build_signature_header
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.payments import PaymentProjection
from hookgate.infrastructure.db.repositories import EventRepository
from hookgate.infrastructure.db.transactions import (
    event_store_scope,
    processing_scope,
    sweep_scope,
)

pytestmark = pytest.mark.integration

Factory = sessionmaker[Session]
S = EventStatus
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)
POLICY = RetryPolicy(max_attempts=3, base_s=2.0, cap_s=300.0)


def body(event_id: str) -> bytes:
    return json.dumps(
        {
            "id": event_id,
            "type": "payment.succeeded",
            "occurred_at": "2026-11-03T11:59:00Z",
            "resource": {"type": "payment", "id": f"pay_{event_id}"},
            "data": {"amount": 1500, "currency": "USD"},
        }
    ).encode()


def insert_event(factory: Factory, event_id: str = "evt_1") -> UUID:
    new = NewEvent(
        identity=EventIdentity("sim", event_id),
        event_type="payment.succeeded",
        resource_type="payment",
        resource_id=f"pay_{event_id}",
        occurred_at=datetime(2026, 11, 3, 11, 59, tzinfo=UTC),
        received_at=datetime.now(UTC),
        raw_body=body(event_id),
        headers={},
        trace_id=f"trace-{event_id}",
    )
    with session_scope(factory) as session:
        stored_id = EventRepository(session).insert_if_new(new)
    assert stored_id is not None
    return stored_id


def run_sql(factory: Factory, sql: str, event_id: UUID) -> None:
    with session_scope(factory) as session:
        session.execute(text(sql), {"id": event_id})


def age_received(factory: Factory, event_id: UUID) -> None:
    run_sql(
        factory,
        "UPDATE webhook_events SET received_at = now() - interval '5 minutes' WHERE id = :id",
        event_id,
    )


def make_retry_due(factory: Factory, event_id: UUID) -> None:
    run_sql(
        factory,
        "UPDATE webhook_events SET next_attempt_at = now() - interval '1 second' WHERE id = :id",
        event_id,
    )


def expire_lease(factory: Factory, event_id: UUID) -> None:
    run_sql(
        factory,
        "UPDATE webhook_events SET locked_until = now() - interval '1 second' WHERE id = :id",
        event_id,
    )


def load(factory: Factory, event_id: UUID) -> StoredEvent:
    with session_scope(factory) as session:
        stored = EventRepository(session).get(event_id)
    assert stored is not None
    return stored


def crash_after_claim(factory: Factory, event_id: UUID) -> None:
    """A worker claims the event, opens its attempt row and dies."""
    with session_scope(factory) as session:
        repository = EventRepository(session)
        claimed = repository.claim(event_id, lease_s=60)
        assert claimed is not None
        repository.start_attempt(event_id, claimed.attempts)


@pytest.fixture
def publisher() -> RecordingPublisher:
    return RecordingPublisher()


@pytest.fixture
def sweep(session_factory: Factory, publisher: RecordingPublisher) -> SweepEvents:
    return SweepEvents(
        units=sweep_scope(session_factory), publisher=publisher, policy=POLICY, rng=FixedRng(1.0)
    )


@pytest.fixture
def process(session_factory: Factory) -> ProcessEvent:
    return ProcessEvent(
        units=processing_scope(session_factory), lease_s=60, policy=POLICY, rng=FixedRng(1.0)
    )


def published_ids(publisher: RecordingPublisher) -> list[UUID]:
    return [event_id for event_id, _ in publisher.published]


# -- what gets republished ---------------------------------------------------------


def test_old_received_event_is_republished_but_a_fresh_one_is_not(
    session_factory: Factory, sweep: SweepEvents, publisher: RecordingPublisher
) -> None:
    old = insert_event(session_factory, "evt_old")
    insert_event(session_factory, "evt_fresh")
    age_received(session_factory, old)

    report = sweep()

    assert report == SweepReport(published=1)
    assert publisher.published == [(old, "trace-evt_old")]


def test_failed_event_is_republished_only_when_its_retry_is_due(
    session_factory: Factory, sweep: SweepEvents, publisher: RecordingPublisher
) -> None:
    event_id = insert_event(session_factory)
    run_sql(
        session_factory,
        "UPDATE webhook_events SET status = 'failed', attempts = 1, "
        "next_attempt_at = now() + interval '1 hour' WHERE id = :id",
        event_id,
    )

    assert sweep().published == 0
    make_retry_due(session_factory, event_id)
    assert sweep().published == 1
    assert published_ids(publisher) == [event_id]


@pytest.mark.parametrize("status", ["processed", "dead"])
def test_finished_events_are_left_alone(
    session_factory: Factory, sweep: SweepEvents, status: str
) -> None:
    event_id = insert_event(session_factory)
    age_received(session_factory, event_id)
    run_sql(
        session_factory, f"UPDATE webhook_events SET status = '{status}' WHERE id = :id", event_id
    )

    assert sweep() == SweepReport()


def test_rows_locked_by_another_sweeper_are_skipped(
    session_factory: Factory, sweep: SweepEvents, publisher: RecordingPublisher
) -> None:
    first = insert_event(session_factory, "evt_a")
    second = insert_event(session_factory, "evt_b")
    age_received(session_factory, first)
    age_received(session_factory, second)

    with session_scope(session_factory) as session:  # another sweeper, mid-transaction
        held = EventRepository(session).lock_due_for_publish(limit=1, received_grace_s=30)
        assert len(held) == 1
        report = sweep()

    assert report.published == 1
    assert published_ids(publisher) == [{first, second}.difference({held[0].id}).pop()]


def test_publish_failures_are_counted_and_retried_on_the_next_sweep(
    session_factory: Factory, sweep: SweepEvents, publisher: RecordingPublisher
) -> None:
    event_id = insert_event(session_factory)
    age_received(session_factory, event_id)

    publisher.fail = True
    assert sweep() == SweepReport(publish_failures=1)
    publisher.fail = False
    assert sweep() == SweepReport(published=1)
    assert load(session_factory, event_id).status is S.RECEIVED


# -- expired leases (VR-4 partial) -------------------------------------------------


def test_live_lease_is_not_touched(session_factory: Factory, sweep: SweepEvents) -> None:
    event_id = insert_event(session_factory)
    crash_after_claim(session_factory, event_id)

    assert sweep() == SweepReport()
    assert load(session_factory, event_id).status is S.PROCESSING


def test_worker_crash_is_recovered_and_the_effect_applied_once(
    session_factory: Factory, sweep: SweepEvents, process: ProcessEvent
) -> None:
    event_id = insert_event(session_factory)
    crash_after_claim(session_factory, event_id)
    expire_lease(session_factory, event_id)

    report = sweep()

    assert report == SweepReport(expired_to_failed=1)
    stored = load(session_factory, event_id)
    assert (stored.status, stored.attempts, stored.locked_until) == (S.FAILED, 1, None)
    assert stored.last_error == LEASE_EXPIRED_ERROR
    assert stored.next_attempt_at is not None  # backoff applies to crashes too

    make_retry_due(session_factory, event_id)
    assert sweep().published == 1
    assert process(event_id).outcome is ProcessOutcome.PROCESSED

    with session_scope(session_factory) as session:
        repository = EventRepository(session)
        attempts = [(a.attempt_no, a.result) for a in repository.list_attempts(event_id)]
        reasons = [t.reason for t in repository.list_transitions(event_id)]
        effects = PaymentProjection(session).effect_count(event_id)
    assert attempts == [(1, "lease_expired"), (2, "processed")]
    assert reasons == ["ingested", "claim", "lease_expired", "claim", "handler_success"]
    assert effects == 1


def test_expired_lease_at_max_attempts_goes_to_dead(
    session_factory: Factory, sweep: SweepEvents
) -> None:
    event_id = insert_event(session_factory)
    crash_after_claim(session_factory, event_id)
    run_sql(session_factory, "UPDATE webhook_events SET attempts = 3 WHERE id = :id", event_id)
    expire_lease(session_factory, event_id)

    assert sweep() == SweepReport(expired_to_dead=1)
    stored = load(session_factory, event_id)
    assert (stored.status, stored.next_attempt_at, stored.locked_until) == (S.DEAD, None, None)


# -- broker down at ingestion (VR-9, application level) ----------------------------


def test_broker_down_at_ingestion_still_acknowledges_and_the_sweeper_recovers(
    session_factory: Factory, sweep: SweepEvents, process: ProcessEvent
) -> None:
    broken_broker = RecordingPublisher(fail=True)
    ingest = IngestEvent(
        secret="s",
        tolerance_s=300,
        clock=FixedClock(NOW),
        events=event_store_scope(session_factory),
        publisher=broken_broker,
    )
    raw = body("evt_vr9")
    result = ingest(
        IngestCommand(
            raw_body=raw,
            signature_header=build_signature_header("s", int(NOW.timestamp()), raw),
            headers={},
            trace_id="trace-vr9",
        )
    )
    assert result.status is IngestStatus.ACCEPTED  # the provider got its 202
    assert result.stored_id is not None
    age_received(session_factory, result.stored_id)  # the grace period passed

    assert sweep().published == 1
    assert process(result.stored_id).outcome is ProcessOutcome.PROCESSED
