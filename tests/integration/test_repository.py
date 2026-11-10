"""Repository behavior against a real PostgreSQL: conflicts, atomic claim, CAS, audit."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from sqlalchemy import insert, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from hookgate.application.records import EventQuery, NewEvent, PageKey, StoredEvent
from hookgate.domain.events import EventIdentity
from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.states import EventStatus, TransitionResult, Trigger, transition
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.models import AppliedEffectRow
from hookgate.infrastructure.db.repositories import EventRepository

pytestmark = pytest.mark.integration

Factory = sessionmaker[Session]
T0 = datetime(2026, 11, 2, 10, 0, 0, tzinfo=UTC)
S = EventStatus


# -- helpers ----------------------------------------------------------------------


def make_event(
    event_id: str = "evt_1",
    *,
    provider: str = "sim",
    raw_body: bytes = b'{"id": "evt_1"}',
    resource_id: str = "pay_1",
    received_at: datetime = T0,
    trace_id: str = "trace-1",
) -> NewEvent:
    return NewEvent(
        identity=EventIdentity(provider, event_id),
        event_type="payment.created",
        resource_type="payment",
        resource_id=resource_id,
        occurred_at=T0,
        received_at=received_at,
        raw_body=raw_body,
        headers={"content-type": "application/json"},
        trace_id=trace_id,
    )


def insert_event(factory: Factory, new: NewEvent | None = None) -> UUID:
    with session_scope(factory) as session:
        event_id = EventRepository(session).insert_if_new(new or make_event())
    assert event_id is not None
    return event_id


def load(factory: Factory, event_id: UUID) -> StoredEvent:
    with session_scope(factory) as session:
        stored = EventRepository(session).get(event_id)
    assert stored is not None
    return stored


def audit(factory: Factory, event_id: UUID) -> list[tuple[EventStatus | None, EventStatus, str]]:
    with session_scope(factory) as session:
        records = EventRepository(session).list_transitions(event_id)
    return [(r.from_status, r.to_status, r.reason) for r in records]


def claim(factory: Factory, event_id: UUID, lease_s: int = 60) -> StoredEvent | None:
    with session_scope(factory) as session:
        return EventRepository(session).claim(event_id, lease_s=lease_s)


def apply(
    factory: Factory,
    event_id: UUID,
    result: TransitionResult,
    *,
    next_attempt_at: datetime | None = None,
    last_error: str | None = None,
    outcome: EventOutcome | None = None,
) -> bool:
    with session_scope(factory) as session:
        return EventRepository(session).apply_transition(
            event_id,
            result,
            next_attempt_at=next_attempt_at,
            last_error=last_error,
            outcome=outcome,
        )


def force_state(
    factory: Factory,
    event_id: UUID,
    status: EventStatus,
    *,
    attempts: int | None = None,
    next_attempt_at: datetime | None = None,
    locked_until: datetime | None = None,
) -> None:
    """Put a row in an arbitrary state (what a crashed or past run would have left)."""
    with session_scope(factory) as session:
        session.execute(
            text(
                "UPDATE webhook_events SET status = CAST(:status AS event_status), "
                "attempts = COALESCE(:attempts, attempts), "
                "next_attempt_at = :next_attempt_at, locked_until = :locked_until "
                "WHERE id = :id"
            ),
            {
                "status": status.value,
                "attempts": attempts,
                "next_attempt_at": next_attempt_at,
                "locked_until": locked_until,
                "id": event_id,
            },
        )


def lease_remaining_s(factory: Factory, event_id: UUID) -> float:
    """Seconds left on the lease, measured with the database clock (no clock skew)."""
    with session_scope(factory) as session:
        value = session.execute(
            text(
                "SELECT EXTRACT(EPOCH FROM (locked_until - now())) "
                "FROM webhook_events WHERE id = :id"
            ),
            {"id": event_id},
        ).scalar_one()
    return float(value)


def hours_from_now(hours: int) -> datetime:
    return datetime.now(UTC) + timedelta(hours=hours)


# -- insert and duplicates --------------------------------------------------------


def test_insert_persists_exact_bytes_and_the_ingestion_audit_row(
    session_factory: Factory,
) -> None:
    raw = b'\x00\xff{"id":\n  "evt_1"}  \r\n'
    event_id = insert_event(session_factory, make_event(raw_body=raw))

    stored = load(session_factory, event_id)

    assert stored.raw_body == raw
    assert stored.status is S.RECEIVED
    assert stored.attempts == 0
    assert stored.headers == {"content-type": "application/json"}
    assert stored.trace_id == "trace-1"
    assert stored.outcome is None
    assert audit(session_factory, event_id) == [(None, S.RECEIVED, "ingested")]


def test_duplicate_returns_none_and_keeps_the_original(session_factory: Factory) -> None:
    first = insert_event(session_factory, make_event(raw_body=b"original"))

    with session_scope(session_factory) as session:
        repository = EventRepository(session)
        second = repository.insert_if_new(make_event(raw_body=b"different body"))
        original_body = repository.find_raw_body(EventIdentity("sim", "evt_1"))

    assert second is None
    assert original_body == b"original"
    assert load(session_factory, first).raw_body == b"original"
    assert len(audit(session_factory, first)) == 1


def test_same_event_id_from_another_provider_is_a_different_event(
    session_factory: Factory,
) -> None:
    first = insert_event(session_factory, make_event(provider="sim"))
    second = insert_event(session_factory, make_event(provider="other"))

    assert first != second


def test_unknown_event_is_not_found(session_factory: Factory) -> None:
    with session_scope(session_factory) as session:
        repository = EventRepository(session)
        assert repository.get(uuid4()) is None
        assert repository.find_raw_body(EventIdentity("sim", "nope")) is None


# -- claim ------------------------------------------------------------------------


def test_claim_sets_processing_increments_attempts_and_sets_the_lease(
    session_factory: Factory,
) -> None:
    event_id = insert_event(session_factory)

    claimed = claim(session_factory, event_id, lease_s=60)

    assert claimed is not None
    assert claimed.status is S.PROCESSING
    assert claimed.attempts == 1
    assert claimed.locked_until is not None
    assert 0 < lease_remaining_s(session_factory, event_id) <= 60
    assert audit(session_factory, event_id)[-1] == (S.RECEIVED, S.PROCESSING, "claim")


def test_claim_rejects_a_non_positive_lease(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)

    with session_scope(session_factory) as session, pytest.raises(ValueError, match="lease_s"):
        EventRepository(session).claim(event_id, lease_s=0)


@pytest.mark.parametrize("status", [S.PROCESSING, S.PROCESSED, S.DEAD])
def test_claim_returns_none_unless_received_or_failed(
    session_factory: Factory, status: EventStatus
) -> None:
    event_id = insert_event(session_factory)
    force_state(session_factory, event_id, status)

    assert claim(session_factory, event_id) is None
    assert load(session_factory, event_id).status is status


def test_failed_event_is_claimable_once_its_retry_time_has_passed(
    session_factory: Factory,
) -> None:
    event_id = insert_event(session_factory)
    force_state(session_factory, event_id, S.FAILED, attempts=1, next_attempt_at=hours_from_now(-1))

    claimed = claim(session_factory, event_id)

    assert claimed is not None
    assert claimed.attempts == 2
    assert audit(session_factory, event_id)[-1] == (S.FAILED, S.PROCESSING, "claim")


def test_failed_event_is_not_claimable_before_its_retry_time(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)
    force_state(session_factory, event_id, S.FAILED, attempts=1, next_attempt_at=hours_from_now(1))

    assert claim(session_factory, event_id) is None
    assert load(session_factory, event_id).status is S.FAILED
    assert len(audit(session_factory, event_id)) == 1


def test_only_one_of_many_concurrent_claims_wins(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)
    workers = 16
    barrier = Barrier(workers)

    def attempt() -> StoredEvent | None:
        barrier.wait(timeout=10)
        return claim(session_factory, event_id)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(attempt) for _ in range(workers)]
        results = [future.result(timeout=30) for future in futures]

    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    assert winners[0].attempts == 1
    assert load(session_factory, event_id).attempts == 1
    claims = [entry for entry in audit(session_factory, event_id) if entry[2] == "claim"]
    assert len(claims) == 1


# -- compare-and-set transitions --------------------------------------------------


def test_handler_success_marks_processed_and_clears_schedule_and_lease(
    session_factory: Factory,
) -> None:
    event_id = insert_event(session_factory)
    claim(session_factory, event_id)

    result = transition(S.PROCESSING, Trigger.HANDLER_SUCCESS, attempts=1)
    applied = apply(session_factory, event_id, result, outcome=EventOutcome.APPLIED)

    stored = load(session_factory, event_id)
    assert applied is True
    assert stored.status is S.PROCESSED
    assert stored.outcome is EventOutcome.APPLIED
    assert stored.processed_at is not None
    assert stored.locked_until is None
    assert stored.next_attempt_at is None
    assert stored.attempts == 1
    assert audit(session_factory, event_id) == [
        (None, S.RECEIVED, "ingested"),
        (S.RECEIVED, S.PROCESSING, "claim"),
        (S.PROCESSING, S.PROCESSED, "handler_success"),
    ]


def test_transient_error_schedules_a_retry_that_is_not_claimable_yet(
    session_factory: Factory,
) -> None:
    event_id = insert_event(session_factory)
    claim(session_factory, event_id)
    retry_at = hours_from_now(1)

    result = transition(S.PROCESSING, Trigger.TRANSIENT_ERROR, attempts=1, max_attempts=5)
    applied = apply(session_factory, event_id, result, next_attempt_at=retry_at, last_error="boom")

    stored = load(session_factory, event_id)
    assert applied is True
    assert stored.status is S.FAILED
    assert stored.next_attempt_at == retry_at
    assert stored.locked_until is None
    assert stored.last_error == "boom"
    assert claim(session_factory, event_id) is None


def test_transient_error_at_max_attempts_goes_to_dead(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)
    force_state(session_factory, event_id, S.PROCESSING, attempts=5, locked_until=hours_from_now(1))

    result = transition(S.PROCESSING, Trigger.TRANSIENT_ERROR, attempts=5, max_attempts=5)
    applied = apply(session_factory, event_id, result, last_error="gave up")

    stored = load(session_factory, event_id)
    assert applied is True
    assert stored.status is S.DEAD
    assert stored.last_error == "gave up"
    assert stored.locked_until is None
    assert stored.next_attempt_at is None


def test_transition_from_an_outdated_status_is_a_no_op(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)  # still RECEIVED

    result = transition(S.PROCESSING, Trigger.PERMANENT_ERROR, attempts=1)
    applied = apply(session_factory, event_id, result, last_error="late")

    assert applied is False
    assert load(session_factory, event_id).status is S.RECEIVED
    assert load(session_factory, event_id).last_error is None
    assert len(audit(session_factory, event_id)) == 1


def test_replay_makes_a_dead_event_claimable_again(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)
    force_state(
        session_factory,
        event_id,
        S.DEAD,
        attempts=5,
        next_attempt_at=hours_from_now(1),
        locked_until=hours_from_now(1),
    )
    assert claim(session_factory, event_id) is None

    result = transition(S.DEAD, Trigger.REPLAY, attempts=5)
    applied = apply(session_factory, event_id, result)

    replayed = load(session_factory, event_id)
    assert applied is True
    assert replayed.status is S.RECEIVED
    assert replayed.attempts == 0
    assert replayed.next_attempt_at is None
    assert replayed.locked_until is None
    claimed = claim(session_factory, event_id)
    assert claimed is not None
    assert claimed.attempts == 1


def test_transition_arguments_are_validated(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)
    retry = transition(S.PROCESSING, Trigger.TRANSIENT_ERROR, attempts=1, max_attempts=5)
    success = transition(S.PROCESSING, Trigger.HANDLER_SUCCESS, attempts=1)
    dead = transition(S.PROCESSING, Trigger.PERMANENT_ERROR, attempts=1)
    claim_result = transition(S.RECEIVED, Trigger.CLAIM)

    with session_scope(session_factory) as session:
        repository = EventRepository(session)
        with pytest.raises(ValueError, match="next_attempt_at"):
            repository.apply_transition(event_id, retry)
        with pytest.raises(ValueError, match="outcome is required"):
            repository.apply_transition(event_id, success)
        with pytest.raises(ValueError, match="only valid"):
            repository.apply_transition(event_id, dead, outcome=EventOutcome.APPLIED)
        with pytest.raises(ValueError, match="claim"):
            repository.apply_transition(event_id, claim_result)


# -- applied effects --------------------------------------------------------------


def test_applied_effects_rejects_a_second_effect_for_the_same_event(
    session_factory: Factory,
) -> None:
    event_id = insert_event(session_factory)
    with session_scope(session_factory) as session:
        session.execute(insert(AppliedEffectRow).values(event_id=event_id))

    with pytest.raises(IntegrityError), session_scope(session_factory) as session:
        session.execute(insert(AppliedEffectRow).values(event_id=event_id))


def test_applied_effects_rejects_an_effect_for_an_unknown_event(
    session_factory: Factory,
) -> None:
    with pytest.raises(IntegrityError), session_scope(session_factory) as session:
        session.execute(insert(AppliedEffectRow).values(event_id=uuid4()))


# -- listing ----------------------------------------------------------------------


def test_list_events_filters_by_status_and_pages_by_keyset(session_factory: Factory) -> None:
    ids = [
        insert_event(session_factory, make_event(f"evt_{i}", received_at=T0 + timedelta(seconds=i)))
        for i in range(5)
    ]
    force_state(session_factory, ids[1], S.DEAD)
    force_state(session_factory, ids[3], S.DEAD)

    with session_scope(session_factory) as session:
        repository = EventRepository(session)
        dead = repository.list_events(EventQuery(statuses=(S.DEAD,)), limit=10)
        received_first = repository.list_events(EventQuery(statuses=(S.RECEIVED,)), limit=2)
        received_next = repository.list_events(
            EventQuery(statuses=(S.RECEIVED,)),
            limit=2,
            after=PageKey(received_at=received_first[-1].received_at, id=received_first[-1].id),
        )

    assert [event.id for event in dead] == [ids[1], ids[3]]
    assert [event.id for event in received_first] == [ids[0], ids[2]]
    assert [event.id for event in received_next] == [ids[4]]
