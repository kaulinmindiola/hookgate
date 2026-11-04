"""Per-resource ordering against a real PostgreSQL (FR-ORD-1..3, ADR-006, P5).

The last applied event of a resource wins; older events end ``processed`` with outcome
``ignored_stale`` and no effect. Events of one resource serialize on its row lock;
events of other resources are not blocked by it.
"""

import json
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker
from tests.fakes import FixedRng

from hookgate.application.process_event import ProcessEvent, ProcessOutcome
from hookgate.application.records import NewEvent, PaymentRecord
from hookgate.domain.events import EventIdentity
from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.retry import RetryPolicy
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.payments import PaymentProjection
from hookgate.infrastructure.db.repositories import EventRepository
from hookgate.infrastructure.db.transactions import processing_scope

pytestmark = pytest.mark.integration

Factory = sessionmaker[Session]
T0 = datetime(2026, 11, 3, 10, 0, 0, tzinfo=UTC)


def insert_event(
    factory: Factory,
    event_id: str,
    *,
    resource_id: str = "pay_1",
    seconds: int = 0,
    event_type: str = "payment.created",
    amount: int = 100,
) -> UUID:
    occurred_at = T0 + timedelta(seconds=seconds)
    raw = json.dumps(
        {
            "id": event_id,
            "type": event_type,
            "occurred_at": occurred_at.isoformat(),
            "resource": {"type": "payment", "id": resource_id},
            "data": {"amount": amount, "currency": "USD"},
        }
    ).encode()
    new = NewEvent(
        identity=EventIdentity("sim", event_id),
        event_type=event_type,
        resource_type="payment",
        resource_id=resource_id,
        occurred_at=occurred_at,
        received_at=T0,
        raw_body=raw,
        headers={},
        trace_id=f"trace-{event_id}",
    )
    with session_scope(factory) as session:
        stored_id = EventRepository(session).insert_if_new(new)
    assert stored_id is not None
    return stored_id


def make_processor(factory: Factory) -> ProcessEvent:
    return ProcessEvent(
        units=processing_scope(factory), lease_s=60, policy=RetryPolicy(), rng=FixedRng(0.5)
    )


def payment(factory: Factory, resource_id: str = "pay_1") -> PaymentRecord:
    with session_scope(factory) as session:
        record = PaymentProjection(session).get(resource_id)
    assert record is not None
    return record


def stored_outcome(factory: Factory, event_id: UUID) -> tuple[EventStatus, EventOutcome | None]:
    with session_scope(factory) as session:
        stored = EventRepository(session).get(event_id)
    assert stored is not None
    return stored.status, stored.outcome


def effects(factory: Factory) -> int:
    with session_scope(factory) as session:
        return int(session.execute(text("SELECT count(*) FROM applied_effects")).scalar_one())


# -- the rule ----------------------------------------------------------------------


def test_first_event_of_a_new_resource_is_applied(session_factory: Factory) -> None:
    event_id = insert_event(session_factory, "evt_a", event_type="payment.authorized")

    result = make_processor(session_factory)(event_id)

    assert result.event_outcome is EventOutcome.APPLIED
    record = payment(session_factory)
    assert (record.status, record.last_event_id, record.last_occurred_at) == (
        "authorized",
        "evt_a",
        T0,
    )


def test_older_event_arriving_late_is_ignored_without_effect(session_factory: Factory) -> None:
    newer = insert_event(session_factory, "evt_new", seconds=10, event_type="payment.succeeded")
    older = insert_event(session_factory, "evt_old", seconds=5, event_type="payment.authorized")
    process = make_processor(session_factory)

    process(newer)
    result = process(older)

    assert result.outcome is ProcessOutcome.PROCESSED
    assert result.event_outcome is EventOutcome.IGNORED_STALE
    assert stored_outcome(session_factory, older) == (
        EventStatus.PROCESSED,
        EventOutcome.IGNORED_STALE,
    )
    record = payment(session_factory)
    assert (record.status, record.last_event_id) == ("succeeded", "evt_new")
    assert effects(session_factory) == 1


def test_newer_event_overwrites_the_snapshot(session_factory: Factory) -> None:
    older = insert_event(session_factory, "evt_1", seconds=1, amount=100)
    newer = insert_event(
        session_factory, "evt_2", seconds=2, event_type="payment.refunded", amount=0
    )
    process = make_processor(session_factory)

    outcomes = [process(older).event_outcome, process(newer).event_outcome]

    assert outcomes == [EventOutcome.APPLIED, EventOutcome.APPLIED]
    record = payment(session_factory)
    assert (record.status, record.amount, record.last_event_id) == ("refunded", 0, "evt_2")
    assert effects(session_factory) == 2


def test_tie_on_occurred_at_is_decided_by_event_id(session_factory: Factory) -> None:
    b = insert_event(session_factory, "evt_b", seconds=7, event_type="payment.succeeded")
    a = insert_event(session_factory, "evt_a", seconds=7, event_type="payment.failed")
    process = make_processor(session_factory)

    process(b)
    result = process(a)  # same instant, smaller id: stale

    assert result.event_outcome is EventOutcome.IGNORED_STALE
    assert payment(session_factory).last_event_id == "evt_b"


def test_stale_event_is_still_validated(session_factory: Factory) -> None:
    newer = insert_event(session_factory, "evt_new", seconds=10)
    poison = insert_event(session_factory, "evt_bad", seconds=5, event_type="payment.exploded")
    process = make_processor(session_factory)

    process(newer)
    result = process(poison)

    assert result.outcome is ProcessOutcome.DEAD
    assert payment(session_factory).last_event_id == "evt_new"


# -- locking (FR-ORD-3) ------------------------------------------------------------


@pytest.fixture
def held_resource_lock(session_factory: Factory) -> Iterator[threading.Event]:
    """Hold ``FOR UPDATE`` on pay_locked in another transaction until the event is set."""
    with session_scope(session_factory) as session:
        PaymentProjection(session).lock_resource("pay_locked")  # create the row
    release = threading.Event()
    locked = threading.Event()

    def hold() -> None:
        with session_scope(session_factory) as session:
            PaymentProjection(session).lock_resource("pay_locked")
            locked.set()
            release.wait(timeout=30)

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    assert locked.wait(timeout=10)
    yield release
    release.set()
    holder.join(timeout=10)


def test_a_locked_resource_blocks_only_its_own_events(
    session_factory: Factory, held_resource_lock: threading.Event
) -> None:
    blocked_id = insert_event(session_factory, "evt_blocked", resource_id="pay_locked")
    free_id = insert_event(session_factory, "evt_free", resource_id="pay_free")
    process = make_processor(session_factory)

    with ThreadPoolExecutor(max_workers=2) as pool:
        blocked = pool.submit(process, blocked_id)
        free = pool.submit(process, free_id)

        assert free.result(timeout=10).outcome is ProcessOutcome.PROCESSED  # not blocked
        with pytest.raises(TimeoutError):
            blocked.result(timeout=0.5)  # waits for the lock on pay_locked

        held_resource_lock.set()
        assert blocked.result(timeout=10).outcome is ProcessOutcome.PROCESSED

    assert payment(session_factory, "pay_locked").last_event_id == "evt_blocked"


def test_concurrent_events_of_one_resource_end_on_the_latest(session_factory: Factory) -> None:
    count = 8
    ids = [insert_event(session_factory, f"evt_{i}", seconds=i, amount=i) for i in range(count)]
    process = make_processor(session_factory)
    barrier = Barrier(count)

    def run(event_id: UUID) -> EventOutcome | None:
        barrier.wait(timeout=10)
        return process(event_id).event_outcome

    with ThreadPoolExecutor(max_workers=count) as pool:
        outcomes = list(pool.map(run, ids))

    assert None not in outcomes  # every event processed: no deadlock, no lost claim
    record = payment(session_factory)
    assert (record.last_event_id, record.amount) == ("evt_7", 7)
    assert effects(session_factory) == outcomes.count(EventOutcome.APPLIED)


def test_concurrent_events_of_different_resources_are_all_applied(
    session_factory: Factory,
) -> None:
    count = 16
    ids = [insert_event(session_factory, f"evt_{i}", resource_id=f"pay_{i}") for i in range(count)]
    process = make_processor(session_factory)
    barrier = Barrier(count)

    def run(event_id: UUID) -> EventOutcome | None:
        barrier.wait(timeout=10)
        return process(event_id).event_outcome

    with ThreadPoolExecutor(max_workers=count) as pool:
        outcomes = list(pool.map(run, ids))

    assert outcomes == [EventOutcome.APPLIED] * count
    assert effects(session_factory) == count
