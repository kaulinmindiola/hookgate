"""process_event against a real PostgreSQL (FR-PRO-1..6, FR-PRO-8; VR-5, VR-6 part 1).

Time is moved by back-dating ``next_attempt_at`` in the database, never by changing a
clock in Python: PostgreSQL is the authority for time (decision P3).
"""

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier
from uuid import UUID

import pytest
from pydantic import SecretStr
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker
from tests.fakes import FixedRng

from hookgate.application.process_event import (
    EffectHandler,
    ProcessEvent,
    ProcessOutcome,
    ProcessResult,
)
from hookgate.application.records import NewEvent, StoredEvent
from hookgate.config import Settings
from hookgate.domain.errors import TransientError
from hookgate.domain.events import Event, EventIdentity
from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.payments import Payment, project_event
from hookgate.domain.retry import RetryPolicy
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.payments import PaymentProjection
from hookgate.infrastructure.db.repositories import EventRepository
from hookgate.infrastructure.db.transactions import processing_scope
from hookgate.wiring import build_process_event

pytestmark = pytest.mark.integration

Factory = sessionmaker[Session]
S = EventStatus
POLICY = RetryPolicy(max_attempts=5, base_s=2.0, cap_s=300.0)


# -- helpers ----------------------------------------------------------------------


def insert_event(
    factory: Factory,
    *,
    event_id: str = "evt_1",
    event_type: str = "payment.succeeded",
    data: object = None,
) -> UUID:
    raw = json.dumps(
        {
            "id": event_id,
            "type": event_type,
            "occurred_at": "2026-11-03T11:59:00Z",
            "resource": {"type": "payment", "id": f"pay_{event_id}"},
            "data": {"amount": 1500, "currency": "USD"} if data is None else data,
        }
    ).encode()
    new = NewEvent(
        identity=EventIdentity("sim", event_id),
        event_type=event_type,
        resource_type="payment",
        resource_id=f"pay_{event_id}",
        occurred_at=datetime(2026, 11, 3, 11, 59, tzinfo=UTC),
        received_at=datetime(2026, 11, 3, 12, 0, tzinfo=UTC),
        raw_body=raw,
        headers={},
        trace_id=f"trace-{event_id}",
    )
    with session_scope(factory) as session:
        stored_id = EventRepository(session).insert_if_new(new)
    assert stored_id is not None
    return stored_id


def make_processor(
    factory: Factory, handler: EffectHandler = project_event, policy: RetryPolicy = POLICY
) -> ProcessEvent:
    return ProcessEvent(
        units=processing_scope(factory),
        lease_s=60,
        policy=policy,
        rng=FixedRng(1.0),  # always the top of the jitter window: exact assertions
        handler=handler,
    )


def load(factory: Factory, event_id: UUID) -> StoredEvent:
    with session_scope(factory) as session:
        stored = EventRepository(session).get(event_id)
    assert stored is not None
    return stored


def audit(factory: Factory, event_id: UUID) -> list[tuple[S | None, S, str]]:
    with session_scope(factory) as session:
        records = EventRepository(session).list_transitions(event_id)
    return [(r.from_status, r.to_status, r.reason) for r in records]


def attempts(factory: Factory, event_id: UUID) -> list[tuple[int, str | None, bool]]:
    with session_scope(factory) as session:
        records = EventRepository(session).list_attempts(event_id)
    return [(r.attempt_no, r.result, r.finished_at is not None) for r in records]


def effect_count(factory: Factory, event_id: UUID) -> int:
    with session_scope(factory) as session:
        return PaymentProjection(session).effect_count(event_id)


def seconds_until_retry(factory: Factory, event_id: UUID) -> float:
    """Measured with the database clock, like the claim guard."""
    with session_scope(factory) as session:
        return float(
            session.execute(
                text(
                    "SELECT EXTRACT(EPOCH FROM (next_attempt_at - now())) "
                    "FROM webhook_events WHERE id = :id"
                ),
                {"id": event_id},
            ).scalar_one()
        )


def make_retry_due(factory: Factory, event_id: UUID) -> None:
    """P3: move time forward by back-dating the stored retry time."""
    with session_scope(factory) as session:
        session.execute(
            text(
                "UPDATE webhook_events SET next_attempt_at = now() - interval '1 second' "
                "WHERE id = :id"
            ),
            {"id": event_id},
        )


class FlakyHandler:
    """A dependency that fails ``failures`` times with ``error`` and then works."""

    def __init__(self, failures: int, error: Callable[[], Exception]) -> None:
        self.failures = failures
        self.error = error
        self.calls = 0

    def __call__(self, event: Event) -> Payment:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error()
        return project_event(event)


def transient() -> Exception:
    return TransientError("gateway timeout")


# -- success and effect idempotency ------------------------------------------------


def test_success_applies_the_effect_and_records_everything(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)

    result = make_processor(session_factory)(event_id)

    assert result == ProcessResult(
        ProcessOutcome.PROCESSED, attempt=1, event_outcome=EventOutcome.APPLIED
    )
    stored = load(session_factory, event_id)
    assert stored.status is S.PROCESSED
    assert stored.outcome is EventOutcome.APPLIED
    assert stored.processed_at is not None
    assert (stored.locked_until, stored.next_attempt_at) == (None, None)
    assert effect_count(session_factory, event_id) == 1
    with session_scope(session_factory) as session:
        payment = PaymentProjection(session).get("pay_evt_1")
    assert payment is not None
    assert (payment.status, payment.amount, payment.currency, payment.last_event_id) == (
        "succeeded",
        1500,
        "USD",
        "evt_1",
    )
    assert audit(session_factory, event_id) == [
        (None, S.RECEIVED, "ingested"),
        (S.RECEIVED, S.PROCESSING, "claim"),
        (S.PROCESSING, S.PROCESSED, "handler_success"),
    ]
    assert attempts(session_factory, event_id) == [(1, "processed", True)]


def test_processing_again_is_a_no_op(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)
    process = make_processor(session_factory)

    outcomes = [process(event_id).outcome for _ in range(3)]

    assert outcomes == [ProcessOutcome.PROCESSED, ProcessOutcome.SKIPPED, ProcessOutcome.SKIPPED]
    assert effect_count(session_factory, event_id) == 1
    assert load(session_factory, event_id).attempts == 1


def test_concurrent_processing_applies_the_effect_once(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)
    process = make_processor(session_factory)
    workers = 8
    barrier = Barrier(workers)

    def run(_: int) -> ProcessOutcome:
        barrier.wait(timeout=10)
        return process(event_id).outcome

    with ThreadPoolExecutor(max_workers=workers) as pool:
        outcomes = list(pool.map(run, range(workers)))

    assert outcomes.count(ProcessOutcome.PROCESSED) == 1
    assert outcomes.count(ProcessOutcome.SKIPPED) == workers - 1
    assert effect_count(session_factory, event_id) == 1
    assert attempts(session_factory, event_id) == [(1, "processed", True)]


# -- transient failures and backoff (VR-5) -----------------------------------------


def test_transient_failures_retry_with_backoff_then_succeed(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)
    handler = FlakyHandler(failures=2, error=transient)
    process = make_processor(session_factory, handler)

    first = process(event_id)
    assert first.outcome is ProcessOutcome.RETRY_SCHEDULED
    assert first.error == "TransientError: gateway timeout"
    stored = load(session_factory, event_id)
    assert (stored.status, stored.attempts, stored.locked_until) == (S.FAILED, 1, None)
    assert 1.0 < seconds_until_retry(session_factory, event_id) <= 2.0  # ceiling(1) = 2 s

    assert process(event_id).outcome is ProcessOutcome.SKIPPED  # backoff respected
    assert handler.calls == 1

    make_retry_due(session_factory, event_id)
    assert process(event_id).outcome is ProcessOutcome.RETRY_SCHEDULED
    assert 3.0 < seconds_until_retry(session_factory, event_id) <= 4.0  # ceiling(2) = 4 s

    make_retry_due(session_factory, event_id)
    final = process(event_id)

    assert final == ProcessResult(
        ProcessOutcome.PROCESSED, attempt=3, event_outcome=EventOutcome.APPLIED
    )
    stored = load(session_factory, event_id)
    assert (stored.status, stored.attempts, stored.next_attempt_at) == (S.PROCESSED, 3, None)
    assert stored.last_error == "TransientError: gateway timeout"
    assert effect_count(session_factory, event_id) == 1
    assert attempts(session_factory, event_id) == [
        (1, "failed", True),
        (2, "failed", True),
        (3, "processed", True),
    ]
    assert audit(session_factory, event_id) == [
        (None, S.RECEIVED, "ingested"),
        (S.RECEIVED, S.PROCESSING, "claim"),
        (S.PROCESSING, S.FAILED, "transient_error"),
        (S.FAILED, S.PROCESSING, "claim"),
        (S.PROCESSING, S.FAILED, "transient_error"),
        (S.FAILED, S.PROCESSING, "claim"),
        (S.PROCESSING, S.PROCESSED, "handler_success"),
    ]


def test_unclassified_errors_are_treated_as_transient(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)
    handler = FlakyHandler(failures=1, error=lambda: RuntimeError("surprise"))

    result = make_processor(session_factory, handler)(event_id)

    assert result.outcome is ProcessOutcome.RETRY_SCHEDULED
    assert load(session_factory, event_id).last_error == "RuntimeError: surprise"


def test_transient_failures_end_in_dead_after_max_attempts(session_factory: Factory) -> None:
    event_id = insert_event(session_factory)
    policy = RetryPolicy(max_attempts=3, base_s=2.0, cap_s=300.0)
    process = make_processor(session_factory, FlakyHandler(99, transient), policy)

    outcomes = [process(event_id).outcome]
    for _ in range(2):
        make_retry_due(session_factory, event_id)
        outcomes.append(process(event_id).outcome)

    assert outcomes == [
        ProcessOutcome.RETRY_SCHEDULED,
        ProcessOutcome.RETRY_SCHEDULED,
        ProcessOutcome.DEAD,
    ]
    stored = load(session_factory, event_id)
    assert (stored.status, stored.attempts, stored.next_attempt_at) == (S.DEAD, 3, None)
    assert audit(session_factory, event_id)[-1] == (S.PROCESSING, S.DEAD, "transient_error")
    assert process(event_id).outcome is ProcessOutcome.SKIPPED
    assert effect_count(session_factory, event_id) == 0


# -- permanent failures (VR-6 part 1) ----------------------------------------------


@pytest.mark.parametrize(
    ("event_type", "data", "error_prefix"),
    [
        ("payment.exploded", None, "PermanentError: unknown event type"),
        ("payment.succeeded", {"amount": -5, "currency": "USD"}, "PermanentError: snapshot"),
        ("payment.succeeded", ["not", "an", "object"], "PermanentError: data must be"),
    ],
)
def test_poison_payload_goes_straight_to_dead(
    session_factory: Factory, event_type: str, data: object, error_prefix: str
) -> None:
    event_id = insert_event(session_factory, event_type=event_type, data=data)

    result = make_processor(session_factory)(event_id)

    assert result.outcome is ProcessOutcome.DEAD
    assert result.attempt == 1
    stored = load(session_factory, event_id)
    assert stored.status is S.DEAD
    assert (stored.last_error or "").startswith(error_prefix)
    assert (stored.locked_until, stored.next_attempt_at) == (None, None)
    assert effect_count(session_factory, event_id) == 0
    with session_scope(session_factory) as session:
        assert PaymentProjection(session).get("pay_evt_1") is None
    assert audit(session_factory, event_id)[-1] == (S.PROCESSING, S.DEAD, "permanent_error")
    assert attempts(session_factory, event_id) == [(1, "dead", True)]


# -- crashes and lost claims -------------------------------------------------------


def test_crash_after_claim_leaves_processing_and_an_open_attempt(
    session_factory: Factory,
) -> None:
    event_id = insert_event(session_factory)
    with session_scope(session_factory) as session:  # a worker claims and then dies
        repository = EventRepository(session)
        claimed = repository.claim(event_id, lease_s=60)
        assert claimed is not None
        repository.start_attempt(event_id, claimed.attempts)

    result = make_processor(session_factory)(event_id)

    assert result.outcome is ProcessOutcome.SKIPPED  # the lease still holds
    stored = load(session_factory, event_id)
    assert (stored.status, stored.attempts) == (S.PROCESSING, 1)
    assert attempts(session_factory, event_id) == [(1, None, False)]
    assert effect_count(session_factory, event_id) == 0


def take_over_claim(factory: Factory, event_id: UUID) -> None:
    """What the sweeper plus another worker would leave: same status, newer attempt."""
    with session_scope(factory) as session:
        session.execute(
            text("UPDATE webhook_events SET attempts = attempts + 1 WHERE id = :id"),
            {"id": event_id},
        )


@pytest.mark.parametrize("handler_fails", [False, True])
def test_a_worker_that_lost_its_claim_writes_nothing(
    session_factory: Factory, handler_fails: bool
) -> None:
    event_id = insert_event(session_factory)

    def slow_handler(event: Event) -> Payment:
        take_over_claim(session_factory, event_id)  # our lease "expired" meanwhile
        if handler_fails:
            raise TransientError("late failure")
        return project_event(event)

    result = make_processor(session_factory, slow_handler)(event_id)

    assert result.outcome is ProcessOutcome.LOST
    stored = load(session_factory, event_id)
    assert (stored.status, stored.attempts, stored.last_error) == (S.PROCESSING, 2, None)
    assert effect_count(session_factory, event_id) == 0
    assert audit(session_factory, event_id)[-1] == (S.RECEIVED, S.PROCESSING, "claim")


def test_unknown_event_is_skipped(session_factory: Factory) -> None:
    result = make_processor(session_factory)(UUID(int=0))

    assert result == ProcessResult(ProcessOutcome.SKIPPED)


def test_wiring_builds_a_working_processor_from_settings(
    database_url: str, engine: Engine, session_factory: Factory
) -> None:
    settings = Settings(
        database_url=database_url,
        redis_url="redis://unused",
        broker_url="amqp://unused",
        webhook_secret=SecretStr("unused-secret"),
        admin_api_key=SecretStr("unused-admin-key"),
        lease_s=30,
        max_attempts=2,
    )
    event_id = insert_event(session_factory)

    result = build_process_event(settings, engine)(event_id)

    assert result.outcome is ProcessOutcome.PROCESSED
    assert effect_count(session_factory, event_id) == 1
