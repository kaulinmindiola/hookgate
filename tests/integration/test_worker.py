"""A real Celery worker (thread pool, ADR-012) against a real PostgreSQL.

The broker is Celery's in-memory transport, so the test needs no RabbitMQ; the task
code, the wiring and the delivery settings are the production ones. Delivery through
a real RabbitMQ and killing workers are covered by the chaos suite (phase F11).
"""

import json
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID

import pytest
from celery import Celery
from celery.contrib.testing.worker import start_worker
from pydantic import SecretStr
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from hookgate.application.records import NewEvent
from hookgate.config import Settings
from hookgate.domain.errors import TransientError
from hookgate.domain.events import Event, EventIdentity
from hookgate.domain.payments import Payment, project_event
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.celery_app import TASK_SWEEP
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.repositories import EventRepository
from hookgate.infrastructure.queue import CeleryPublisher
from hookgate.wiring import build_celery_app

pytestmark = pytest.mark.integration

Factory = sessionmaker[Session]


def insert_event(factory: Factory, event_id: str) -> UUID:
    raw = json.dumps(
        {
            "id": event_id,
            "type": "payment.succeeded",
            "occurred_at": "2026-11-03T11:59:00Z",
            "resource": {"type": "payment", "id": f"pay_{event_id}"},
            "data": {"amount": 1500, "currency": "USD"},
        }
    ).encode()
    new = NewEvent(
        identity=EventIdentity("sim", event_id),
        event_type="payment.succeeded",
        resource_type="payment",
        resource_id=f"pay_{event_id}",
        occurred_at=datetime(2026, 11, 3, 11, 59, tzinfo=UTC),
        received_at=datetime.now(UTC),
        raw_body=raw,
        headers={},
        trace_id=f"trace-{event_id}",
    )
    with session_scope(factory) as session:
        stored_id = EventRepository(session).insert_if_new(new)
    assert stored_id is not None
    return stored_id


def wait_for_status(
    factory: Factory, event_id: UUID, status: EventStatus, timeout_s: float = 15.0
) -> None:
    deadline = time.monotonic() + timeout_s
    current = None
    while time.monotonic() < deadline:
        with session_scope(factory) as session:
            stored = EventRepository(session).get(event_id)
        current = stored.status if stored else None
        if current is status:
            return
        time.sleep(0.05)
    raise AssertionError(f"event stayed {current}, expected {status}")


class ScriptedHandler:
    """The production handler, plus a number of transient failures a test can request."""

    def __init__(self) -> None:
        self.failures_left = 0
        self.calls = 0

    def __call__(self, event: Event) -> Payment:
        self.calls += 1
        if self.failures_left > 0:
            self.failures_left -= 1
            raise TransientError("scripted failure")
        return project_event(event)


@pytest.fixture(scope="module")
def handler() -> ScriptedHandler:
    return ScriptedHandler()


@pytest.fixture(scope="module")
def worker_app(database_url: str, engine: Engine, handler: ScriptedHandler) -> Iterator[Celery]:
    """One worker for the module (stopping a worker takes a few seconds)."""
    settings = Settings(
        database_url=database_url,
        redis_url="redis://unused",
        broker_url="memory://",
        webhook_secret=SecretStr("unused-secret"),
        admin_api_key=SecretStr("unused-admin-key"),
        retry_base_s=0.2,
        retry_cap_s=0.5,
    )
    app = build_celery_app(settings, engine, handler=handler)
    # The in-memory transport polls; poll often so the test does not idle.
    app.conf.broker_transport_options = {"max_retries": 0, "polling_interval": 0.05}
    with start_worker(
        app, pool="threads", concurrency=4, perform_ping_check=False, loglevel="WARNING"
    ):
        yield app


@pytest.fixture(autouse=True)
def reset_handler(handler: ScriptedHandler) -> None:
    handler.failures_left = 0
    handler.calls = 0


def test_published_event_is_processed_by_the_worker(
    session_factory: Factory, worker_app: Celery
) -> None:
    event_id = insert_event(session_factory, "evt_worker")

    CeleryPublisher(worker_app).publish(event_id, trace_id="trace-evt_worker")

    wait_for_status(session_factory, event_id, EventStatus.PROCESSED)


def test_duplicate_messages_apply_the_effect_once(
    session_factory: Factory, worker_app: Celery
) -> None:
    event_id = insert_event(session_factory, "evt_dup")
    publisher = CeleryPublisher(worker_app)

    for _ in range(5):
        publisher.publish(event_id, trace_id="t")
    wait_for_status(session_factory, event_id, EventStatus.PROCESSED)
    time.sleep(0.5)  # let the remaining duplicate messages run

    with session_scope(session_factory) as session:
        effects = int(session.execute(text("SELECT count(*) FROM applied_effects")).scalar_one())
        attempts = int(session.execute(text("SELECT count(*) FROM event_attempts")).scalar_one())
    assert (effects, attempts) == (1, 1)


def test_transient_failure_is_retried_on_time_by_a_delayed_message(
    session_factory: Factory, worker_app: Celery, handler: ScriptedHandler
) -> None:
    handler.failures_left = 1
    event_id = insert_event(session_factory, "evt_retry")

    CeleryPublisher(worker_app).publish(event_id, trace_id="t")

    wait_for_status(session_factory, event_id, EventStatus.PROCESSED)
    assert handler.calls == 2


def test_sweep_task_republishes_a_lost_event(session_factory: Factory, worker_app: Celery) -> None:
    event_id = insert_event(session_factory, "evt_lost")  # its first message never existed
    with session_scope(session_factory) as session:
        session.execute(
            text(
                "UPDATE webhook_events SET received_at = now() - interval '5 minutes' "
                "WHERE id = :id"
            ),
            {"id": event_id},
        )

    worker_app.send_task(TASK_SWEEP)

    wait_for_status(session_factory, event_id, EventStatus.PROCESSED)
