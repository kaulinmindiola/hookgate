"""Helpers shared by the chaos tests: stored events, signed deliveries, a live API."""

import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
from prometheus_client import CollectorRegistry
from pydantic import SecretStr
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker
from tests.chaos.services import wait_until
from tests.live_server import live_server

from hookgate.api.main import create_app
from hookgate.application.health import Readiness
from hookgate.application.ports import DedupeCache, EventPublisher, IngestionMetrics
from hookgate.application.records import AttemptRecord, NewEvent, StoredEvent
from hookgate.config import Settings
from hookgate.domain.events import EventIdentity
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.repositories import EventRepository
from simulator.client import envelope, signature_header
from simulator.model import EventSpec

SECRET = "chaos-webhook-secret"
BASE = datetime(2026, 11, 4, 12, 0, tzinfo=UTC)

Factory = sessionmaker[Session]


def spec(event_id: str, minute: int = 0) -> EventSpec:
    return EventSpec(
        event_id, "payment.succeeded", f"pay_{event_id}", BASE + timedelta(minutes=minute), 1500
    )


def insert_event(factory: Factory, event_id: str) -> UUID:
    """Store an event as ingestion would (status ``received``), without HTTP."""
    event = spec(event_id)
    new = NewEvent(
        identity=EventIdentity("sim", event_id),
        event_type=event.event_type,
        resource_type="payment",
        resource_id=event.resource_id,
        occurred_at=event.occurred_at,
        received_at=datetime.now(UTC),
        raw_body=envelope(event),
        headers={},
        trace_id=f"trace-{event_id}",
    )
    with session_scope(factory) as session:
        stored_id = EventRepository(session).insert_if_new(new)
    assert stored_id is not None
    return stored_id


def load(factory: Factory, event_id: UUID) -> StoredEvent:
    with session_scope(factory) as session:
        stored = EventRepository(session).get(event_id)
    assert stored is not None
    return stored


def attempts(factory: Factory, event_id: UUID) -> list[AttemptRecord]:
    with session_scope(factory) as session:
        return EventRepository(session).list_attempts(event_id)


def count(factory: Factory, table: str) -> int:
    with session_scope(factory) as session:
        return int(session.execute(text(f"SELECT count(*) FROM {table}")).scalar_one())


def find(factory: Factory, provider_event_id: str) -> StoredEvent | None:
    with session_scope(factory) as session:
        row = session.execute(
            text("SELECT id FROM webhook_events WHERE event_id = :e"), {"e": provider_event_id}
        ).first()
        return None if row is None else EventRepository(session).get(row[0])


def wait_for_status(
    factory: Factory, provider_event_id: str, status: EventStatus, *, timeout_s: float = 60
) -> None:
    def reached() -> bool:
        stored = find(factory, provider_event_id)
        return stored is not None and stored.status is status

    wait_until(reached, timeout_s=timeout_s, what=f"{provider_event_id} to be {status.value}")


def deliver(client: httpx.Client, event: EventSpec) -> tuple[int, str, float]:
    """One signed delivery; returns status code, ``status``/``error`` and seconds taken."""
    body = envelope(event)
    headers = {
        "Content-Type": "application/json",
        "Hookgate-Signature": signature_header(SECRET, int(time.time()), body),
    }
    started = time.monotonic()
    response = client.post("/webhooks/payments", content=body, headers=headers)
    elapsed = time.monotonic() - started
    payload = response.json()
    return response.status_code, str(payload.get("status") or payload.get("error")), elapsed


@contextmanager
def live_api(
    database_url: str,
    engine: Engine,
    *,
    cache: DedupeCache,
    publisher: EventPublisher,
    readiness: Readiness,
    metrics: IngestionMetrics | None = None,
) -> Iterator[httpx.Client]:
    """The production app on a real uvicorn server, with the given collaborators."""
    settings = Settings(
        database_url=database_url,
        redis_url="redis://unused",
        broker_url="amqp://unused:unused@127.0.0.1:1//",
        webhook_secret=SecretStr(SECRET),
        admin_api_key=SecretStr("chaos-admin-key"),
    )
    app = create_app(
        settings,
        engine=engine,
        cache=cache,
        publisher=publisher,
        metrics=metrics,
        metrics_registry=CollectorRegistry(),
        readiness=readiness,
    )
    with live_server(app) as url, httpx.Client(base_url=url, timeout=30) as client:
        yield client
