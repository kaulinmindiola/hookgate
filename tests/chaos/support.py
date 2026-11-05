"""Helpers shared by the chaos tests: stored events and simple database reads."""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from hookgate.application.records import AttemptRecord, NewEvent, StoredEvent
from hookgate.domain.events import EventIdentity
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.repositories import EventRepository
from simulator.client import envelope
from simulator.model import EventSpec

BASE = datetime(2026, 11, 4, 12, 0, tzinfo=UTC)

Factory = sessionmaker[Session]


def spec(event_id: str) -> EventSpec:
    return EventSpec(event_id, "payment.succeeded", f"pay_{event_id}", BASE, 1500)


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
