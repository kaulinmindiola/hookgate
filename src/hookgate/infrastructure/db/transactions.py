"""Transaction scopes that hand repositories to the use cases."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from sqlalchemy.orm import Session, sessionmaker

from hookgate.application.ports import (
    AdminEvents,
    AdminScope,
    EventStore,
    EventStoreScope,
    ProcessingScope,
    ProcessingUnit,
    SweepEvents,
    SweepScope,
)
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.payments import PaymentProjection
from hookgate.infrastructure.db.repositories import EventRepository


def event_store_scope(factory: sessionmaker[Session]) -> EventStoreScope:
    """One database transaction per ``with`` block, exposing the event repository."""

    @contextmanager
    def scope() -> Iterator[EventStore]:
        with session_scope(factory) as session:
            yield EventRepository(session)

    return scope


@dataclass(frozen=True, slots=True)
class SqlProcessingUnit:
    events: EventRepository
    payments: PaymentProjection


def processing_scope(factory: sessionmaker[Session]) -> ProcessingScope:
    """One database transaction per ``with`` block, exposing events and payments."""

    @contextmanager
    def scope() -> Iterator[ProcessingUnit]:
        with session_scope(factory) as session:
            yield SqlProcessingUnit(EventRepository(session), PaymentProjection(session))

    return scope


def sweep_scope(factory: sessionmaker[Session]) -> SweepScope:
    """One database transaction per ``with`` block, exposing the sweeper's queries."""

    @contextmanager
    def scope() -> Iterator[SweepEvents]:
        with session_scope(factory) as session:
            yield EventRepository(session)

    return scope


def admin_scope(factory: sessionmaker[Session]) -> AdminScope:
    """One database transaction per ``with`` block, exposing the admin queries and replay."""

    @contextmanager
    def scope() -> Iterator[AdminEvents]:
        with session_scope(factory) as session:
            yield EventRepository(session)

    return scope


def status_counts(factory: sessionmaker[Session]) -> Callable[[], dict[EventStatus, int]]:
    """A short read transaction per call, counting events by status (metrics gauge)."""

    def counts() -> dict[EventStatus, int]:
        with session_scope(factory) as session:
            return EventRepository(session).count_by_status()

    return counts
