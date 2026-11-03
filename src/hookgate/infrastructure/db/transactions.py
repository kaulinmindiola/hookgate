"""Transaction scopes that hand repositories to the use cases."""

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy.orm import Session, sessionmaker

from hookgate.application.ports import EventStore, EventStoreScope
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.repositories import EventRepository


def event_store_scope(factory: sessionmaker[Session]) -> EventStoreScope:
    """One database transaction per ``with`` block, exposing the event repository."""

    @contextmanager
    def scope() -> Iterator[EventStore]:
        with session_scope(factory) as session:
            yield EventRepository(session)

    return scope
