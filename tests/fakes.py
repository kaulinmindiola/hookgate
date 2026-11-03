"""In-memory fakes for the application ports (no database, no broker)."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from uuid import UUID, uuid4

from hookgate.application.ports import EventStore
from hookgate.application.records import NewEvent
from hookgate.domain.events import EventIdentity


class FixedClock:
    def __init__(self, now: datetime) -> None:
        self.current = now

    def now(self) -> datetime:
        return self.current


class InMemoryEvents:
    """Fake ``EventStore`` plus its scope. ``journal`` records commits for ordering checks."""

    def __init__(self, journal: list[str] | None = None) -> None:
        self.rows: dict[EventIdentity, tuple[UUID, NewEvent]] = {}
        self.journal = journal if journal is not None else []

    def insert_if_new(self, new: NewEvent) -> UUID | None:
        if new.identity in self.rows:
            return None
        stored_id = uuid4()
        self.rows[new.identity] = (stored_id, new)
        return stored_id

    def find_raw_body(self, identity: EventIdentity) -> bytes | None:
        row = self.rows.get(identity)
        return None if row is None else row[1].raw_body

    def stored(self, event_id: str, provider: str = "sim") -> NewEvent:
        return self.rows[EventIdentity(provider, event_id)][1]

    @contextmanager
    def scope(self) -> Iterator[EventStore]:
        yield self
        self.journal.append("commit")


class RecordingPublisher:
    def __init__(self, journal: list[str] | None = None, *, fail: bool = False) -> None:
        self.published: list[tuple[UUID, str]] = []
        self.journal = journal if journal is not None else []
        self.fail = fail

    def publish(self, event_id: UUID, *, trace_id: str) -> None:
        self.journal.append("publish")
        if self.fail:
            raise ConnectionError("broker unreachable")
        self.published.append((event_id, trace_id))
