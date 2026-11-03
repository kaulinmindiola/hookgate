"""Ports: what the use cases need from the outside world (SDD 6.1).

Adapters live in ``hookgate.infrastructure``; tests use in-memory fakes.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from hookgate.application.records import NewEvent
from hookgate.domain.events import EventIdentity


class Clock(Protocol):
    def now(self) -> datetime:
        """Current time, timezone-aware."""
        ...


class EventStore(Protocol):
    """The slice of the event repository that ingestion uses."""

    def insert_if_new(self, new: NewEvent) -> UUID | None:
        """Return the new internal id, or None when ``(provider, event_id)`` exists."""
        ...

    def find_raw_body(self, identity: EventIdentity) -> bytes | None: ...


# Opens one transaction: commits when the block exits cleanly, rolls back otherwise.
EventStoreScope = Callable[[], AbstractContextManager[EventStore]]


class EventPublisher(Protocol):
    """Hands a stored event to the processing queue (Celery adapter in F7)."""

    def publish(self, event_id: UUID, *, trace_id: str) -> None: ...


class DedupeCache(Protocol):
    """Short-lived markers for events already stored (ADR-003, FR-IDE-3).

    Only a fast path: PostgreSQL decides. Any method may raise on an outage, and the
    caller must degrade to PostgreSQL instead of failing the delivery (FR-IDE-4).
    """

    def seen(self, identity: EventIdentity) -> bool: ...

    def remember(self, identity: EventIdentity) -> None:
        """Write the marker. Call it only after the event row is committed."""
        ...


CacheOp = Literal["get", "set"]


class IngestionMetrics(Protocol):
    """Counters recorded by ingestion. The Prometheus adapter arrives in phase F9."""

    def cache_error(self, op: CacheOp) -> None: ...
