"""Ports: what the use cases need from the outside world (SDD 6.1).

Adapters live in ``hookgate.infrastructure``; tests use in-memory fakes.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from typing import Literal, Protocol
from uuid import UUID

from hookgate.application.records import (
    AttemptRecord,
    DueEvent,
    EventQuery,
    NewEvent,
    PageKey,
    StoredEvent,
    TransitionRecord,
)
from hookgate.domain.events import EventIdentity
from hookgate.domain.ordering import OrderingKey
from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.payments import Payment
from hookgate.domain.states import TransitionResult


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


class ProcessingEvents(Protocol):
    """The slice of the event repository that processing uses (SDD 6.3, ADR-011)."""

    def claim(self, event_id: UUID, *, lease_s: int) -> StoredEvent | None: ...

    def start_attempt(self, event_id: UUID, attempt_no: int) -> int: ...

    def finish_attempt(self, attempt_id: int, *, result: str, error: str | None) -> None: ...

    def apply_transition(
        self,
        event_id: UUID,
        result: TransitionResult,
        *,
        retry_after: timedelta | None = None,
        last_error: str | None = None,
        outcome: EventOutcome | None = None,
        expected_attempts: int | None = None,
    ) -> bool: ...


class PaymentEffects(Protocol):
    def lock_resource(self, resource_id: str) -> OrderingKey | None:
        """Lock the resource row until commit; return the last applied key (None: none)."""
        ...

    def apply(self, payment: Payment, event_id: UUID) -> None:
        """Write the projection and the ``applied_effects`` row (same transaction)."""
        ...


class ProcessingUnit(Protocol):
    """Repositories that share one transaction."""

    @property
    def events(self) -> ProcessingEvents: ...

    @property
    def payments(self) -> PaymentEffects: ...


# Opens one transaction: commits when the block exits cleanly, rolls back otherwise.
ProcessingScope = Callable[[], AbstractContextManager[ProcessingUnit]]


class SweepEvents(Protocol):
    """The slice of the event repository that the sweeper uses (FR-PRO-7, SDD 12)."""

    def lock_expired_leases(self, *, limit: int) -> list[StoredEvent]: ...

    def lock_due_for_publish(self, *, limit: int, received_grace_s: int) -> list[DueEvent]: ...

    def close_open_attempts(self, event_id: UUID, *, result: str, error: str) -> None: ...

    def apply_transition(
        self,
        event_id: UUID,
        result: TransitionResult,
        *,
        retry_after: timedelta | None = None,
        last_error: str | None = None,
        outcome: EventOutcome | None = None,
        expected_attempts: int | None = None,
    ) -> bool: ...


# Opens one transaction: commits when the block exits cleanly, rolls back otherwise.
SweepScope = Callable[[], AbstractContextManager[SweepEvents]]


class AdminEvents(Protocol):
    """The slice of the event repository that the admin use cases use (FR-DLQ-1..4)."""

    def list_events(
        self, query: EventQuery, *, limit: int, after: PageKey | None = None
    ) -> list[StoredEvent]: ...

    def get(self, event_id: UUID) -> StoredEvent | None: ...

    def list_transitions(self, event_id: UUID) -> list[TransitionRecord]: ...

    def list_attempts(self, event_id: UUID) -> list[AttemptRecord]: ...

    def apply_transition(
        self,
        event_id: UUID,
        result: TransitionResult,
        *,
        retry_after: timedelta | None = None,
        last_error: str | None = None,
        outcome: EventOutcome | None = None,
        expected_attempts: int | None = None,
    ) -> bool: ...


# Opens one transaction: commits when the block exits cleanly, rolls back otherwise.
AdminScope = Callable[[], AbstractContextManager[AdminEvents]]
