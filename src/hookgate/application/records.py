"""Plain records exchanged between use cases and repositories (no I/O)."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from hookgate.domain.events import EventIdentity
from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.states import EventStatus
from hookgate.domain.timeutil import require_aware


@dataclass(frozen=True, slots=True)
class NewEvent:
    """What ingestion persists before answering 202 (FR-ING-5)."""

    identity: EventIdentity
    event_type: str
    resource_type: str
    resource_id: str
    occurred_at: datetime
    received_at: datetime
    raw_body: bytes
    headers: Mapping[str, str]
    trace_id: str

    def __post_init__(self) -> None:
        require_aware(self.occurred_at, "occurred_at")
        require_aware(self.received_at, "received_at")


@dataclass(frozen=True, slots=True)
class StoredEvent:
    id: UUID
    provider: str
    event_id: str
    event_type: str
    resource_type: str
    resource_id: str
    occurred_at: datetime
    received_at: datetime
    raw_body: bytes
    headers: Mapping[str, str]
    status: EventStatus
    outcome: EventOutcome | None
    attempts: int
    next_attempt_at: datetime | None
    locked_until: datetime | None
    last_error: str | None
    trace_id: str
    processed_at: datetime | None


@dataclass(frozen=True, slots=True)
class TransitionRecord:
    """One row of the audit table ``event_transitions``."""

    from_status: EventStatus | None
    to_status: EventStatus
    reason: str
    trace_id: str | None
    at: datetime


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    """One row of ``event_attempts``. ``finished_at`` None means the worker never finished."""

    attempt_no: int
    started_at: datetime
    finished_at: datetime | None
    result: str | None
    error: str | None


@dataclass(frozen=True, slots=True)
class PaymentRecord:
    """One row of the ``payments`` projection."""

    resource_id: str
    status: str | None
    amount: int | None
    currency: str | None
    last_occurred_at: datetime | None
    last_event_id: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class DueEvent:
    """An event the sweeper must hand to the queue again (FR-PRO-7)."""

    id: UUID
    trace_id: str
