"""Request and response bodies of the HTTP API."""

import base64
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from hookgate.application.health import CheckStatus, HealthStatus
from hookgate.application.ingest_event import IngestStatus
from hookgate.application.records import AttemptRecord, StoredEvent, TransitionRecord
from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.states import EventStatus


class WebhookAck(BaseModel):
    status: IngestStatus


class ErrorBody(BaseModel):
    error: str
    detail: list[str] | None = None


class HealthBody(BaseModel):
    status: str


class ReadinessBody(BaseModel):
    status: HealthStatus
    checks: dict[str, CheckStatus]


# -- admin (FR-DLQ-1..3) -----------------------------------------------------------


class EventSummary(BaseModel):
    id: UUID
    provider: str
    event_id: str
    event_type: str
    resource_type: str
    resource_id: str
    status: EventStatus
    outcome: EventOutcome | None
    attempts: int
    occurred_at: datetime
    received_at: datetime
    next_attempt_at: datetime | None
    processed_at: datetime | None
    last_error: str | None
    trace_id: str

    @classmethod
    def from_stored(cls, event: StoredEvent) -> "EventSummary":
        return cls(
            id=event.id,
            provider=event.provider,
            event_id=event.event_id,
            event_type=event.event_type,
            resource_type=event.resource_type,
            resource_id=event.resource_id,
            status=event.status,
            outcome=event.outcome,
            attempts=event.attempts,
            occurred_at=event.occurred_at,
            received_at=event.received_at,
            next_attempt_at=event.next_attempt_at,
            processed_at=event.processed_at,
            last_error=event.last_error,
            trace_id=event.trace_id,
        )


class EventPageBody(BaseModel):
    items: list[EventSummary]
    next_cursor: str | None


class TransitionBody(BaseModel):
    from_status: EventStatus | None
    to_status: EventStatus
    reason: str
    trace_id: str | None
    at: datetime

    @classmethod
    def from_record(cls, record: TransitionRecord) -> "TransitionBody":
        return cls(
            from_status=record.from_status,
            to_status=record.to_status,
            reason=record.reason,
            trace_id=record.trace_id,
            at=record.at,
        )


class AttemptBody(BaseModel):
    attempt_no: int
    started_at: datetime
    finished_at: datetime | None
    result: str | None
    error: str | None

    @classmethod
    def from_record(cls, record: AttemptRecord) -> "AttemptBody":
        return cls(
            attempt_no=record.attempt_no,
            started_at=record.started_at,
            finished_at=record.finished_at,
            result=record.result,
            error=record.error,
        )


class EventDetailBody(EventSummary):
    # The exact stored bytes (base64) and, for reading, the same bytes as text.
    raw_body_base64: str
    raw_body: str
    headers: dict[str, str]
    locked_until: datetime | None
    transitions: list[TransitionBody]
    attempts_history: list[AttemptBody]

    @classmethod
    def build(
        cls,
        event: StoredEvent,
        transitions: list[TransitionRecord],
        attempts: list[AttemptRecord],
    ) -> "EventDetailBody":
        summary = EventSummary.from_stored(event).model_dump()
        return cls(
            **summary,
            raw_body_base64=base64.b64encode(event.raw_body).decode("ascii"),
            raw_body=event.raw_body.decode("utf-8", errors="replace"),
            headers=dict(event.headers),
            locked_until=event.locked_until,
            transitions=[TransitionBody.from_record(t) for t in transitions],
            attempts_history=[AttemptBody.from_record(a) for a in attempts],
        )


class ReplayAck(BaseModel):
    status: EventStatus
    enqueued: bool
