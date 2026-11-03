"""ORM models for the schema of SDD section 8.

Alembic owns the schema; these models describe it for typing and for the
"models match migrations" test.
"""

import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Identity,
    Index,
    Integer,
    LargeBinary,
    Text,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.states import EventStatus


def _enum_values(enum_class: type[enum.Enum]) -> list[str]:
    """Store the enum *values* ('received'), not the member names ('RECEIVED')."""
    return [str(member.value) for member in enum_class]


EVENT_STATUS = Enum(EventStatus, name="event_status", values_callable=_enum_values)
EVENT_OUTCOME = Enum(EventOutcome, name="event_outcome", values_callable=_enum_values)
TIMESTAMPTZ = DateTime(timezone=True)


class Base(DeclarativeBase):
    pass


class WebhookEventRow(Base):
    __tablename__ = "webhook_events"
    __table_args__ = (
        UniqueConstraint("provider", "event_id", name="uq_webhook_events_identity"),
        Index("ix_webhook_events_resource", "resource_type", "resource_id"),
        Index("ix_webhook_events_status_next_attempt", "status", "next_attempt_at"),
        Index("ix_webhook_events_status_received", "status", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    provider: Mapped[str] = mapped_column(Text)
    event_id: Mapped[str] = mapped_column(Text)
    event_type: Mapped[str] = mapped_column(Text)
    resource_type: Mapped[str] = mapped_column(Text)
    resource_id: Mapped[str] = mapped_column(Text)
    occurred_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)
    received_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)
    raw_body: Mapped[bytes] = mapped_column(LargeBinary)
    headers: Mapped[dict[str, str]] = mapped_column(JSONB)
    status: Mapped[EventStatus] = mapped_column(EVENT_STATUS)
    outcome: Mapped[EventOutcome | None] = mapped_column(EVENT_OUTCOME)
    attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    next_attempt_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)
    locked_until: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)
    last_error: Mapped[str | None] = mapped_column(Text)
    trace_id: Mapped[str] = mapped_column(Text)
    processed_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)


class EventTransitionRow(Base):
    """Audit trail: every state change with reason and trace id (FR-PRO-8)."""

    __tablename__ = "event_transitions"
    __table_args__ = (Index("ix_event_transitions_event", "event_id", "id"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("webhook_events.id"))
    from_status: Mapped[EventStatus | None] = mapped_column(EVENT_STATUS)
    to_status: Mapped[EventStatus] = mapped_column(EVENT_STATUS)
    reason: Mapped[str] = mapped_column(Text)
    trace_id: Mapped[str | None] = mapped_column(Text)
    at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, server_default=func.now())


class EventAttemptRow(Base):
    """One row per processing attempt. Not unique per attempt_no: replay resets it."""

    __tablename__ = "event_attempts"
    __table_args__ = (Index("ix_event_attempts_event", "event_id", "id"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("webhook_events.id"))
    attempt_no: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)
    result: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)


class PaymentRow(Base):
    """Projection. Everything but the key is nullable: the row is created empty first."""

    __tablename__ = "payments"

    resource_id: Mapped[str] = mapped_column(Text, primary_key=True)
    status: Mapped[str | None] = mapped_column(Text)
    amount: Mapped[int | None] = mapped_column(BigInteger)
    currency: Mapped[str | None] = mapped_column(Text)
    last_occurred_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)
    last_event_id: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, server_default=func.now())


class AppliedEffectRow(Base):
    """Written with the projection; a second insert for one event is impossible."""

    __tablename__ = "applied_effects"

    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("webhook_events.id"), primary_key=True)
