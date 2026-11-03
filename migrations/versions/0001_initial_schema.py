"""Initial schema (SDD section 8).

Revision ID: 0001
Revises:
Create Date: 2026-11-03
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Types are created and dropped explicitly: they are shared by several tables.
EVENT_STATUS = postgresql.ENUM(
    "received",
    "processing",
    "processed",
    "failed",
    "dead",
    name="event_status",
    create_type=False,
)
EVENT_OUTCOME = postgresql.ENUM(
    "applied",
    "ignored_stale",
    name="event_outcome",
    create_type=False,
)
TIMESTAMPTZ = sa.DateTime(timezone=True)
NOW = sa.text("now()")


def upgrade() -> None:
    bind = op.get_bind()
    EVENT_STATUS.create(bind, checkfirst=False)
    EVENT_OUTCOME.create(bind, checkfirst=False)

    op.create_table(
        "webhook_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("resource_type", sa.Text(), nullable=False),
        sa.Column("resource_id", sa.Text(), nullable=False),
        sa.Column("occurred_at", TIMESTAMPTZ, nullable=False),
        sa.Column("received_at", TIMESTAMPTZ, nullable=False),
        sa.Column("raw_body", sa.LargeBinary(), nullable=False),
        sa.Column("headers", postgresql.JSONB(), nullable=False),
        sa.Column("status", EVENT_STATUS, nullable=False),
        sa.Column("outcome", EVENT_OUTCOME, nullable=True),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("next_attempt_at", TIMESTAMPTZ, nullable=True),
        sa.Column("locked_until", TIMESTAMPTZ, nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("trace_id", sa.Text(), nullable=False),
        sa.Column("processed_at", TIMESTAMPTZ, nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("provider", "event_id", name="uq_webhook_events_identity"),
    )
    op.create_index(
        "ix_webhook_events_resource", "webhook_events", ["resource_type", "resource_id"]
    )
    op.create_index(
        "ix_webhook_events_status_next_attempt", "webhook_events", ["status", "next_attempt_at"]
    )
    op.create_index(
        "ix_webhook_events_status_received", "webhook_events", ["status", "received_at"]
    )

    op.create_table(
        "event_transitions",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("from_status", EVENT_STATUS, nullable=True),
        sa.Column("to_status", EVENT_STATUS, nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("trace_id", sa.Text(), nullable=True),
        sa.Column("at", TIMESTAMPTZ, server_default=NOW, nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["webhook_events.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_event_transitions_event", "event_transitions", ["event_id", "id"])

    op.create_table(
        "event_attempts",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("started_at", TIMESTAMPTZ, nullable=False),
        sa.Column("finished_at", TIMESTAMPTZ, nullable=True),
        sa.Column("result", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["event_id"], ["webhook_events.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_event_attempts_event", "event_attempts", ["event_id", "id"])

    op.create_table(
        "payments",
        sa.Column("resource_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("amount", sa.BigInteger(), nullable=True),
        sa.Column("currency", sa.Text(), nullable=True),
        sa.Column("last_occurred_at", TIMESTAMPTZ, nullable=True),
        sa.Column("last_event_id", sa.Text(), nullable=True),
        sa.Column("updated_at", TIMESTAMPTZ, server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("resource_id"),
    )

    op.create_table(
        "applied_effects",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["webhook_events.id"]),
        sa.PrimaryKeyConstraint("event_id"),
    )


def downgrade() -> None:
    op.drop_table("applied_effects")
    op.drop_table("payments")
    op.drop_table("event_attempts")
    op.drop_table("event_transitions")
    op.drop_table("webhook_events")

    bind = op.get_bind()
    EVENT_OUTCOME.drop(bind, checkfirst=False)
    EVENT_STATUS.drop(bind, checkfirst=False)
