"""Event repository: idempotent insert, atomic claim, compare-and-set transitions.

Methods never commit: the caller owns the transaction (``session_scope``).
Time comparisons and stored times use the database clock (``now()``), which is
authoritative (ADR-011, decision P3): tests move time by back-dating columns.
"""

import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any, cast

from sqlalchemy import Row, Table, and_, func, insert, or_, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from hookgate.application.records import (
    AttemptRecord,
    DueEvent,
    NewEvent,
    StoredEvent,
    TransitionRecord,
)
from hookgate.domain.errors import InvalidTransition
from hookgate.domain.events import EventIdentity
from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.states import (
    AttemptsAction,
    ColumnAction,
    EventStatus,
    TransitionResult,
    Trigger,
    transition,
)
from hookgate.infrastructure.db.models import EventAttemptRow, EventTransitionRow, WebhookEventRow

_EVENTS = cast(Table, WebhookEventRow.__table__)
_TRANSITIONS = cast(Table, EventTransitionRow.__table__)
_ATTEMPTS = cast(Table, EventAttemptRow.__table__)


def _claim_rule() -> tuple[tuple[EventStatus, ...], EventStatus]:
    """Derive the claim sources and target from the domain table (one rule, one place)."""
    sources: list[EventStatus] = []
    targets: set[EventStatus] = set()
    for status in EventStatus:
        try:
            result = transition(status, Trigger.CLAIM)
        except InvalidTransition:
            continue
        sources.append(status)
        targets.add(result.to_status)
    (target,) = targets
    return tuple(sources), target


_CLAIM_SOURCES, _CLAIM_TARGET = _claim_rule()


def _to_stored(row: Row[Any]) -> StoredEvent:
    data = row._mapping
    return StoredEvent(
        id=data["id"],
        provider=data["provider"],
        event_id=data["event_id"],
        event_type=data["event_type"],
        resource_type=data["resource_type"],
        resource_id=data["resource_id"],
        occurred_at=data["occurred_at"],
        received_at=data["received_at"],
        raw_body=bytes(data["raw_body"]),
        headers=dict(data["headers"]),
        status=data["status"],
        outcome=data["outcome"],
        attempts=data["attempts"],
        next_attempt_at=data["next_attempt_at"],
        locked_until=data["locked_until"],
        last_error=data["last_error"],
        trace_id=data["trace_id"],
        processed_at=data["processed_at"],
    )


class EventRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    # -- ingestion ---------------------------------------------------------------

    def insert_if_new(self, new: NewEvent) -> uuid.UUID | None:
        """Insert the event unless ``(provider, event_id)`` exists.

        Returns the new internal id, or None for a duplicate (FR-IDE-2). On insert it
        also writes the audit row null -> received in the same transaction.
        """
        statement = (
            pg_insert(_EVENTS)
            .values(
                id=uuid.uuid4(),
                provider=new.identity.provider,
                event_id=new.identity.event_id,
                event_type=new.event_type,
                resource_type=new.resource_type,
                resource_id=new.resource_id,
                occurred_at=new.occurred_at,
                received_at=new.received_at,
                raw_body=new.raw_body,
                headers=dict(new.headers),
                status=EventStatus.RECEIVED,
                attempts=0,
                trace_id=new.trace_id,
            )
            .on_conflict_do_nothing(index_elements=["provider", "event_id"])
            .returning(_EVENTS.c.id)
        )
        inserted: uuid.UUID | None = self._session.execute(statement).scalar_one_or_none()
        if inserted is None:
            return None
        self._record_transition(inserted, None, EventStatus.RECEIVED, "ingested", new.trace_id)
        return inserted

    def find_raw_body(self, identity: EventIdentity) -> bytes | None:
        """Stored body of an existing event (used to detect FR-IDE-6 mismatches)."""
        statement = select(_EVENTS.c.raw_body).where(
            _EVENTS.c.provider == identity.provider,
            _EVENTS.c.event_id == identity.event_id,
        )
        value = self._session.execute(statement).scalar_one_or_none()
        return None if value is None else bytes(value)

    # -- reads -------------------------------------------------------------------

    def get(self, event_id: uuid.UUID) -> StoredEvent | None:
        row = self._session.execute(select(_EVENTS).where(_EVENTS.c.id == event_id)).first()
        return None if row is None else _to_stored(row)

    def list_by_status(
        self,
        statuses: Sequence[EventStatus],
        *,
        limit: int,
        after: tuple[datetime, uuid.UUID] | None = None,
    ) -> list[StoredEvent]:
        """Page through events ordered by ``(received_at, id)`` (keyset pagination)."""
        statement = select(_EVENTS).where(_EVENTS.c.status.in_(list(statuses)))
        if after is not None:
            statement = statement.where(
                tuple_(_EVENTS.c.received_at, _EVENTS.c.id) > tuple_(after[0], after[1])
            )
        statement = statement.order_by(_EVENTS.c.received_at, _EVENTS.c.id).limit(limit)
        return [_to_stored(row) for row in self._session.execute(statement)]

    def list_transitions(self, event_id: uuid.UUID) -> list[TransitionRecord]:
        statement = (
            select(_TRANSITIONS)
            .where(_TRANSITIONS.c.event_id == event_id)
            .order_by(_TRANSITIONS.c.id)
        )
        return [
            TransitionRecord(
                from_status=row._mapping["from_status"],
                to_status=row._mapping["to_status"],
                reason=row._mapping["reason"],
                trace_id=row._mapping["trace_id"],
                at=row._mapping["at"],
            )
            for row in self._session.execute(statement)
        ]

    def list_attempts(self, event_id: uuid.UUID) -> list[AttemptRecord]:
        statement = (
            select(_ATTEMPTS).where(_ATTEMPTS.c.event_id == event_id).order_by(_ATTEMPTS.c.id)
        )
        return [
            AttemptRecord(
                attempt_no=row._mapping["attempt_no"],
                started_at=row._mapping["started_at"],
                finished_at=row._mapping["finished_at"],
                result=row._mapping["result"],
                error=row._mapping["error"],
            )
            for row in self._session.execute(statement)
        ]

    # -- processing --------------------------------------------------------------

    def claim(self, event_id: uuid.UUID, *, lease_s: int) -> StoredEvent | None:
        """Atomic compare-and-set claim (SDD 6.3). None means 'not yours or not due'.

        One UPDATE per source status, so the audit row carries the exact from_status.
        ``next_attempt_at`` and the lease use the database clock (``now()`` is the start
        time of the current transaction: commit the claim before doing the work).
        """
        if lease_s < 1:
            raise ValueError("lease_s must be >= 1")
        for source in _CLAIM_SOURCES:
            statement = (
                update(_EVENTS)
                .where(
                    _EVENTS.c.id == event_id,
                    _EVENTS.c.status == source,
                    or_(
                        _EVENTS.c.next_attempt_at.is_(None),
                        _EVENTS.c.next_attempt_at <= func.now(),
                    ),
                )
                .values(
                    status=_CLAIM_TARGET,
                    attempts=_EVENTS.c.attempts + 1,
                    locked_until=func.now() + timedelta(seconds=lease_s),
                )
                .returning(_EVENTS)
            )
            row = self._session.execute(statement).first()
            if row is not None:
                stored = _to_stored(row)
                self._record_transition(
                    stored.id, source, _CLAIM_TARGET, Trigger.CLAIM.value, stored.trace_id
                )
                return stored
        return None

    def apply_transition(
        self,
        event_id: uuid.UUID,
        result: TransitionResult,
        *,
        next_attempt_at: datetime | None = None,
        retry_after: timedelta | None = None,
        last_error: str | None = None,
        outcome: EventOutcome | None = None,
        expected_attempts: int | None = None,
    ) -> bool:
        """Apply a domain decision with a compare-and-set on ``result.from_status``.

        Returns False (and writes nothing) when the event is no longer in that status.
        ``expected_attempts`` also pins the attempt number: a worker whose lease expired
        and whose event was claimed again (higher ``attempts``) can no longer write.
        A retry is scheduled either at ``next_attempt_at`` or at ``now() + retry_after``
        (database clock). Claims go through ``claim()``: they need the database clock.
        """
        if result.trigger is Trigger.CLAIM:
            raise ValueError("claims must go through claim()")

        values: dict[str, Any] = {"status": result.to_status}

        if result.attempts is AttemptsAction.RESET:
            values["attempts"] = 0
        elif result.attempts is AttemptsAction.INCREMENT:
            values["attempts"] = _EVENTS.c.attempts + 1

        if result.locked_until is ColumnAction.CLEAR:
            values["locked_until"] = None
        elif result.locked_until is ColumnAction.SET:
            raise ValueError("only claim() sets the lease")

        if result.next_attempt_at is ColumnAction.CLEAR:
            values["next_attempt_at"] = None
        elif result.next_attempt_at is ColumnAction.SET:
            if (next_attempt_at is None) == (retry_after is None):
                raise ValueError(
                    "exactly one of next_attempt_at or retry_after is required for a retry"
                )
            if retry_after is not None:
                if retry_after < timedelta(0):
                    raise ValueError("retry_after must be >= 0")
                values["next_attempt_at"] = func.now() + retry_after
            else:
                values["next_attempt_at"] = next_attempt_at

        if last_error is not None:
            values["last_error"] = last_error

        if result.to_status is EventStatus.PROCESSED:
            if outcome is None:
                raise ValueError("outcome is required when the event is processed")
            values["outcome"] = outcome
            values["processed_at"] = func.now()
        elif outcome is not None:
            raise ValueError("outcome is only valid when the event is processed")

        conditions = [_EVENTS.c.id == event_id, _EVENTS.c.status == result.from_status]
        if expected_attempts is not None:
            conditions.append(_EVENTS.c.attempts == expected_attempts)
        statement = (
            update(_EVENTS).where(*conditions).values(**values).returning(_EVENTS.c.trace_id)
        )
        row = self._session.execute(statement).first()
        if row is None:
            return False
        self._record_transition(
            event_id, result.from_status, result.to_status, result.reason, row[0]
        )
        return True

    def start_attempt(self, event_id: uuid.UUID, attempt_no: int) -> int:
        """Open an ``event_attempts`` row; an unfinished row is evidence of a crash."""
        statement = (
            insert(_ATTEMPTS)
            .values(event_id=event_id, attempt_no=attempt_no, started_at=func.now())
            .returning(_ATTEMPTS.c.id)
        )
        attempt_id: int = self._session.execute(statement).scalar_one()
        return attempt_id

    def finish_attempt(self, attempt_id: int, *, result: str, error: str | None) -> None:
        self._session.execute(
            update(_ATTEMPTS)
            .where(_ATTEMPTS.c.id == attempt_id)
            .values(finished_at=func.now(), result=result, error=error)
        )

    def close_open_attempts(self, event_id: uuid.UUID, *, result: str, error: str) -> None:
        """Close attempt rows a dead worker left open (evidence stays, marked as such)."""
        self._session.execute(
            update(_ATTEMPTS)
            .where(_ATTEMPTS.c.event_id == event_id, _ATTEMPTS.c.finished_at.is_(None))
            .values(finished_at=func.now(), result=result, error=error)
        )

    # -- sweeper (FR-PRO-7, SDD 12) ----------------------------------------------

    def lock_expired_leases(self, *, limit: int) -> list[StoredEvent]:
        """``processing`` events whose lease ended, locked; rows other sweepers hold are skipped."""
        statement = (
            select(_EVENTS)
            .where(
                _EVENTS.c.status == EventStatus.PROCESSING,
                _EVENTS.c.locked_until < func.now(),
            )
            .order_by(_EVENTS.c.locked_until)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [_to_stored(row) for row in self._session.execute(statement)]

    def lock_due_for_publish(self, *, limit: int, received_grace_s: int) -> list[DueEvent]:
        """Events to enqueue again, locked; rows other sweepers hold are skipped.

        ``received`` older than the grace period (its first message was probably lost)
        and ``failed`` whose retry time has come.
        """
        statement = (
            select(_EVENTS.c.id, _EVENTS.c.trace_id)
            .where(
                or_(
                    and_(
                        _EVENTS.c.status == EventStatus.RECEIVED,
                        _EVENTS.c.received_at < func.now() - timedelta(seconds=received_grace_s),
                    ),
                    and_(
                        _EVENTS.c.status == EventStatus.FAILED,
                        _EVENTS.c.next_attempt_at <= func.now(),
                    ),
                )
            )
            .order_by(_EVENTS.c.received_at, _EVENTS.c.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [
            DueEvent(id=row.id, trace_id=row.trace_id) for row in self._session.execute(statement)
        ]

    # -- internals ---------------------------------------------------------------

    def _record_transition(
        self,
        event_id: uuid.UUID,
        from_status: EventStatus | None,
        to_status: EventStatus,
        reason: str,
        trace_id: str | None,
    ) -> None:
        self._session.execute(
            insert(_TRANSITIONS).values(
                event_id=event_id,
                from_status=from_status,
                to_status=to_status,
                reason=reason,
                trace_id=trace_id,
            )
        )
