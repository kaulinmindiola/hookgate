"""Event state machine (FR-PRO-1, SDD section 9, ADR-011).

``transition()`` is pure. It decides the target status and which columns change; the
repository applies that decision in SQL with a compare-and-set on the current status.
The ``next_attempt_at <= now`` guard of the claim is NOT here: it needs the database
clock and lives only in the claim SQL (ADR-011).
"""

from dataclasses import dataclass
from enum import StrEnum

from hookgate.domain.errors import InvalidTransition

DEFAULT_MAX_ATTEMPTS = 5


class EventStatus(StrEnum):
    RECEIVED = "received"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"
    DEAD = "dead"


class Trigger(StrEnum):
    CLAIM = "claim"
    HANDLER_SUCCESS = "handler_success"
    TRANSIENT_ERROR = "transient_error"
    PERMANENT_ERROR = "permanent_error"
    LEASE_EXPIRED = "lease_expired"
    REPLAY = "replay"


class AttemptsAction(StrEnum):
    KEEP = "keep"
    INCREMENT = "increment"
    RESET = "reset"


class ColumnAction(StrEnum):
    """What to do with a nullable column (``locked_until``, ``next_attempt_at``)."""

    KEEP = "keep"
    SET = "set"
    CLEAR = "clear"


@dataclass(frozen=True, slots=True)
class TransitionResult:
    from_status: EventStatus
    to_status: EventStatus
    trigger: Trigger
    attempts: AttemptsAction
    locked_until: ColumnAction
    next_attempt_at: ColumnAction

    @property
    def reason(self) -> str:
        """Value for the audit row (``event_transitions.reason``)."""
        return self.trigger.value


def transition(
    status: EventStatus,
    trigger: Trigger,
    *,
    attempts: int = 0,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> TransitionResult:
    """Return the decision for ``trigger`` applied to ``status``.

    ``attempts`` is the value already stored in the row (it is incremented at claim
    time, ADR-008). Raises ``InvalidTransition`` for any pair not in the SDD table.
    """
    if attempts < 0:
        raise ValueError("attempts must be >= 0")
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    keep = AttemptsAction.KEEP
    clear = ColumnAction.CLEAR

    if trigger is Trigger.CLAIM and status in (EventStatus.RECEIVED, EventStatus.FAILED):
        return TransitionResult(
            status,
            EventStatus.PROCESSING,
            trigger,
            AttemptsAction.INCREMENT,
            ColumnAction.SET,
            ColumnAction.KEEP,
        )

    if status is EventStatus.PROCESSING:
        if trigger is Trigger.HANDLER_SUCCESS:
            return TransitionResult(status, EventStatus.PROCESSED, trigger, keep, clear, clear)
        if trigger is Trigger.PERMANENT_ERROR:
            return TransitionResult(status, EventStatus.DEAD, trigger, keep, clear, clear)
        if trigger in (Trigger.TRANSIENT_ERROR, Trigger.LEASE_EXPIRED):
            if attempts >= max_attempts:
                return TransitionResult(status, EventStatus.DEAD, trigger, keep, clear, clear)
            return TransitionResult(
                status, EventStatus.FAILED, trigger, keep, clear, ColumnAction.SET
            )

    if status is EventStatus.DEAD and trigger is Trigger.REPLAY:
        return TransitionResult(
            status, EventStatus.RECEIVED, trigger, AttemptsAction.RESET, clear, clear
        )

    raise InvalidTransition(status.value, trigger.value)
