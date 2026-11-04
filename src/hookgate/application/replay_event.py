"""Operator replay of a dead event (FR-DLQ-3, FR-DLQ-4, SDD 14).

One transaction: compare-and-set ``dead -> received`` through the state machine
(``attempts`` reset, ``next_attempt_at`` and ``locked_until`` cleared, audit row with
reason ``operator_replay``). History (transitions, attempts, ``last_error``) is kept.
The event is published only after the commit.

Concurrent calls are safe: the UPDATE matches ``status = 'dead'``, so PostgreSQL lets
exactly one of them win; the others see the new status and get ``NOT_DEAD``. Only the
winner publishes, so the event is enqueued once. If publishing fails, the event stays
``received`` and the sweeper enqueues it after its grace period (FR-ING-7, ADR-007).
"""

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

import structlog

from hookgate.application.ports import AdminScope, EventPublisher
from hookgate.domain.states import EventStatus, Trigger, transition

_log = structlog.get_logger(__name__)


class ReplayOutcome(StrEnum):
    REPLAYED = "replayed"
    NOT_FOUND = "not_found"
    NOT_DEAD = "not_dead"


@dataclass(frozen=True, slots=True)
class ReplayResult:
    outcome: ReplayOutcome
    status: EventStatus | None = None  # the event's status after the call
    enqueued: bool = False


class ReplayEvent:
    def __init__(self, *, units: AdminScope, publisher: EventPublisher) -> None:
        self._units = units
        self._publisher = publisher
        self._decision = transition(EventStatus.DEAD, Trigger.REPLAY)

    def __call__(self, event_id: UUID, *, request_trace_id: str) -> ReplayResult:
        with self._units() as events:
            if events.apply_transition(event_id, self._decision):
                stored = events.get(event_id)
            else:
                current = events.get(event_id)
                if current is None:
                    return ReplayResult(ReplayOutcome.NOT_FOUND)
                _log.info(
                    "replay_rejected",
                    trace_id=current.trace_id,
                    request_trace_id=request_trace_id,
                    event_id=current.event_id,
                    status=current.status.value,
                )
                return ReplayResult(ReplayOutcome.NOT_DEAD, status=current.status)
        assert stored is not None  # the CAS just matched this row in the same transaction

        # Committed. Publish outside the transaction (SDD 12).
        enqueued = True
        try:
            self._publisher.publish(stored.id, trace_id=stored.trace_id)
        except Exception as exc:
            enqueued = False
            _log.warning(
                "publish_failed",
                trace_id=stored.trace_id,
                event_id=stored.event_id,
                error=type(exc).__name__,
            )
        _log.info(
            "event_replayed",
            trace_id=stored.trace_id,
            request_trace_id=request_trace_id,
            event_id=stored.event_id,
            resource_id=stored.resource_id,
            enqueued=enqueued,
        )
        return ReplayResult(ReplayOutcome.REPLAYED, status=stored.status, enqueued=enqueued)
