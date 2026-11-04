"""Process one stored event (FR-PRO-1..6, FR-PRO-8, FR-ORD-1..4, SDD 6.3, 12 and 13).

Three short transactions, never one long one:

1. **Claim.** Atomic compare-and-set to ``processing`` with ``attempts + 1`` and a
   lease, plus an open ``event_attempts`` row. Committed before any work, so a crash
   leaves evidence and the lease lets the sweeper recover the event (F7).
2. **Effect.** Lock the resource row (same-resource events serialize, others run in
   parallel), decide ``applied`` or ``ignored_stale`` with the pure ordering rule, then
   the ``processing -> processed`` transition (guarded by status *and* attempt number)
   and, if applied, the snapshot and the ``applied_effects`` row. All commit together
   or not at all (FR-PRO-3, FR-ORD-2/3).
3. **Failure** (only if 2 raised). The effect was rolled back; a separate transaction
   records ``failed`` with a jittered retry time, or ``dead`` (FR-PRO-4/5).

Times are taken from the database clock (decision P3). Callable without a broker, so
tests drive it directly; the Celery task in F7 only delegates here.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from uuid import UUID

import structlog
from structlog.typing import FilteringBoundLogger

from hookgate.application.ports import ProcessingScope
from hookgate.application.records import StoredEvent
from hookgate.domain.errors import PermanentError
from hookgate.domain.events import Event, EventIdentity
from hookgate.domain.ordering import is_stale
from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.payments import Payment, project_event
from hookgate.domain.retry import RetryPolicy, SupportsUniform, delay
from hookgate.domain.states import ColumnAction, EventStatus, Trigger, transition

MAX_ERROR_LENGTH = 500

# Computes the effect of an event. Pure in production (``project_event``); tests inject
# a flaky one to simulate an unreliable dependency (VR-5).
EffectHandler = Callable[[Event], Payment]

_log = structlog.get_logger(__name__)


class ProcessOutcome(StrEnum):
    PROCESSED = "processed"
    RETRY_SCHEDULED = "retry_scheduled"
    DEAD = "dead"
    SKIPPED = "skipped"  # not claimable: done, owned by another worker or not due yet
    LOST = "lost"  # the claim was taken over (lease expired); nothing was written


@dataclass(frozen=True, slots=True)
class ProcessResult:
    outcome: ProcessOutcome
    attempt: int | None = None
    error: str | None = None
    event_outcome: EventOutcome | None = None  # applied or ignored_stale, when processed


class _ClaimLost(Exception):
    """Raised inside the effect transaction to roll it back."""


def to_domain_event(stored: StoredEvent) -> Event:
    """Rebuild the domain event from the stored raw body (ingestion validated the rest)."""
    try:
        document = json.loads(stored.raw_body)
    except ValueError as exc:
        raise PermanentError(f"stored body is not valid JSON: {exc}") from None
    data = document.get("data", {}) if isinstance(document, dict) else None
    if not isinstance(data, dict):
        raise PermanentError("data must be a JSON object")
    return Event(
        identity=EventIdentity(stored.provider, stored.event_id),
        event_type=stored.event_type,
        resource_type=stored.resource_type,
        resource_id=stored.resource_id,
        occurred_at=stored.occurred_at,
        data=data,
    )


def describe_error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH]


class ProcessEvent:
    def __init__(
        self,
        *,
        units: ProcessingScope,
        lease_s: int,
        policy: RetryPolicy,
        rng: SupportsUniform,
        handler: EffectHandler = project_event,
    ) -> None:
        if lease_s < 1:
            raise ValueError("lease_s must be >= 1")
        self._units = units
        self._lease_s = lease_s
        self._policy = policy
        self._rng = rng
        self._handler = handler

    def __call__(self, event_id: UUID) -> ProcessResult:
        with self._units() as unit:
            claimed = unit.events.claim(event_id, lease_s=self._lease_s)
            attempt_id = (
                unit.events.start_attempt(claimed.id, claimed.attempts) if claimed else None
            )
        if claimed is None or attempt_id is None:
            _log.info("event_skipped", event_uuid=str(event_id))
            return ProcessResult(ProcessOutcome.SKIPPED)

        log = _log.bind(
            trace_id=claimed.trace_id,
            event_id=claimed.event_id,
            resource_id=claimed.resource_id,
            attempt=claimed.attempts,
        )
        try:
            payment = self._handler(to_domain_event(claimed))
            success = transition(
                EventStatus.PROCESSING,
                Trigger.HANDLER_SUCCESS,
                attempts=claimed.attempts,
                max_attempts=self._policy.max_attempts,
            )
            with self._units() as unit:
                # Resource lock first: always resource row, then event row, in every
                # transaction, so two workers can never wait on each other in a cycle.
                last_applied = unit.payments.lock_resource(payment.resource_id)
                outcome = (
                    EventOutcome.IGNORED_STALE
                    if is_stale(payment.last_key, last_applied)
                    else EventOutcome.APPLIED
                )
                # The compare-and-set proves we still own the claim before any write.
                if not unit.events.apply_transition(
                    claimed.id, success, outcome=outcome, expected_attempts=claimed.attempts
                ):
                    raise _ClaimLost
                if outcome is EventOutcome.APPLIED:
                    unit.payments.apply(payment, claimed.id)
                unit.events.finish_attempt(attempt_id, result=success.to_status.value, error=None)
        except _ClaimLost:
            log.warning("event_claim_lost")
            return ProcessResult(ProcessOutcome.LOST, claimed.attempts)
        except Exception as exc:
            return self._record_failure(claimed, attempt_id, exc, log)

        log.info("event_processed", status=EventStatus.PROCESSED.value, outcome=outcome.value)
        return ProcessResult(ProcessOutcome.PROCESSED, claimed.attempts, event_outcome=outcome)

    def _record_failure(
        self, claimed: StoredEvent, attempt_id: int, exc: Exception, log: FilteringBoundLogger
    ) -> ProcessResult:
        # Anything not known to be permanent is treated as transient (SDD 12).
        trigger = (
            Trigger.PERMANENT_ERROR if isinstance(exc, PermanentError) else Trigger.TRANSIENT_ERROR
        )
        decision = transition(
            EventStatus.PROCESSING,
            trigger,
            attempts=claimed.attempts,
            max_attempts=self._policy.max_attempts,
        )
        retry_after = None
        if decision.next_attempt_at is ColumnAction.SET:
            retry_after = timedelta(seconds=delay(claimed.attempts, self._rng, self._policy))
        error = describe_error(exc)

        with self._units() as unit:
            recorded = unit.events.apply_transition(
                claimed.id,
                decision,
                retry_after=retry_after,
                last_error=error,
                expected_attempts=claimed.attempts,
            )
            if recorded:
                unit.events.finish_attempt(attempt_id, result=decision.to_status.value, error=error)
        if not recorded:
            log.warning("event_claim_lost", error=type(exc).__name__)
            return ProcessResult(ProcessOutcome.LOST, claimed.attempts, error)

        if decision.to_status is EventStatus.DEAD:
            log.warning("event_dead", status="dead", reason=trigger.value, error=type(exc).__name__)
            return ProcessResult(ProcessOutcome.DEAD, claimed.attempts, error)
        log.warning(
            "event_retry_scheduled",
            status="failed",
            error=type(exc).__name__,
            retry_after_s=retry_after.total_seconds() if retry_after else None,
        )
        return ProcessResult(ProcessOutcome.RETRY_SCHEDULED, claimed.attempts, error)
