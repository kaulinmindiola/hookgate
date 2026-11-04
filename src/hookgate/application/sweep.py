"""Sweeper: the recovery path that makes "no acknowledged event is lost" true (FR-PRO-7).

One short transaction, then publishing:

1. ``processing`` events whose lease expired (the worker died or hung) go through the
   state machine as ``lease_expired``: ``failed`` with backoff, or ``dead`` once
   ``attempts`` reached the maximum. The attempt row the worker left open is closed.
2. Events to enqueue again are selected: ``received`` older than the grace period
   (the first message was lost, e.g. broker down at ingestion, FR-ING-7) and ``failed``
   whose retry time has come.

Rows are selected ``FOR UPDATE SKIP LOCKED``, so concurrent sweepers split the work
instead of waiting. Publishing happens only after the commit; a failed publish is
simply retried by the next sweep, and a duplicate message is harmless because the
claim is a compare-and-set (SDD 12).
"""

from dataclasses import dataclass
from datetime import timedelta

import structlog

from hookgate.application.ports import EventPublisher, SweepScope
from hookgate.application.records import DueEvent
from hookgate.domain.retry import RetryPolicy, SupportsUniform, delay
from hookgate.domain.states import ColumnAction, EventStatus, Trigger, transition

DEFAULT_BATCH_SIZE = 500
DEFAULT_RECEIVED_GRACE_S = 30
LEASE_EXPIRED_ERROR = "lease expired: the worker died or exceeded LEASE_S"

_log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class SweepReport:
    expired_to_failed: int = 0
    expired_to_dead: int = 0
    published: int = 0
    publish_failures: int = 0


class SweepEvents:
    def __init__(
        self,
        *,
        units: SweepScope,
        publisher: EventPublisher,
        policy: RetryPolicy,
        rng: SupportsUniform,
        batch_size: int = DEFAULT_BATCH_SIZE,
        received_grace_s: int = DEFAULT_RECEIVED_GRACE_S,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if received_grace_s < 0:
            raise ValueError("received_grace_s must be >= 0")
        self._units = units
        self._publisher = publisher
        self._policy = policy
        self._rng = rng
        self._batch_size = batch_size
        self._received_grace_s = received_grace_s

    def __call__(self) -> SweepReport:
        to_failed = to_dead = 0
        with self._units() as events:
            for stored in events.lock_expired_leases(limit=self._batch_size):
                decision = transition(
                    EventStatus.PROCESSING,
                    Trigger.LEASE_EXPIRED,
                    attempts=stored.attempts,
                    max_attempts=self._policy.max_attempts,
                )
                retry_after = None
                if decision.next_attempt_at is ColumnAction.SET:
                    seconds = delay(stored.attempts, self._rng, self._policy)
                    retry_after = timedelta(seconds=seconds)
                if not events.apply_transition(
                    stored.id,
                    decision,
                    retry_after=retry_after,
                    last_error=LEASE_EXPIRED_ERROR,
                    expected_attempts=stored.attempts,
                ):
                    continue  # changed meanwhile: nothing to recover
                events.close_open_attempts(
                    stored.id, result="lease_expired", error=LEASE_EXPIRED_ERROR
                )
                if decision.to_status is EventStatus.DEAD:
                    to_dead += 1
                else:
                    to_failed += 1
                _log.warning(
                    "lease_expired",
                    trace_id=stored.trace_id,
                    event_id=stored.event_id,
                    attempt=stored.attempts,
                    status=decision.to_status.value,
                )
            due = events.lock_due_for_publish(
                limit=self._batch_size, received_grace_s=self._received_grace_s
            )
        # Committed. Publishing outside the transaction never holds row locks.
        published, failures = self._publish(due)
        report = SweepReport(to_failed, to_dead, published, failures)
        if report != SweepReport():
            _log.info(
                "sweep_finished",
                expired_to_failed=report.expired_to_failed,
                expired_to_dead=report.expired_to_dead,
                published=report.published,
                publish_failures=report.publish_failures,
            )
        return report

    def _publish(self, due: list[DueEvent]) -> tuple[int, int]:
        published = failures = 0
        for event in due:
            try:
                self._publisher.publish(event.id, trace_id=event.trace_id)
            except Exception as exc:
                failures += 1
                _log.warning("publish_failed", trace_id=event.trace_id, error=type(exc).__name__)
            else:
                published += 1
        return published, failures
