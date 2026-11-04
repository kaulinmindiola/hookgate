"""Celery tasks. They only delegate to the use cases (SDD 6.3, plan F7).

``process_event``: run the use case; when it scheduled a retry, also send a delayed
message so the retry happens on time (the stored ``next_attempt_at`` stays the
authority: an early message is just a skipped claim, and the sweeper is the fallback).
An exception escaping the use case (for example the database is down) fails the task;
the event stays ``received``/``failed``/``processing`` and the sweeper recovers it.

``sweep``: run the sweeper; scheduled by Celery beat every ``SWEEP_INTERVAL_S``.
"""

from uuid import UUID

import structlog
from celery import Celery

from hookgate.application.process_event import ProcessEvent, ProcessOutcome
from hookgate.application.sweep import SweepEvents
from hookgate.infrastructure.celery_app import TASK_PROCESS_EVENT, TASK_SWEEP
from hookgate.infrastructure.queue import CeleryPublisher

# The delayed message arrives slightly after next_attempt_at, never before it.
RETRY_MARGIN_S = 0.5

_log = structlog.get_logger(__name__)


def register_tasks(
    app: Celery,
    *,
    process_event: ProcessEvent,
    sweep: SweepEvents,
    publisher: CeleryPublisher,
) -> None:
    # shared=False: bound to this app only (Celery shares tasks across apps by default).
    @app.task(name=TASK_PROCESS_EVENT, shared=False)  # type: ignore[untyped-decorator]
    def process_event_task(event_id: str) -> str:
        result = process_event(UUID(event_id))
        if result.outcome is ProcessOutcome.RETRY_SCHEDULED and result.retry_after_s is not None:
            try:
                publisher.publish_later(
                    UUID(event_id),
                    trace_id="",
                    countdown_s=result.retry_after_s + RETRY_MARGIN_S,
                )
            except Exception as exc:
                # The sweeper republishes it once next_attempt_at has passed.
                _log.warning("retry_publish_failed", event_uuid=event_id, error=type(exc).__name__)
        return result.outcome.value

    @app.task(name=TASK_SWEEP, shared=False)  # type: ignore[untyped-decorator]
    def sweep_task() -> dict[str, int]:
        report = sweep()
        return {
            "expired_to_failed": report.expired_to_failed,
            "expired_to_dead": report.expired_to_dead,
            "published": report.published,
            "publish_failures": report.publish_failures,
        }
