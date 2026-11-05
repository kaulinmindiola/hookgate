"""Celery tasks. They only delegate to the use cases (SDD 6.3, plan F7).

``process_event``: run the use case; when it scheduled a retry, also send a delayed
message so the retry happens on time (the stored ``next_attempt_at`` stays the
authority: an early message is just a skipped claim, and the sweeper is the fallback).
An exception escaping the use case (for example the database is down) fails the task;
the event stays ``received``/``failed``/``processing`` and the sweeper recovers it.

``sweep``: run the sweeper; scheduled by Celery beat every ``SWEEP_INTERVAL_S``.

Trace propagation (FR-OBS-1, SDD 15.3): publishers send the event's ``trace_id`` as a
message header. The task binds it to the structlog context for its whole run, so every
line the worker writes for that message (including skips and publish failures, which
happen before or without a claim) carries the same ``trace_id`` as the API lines.
"""

from typing import Any
from uuid import UUID

import structlog
from celery import Celery

from hookgate.application.process_event import ProcessEvent, ProcessOutcome
from hookgate.application.sweep import SweepEvents
from hookgate.infrastructure.celery_app import TASK_PROCESS_EVENT, TASK_SWEEP
from hookgate.infrastructure.queue import TRACE_HEADER, CeleryPublisher

# The delayed message arrives slightly after next_attempt_at, never before it.
RETRY_MARGIN_S = 0.5

_log = structlog.get_logger(__name__)


def message_trace_id(request: Any) -> str | None:
    """The ``trace_id`` header of the message being executed, if any.

    Celery exposes custom headers as request attributes for messages that went through
    a broker, and only in ``request.headers`` for eagerly applied tasks.
    """
    value = request.get(TRACE_HEADER) or (request.headers or {}).get(TRACE_HEADER)
    return str(value) if value else None


def register_tasks(
    app: Celery,
    *,
    process_event: ProcessEvent,
    sweep: SweepEvents,
    publisher: CeleryPublisher,
) -> None:
    # shared=False: bound to this app only (Celery shares tasks across apps by default).
    @app.task(name=TASK_PROCESS_EVENT, shared=False, bind=True)  # type: ignore[untyped-decorator]
    def process_event_task(task: Any, event_id: str) -> str:
        trace_id = message_trace_id(task.request)
        context = {"trace_id": trace_id} if trace_id else {}
        with structlog.contextvars.bound_contextvars(**context):
            result = process_event(UUID(event_id))
            if (
                result.outcome is ProcessOutcome.RETRY_SCHEDULED
                and result.retry_after_s is not None
            ):
                try:
                    publisher.publish_later(
                        UUID(event_id),
                        trace_id=trace_id or "",
                        countdown_s=result.retry_after_s + RETRY_MARGIN_S,
                    )
                except Exception as exc:
                    # The sweeper republishes it once next_attempt_at has passed.
                    _log.warning(
                        "retry_publish_failed", event_uuid=event_id, error=type(exc).__name__
                    )
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
