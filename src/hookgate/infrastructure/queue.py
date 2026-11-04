"""Event publishers for the ``EventPublisher`` port."""

from uuid import UUID

from celery import Celery

from hookgate.infrastructure.celery_app import TASK_PROCESS_EVENT

TRACE_HEADER = "trace_id"


class NullPublisher:
    """Publishes nothing. Useful where no broker exists (tools, some tests).

    Stored events stay in ``received`` and the sweeper picks them up, which is the
    same recovery path used when the broker is down (FR-ING-7).
    """

    def publish(self, event_id: UUID, *, trace_id: str) -> None:
        return None


class CeleryPublisher:
    """Sends a ``process_event`` task message. Raises if the broker is unreachable."""

    def __init__(self, app: Celery) -> None:
        self._app = app

    def publish(self, event_id: UUID, *, trace_id: str) -> None:
        self.publish_later(event_id, trace_id=trace_id, countdown_s=None)

    def publish_later(self, event_id: UUID, *, trace_id: str, countdown_s: float | None) -> None:
        self._app.send_task(
            TASK_PROCESS_EVENT,
            args=[str(event_id)],
            headers={TRACE_HEADER: trace_id},
            countdown=countdown_s,
        )
