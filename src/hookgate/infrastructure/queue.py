"""Event publishers for the ``EventPublisher`` port."""

from uuid import UUID


class NullPublisher:
    """Placeholder until the Celery publisher arrives in phase F7.

    Stored events stay in ``received``; the sweeper (F7) picks them up, which is the
    same recovery path used when the broker is down (FR-ING-7).
    """

    def publish(self, event_id: UUID, *, trace_id: str) -> None:
        return None
