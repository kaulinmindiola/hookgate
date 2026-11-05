"""A metrics implementation that records nothing, used when no adapter is wired."""

from hookgate.application.ports import CacheOp, DuplicateLayer, ReceivedOutcome
from hookgate.domain.outcomes import EventOutcome


class NullMetrics:
    """Satisfies ``IngestionMetrics`` and ``ProcessingMetrics``; every call is a no-op."""

    def received(self, outcome: ReceivedOutcome) -> None:
        return None

    def duplicate(self, layer: DuplicateLayer) -> None:
        return None

    def cache_error(self, op: CacheOp) -> None:
        return None

    def processed(self, outcome: EventOutcome) -> None:
        return None

    def retried(self) -> None:
        return None

    def dead(self, reason: str) -> None:
        return None

    def processing_seconds(self, seconds: float) -> None:
        return None


NULL_METRICS = NullMetrics()
