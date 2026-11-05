"""Prometheus adapter for the metrics ports (SDD 15.2, FR-OBS-3).

Each process owns one ``CollectorRegistry``: the API serves it on ``/metrics`` and the
worker on its own port (ADR-012: one worker process, so one registry). Counters are
incremented in the process that changes the state. Every labelled series is created
at start-up with value 0, so dashboards and ``rate()`` see it before the first event.

``hookgate_events_by_status`` is computed when Prometheus scrapes the API: one
``GROUP BY status`` query per scrape. If the query fails the gauge is omitted from
that scrape (and a warning is logged); the scrape itself never fails.
"""

from collections.abc import Callable, Iterator, Mapping

import structlog
from prometheus_client import CollectorRegistry, Counter, Histogram
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from hookgate.application.ports import CacheOp, DuplicateLayer, ReceivedOutcome
from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.states import EventStatus, Trigger

DEAD_REASONS = (
    Trigger.PERMANENT_ERROR.value,
    Trigger.TRANSIENT_ERROR.value,
    Trigger.LEASE_EXPIRED.value,
)
# Processing is one handler call plus two or three short transactions.
PROCESSING_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

StatusCounts = Callable[[], Mapping[EventStatus, int]]

_log = structlog.get_logger(__name__)


class PrometheusMetrics:
    """Satisfies ``IngestionMetrics`` and ``ProcessingMetrics``."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self.registry = registry
        self._received = Counter(
            "hookgate_events_received",
            "Webhook deliveries by result.",
            ["outcome"],
            registry=registry,
        )
        self._duplicates = Counter(
            "hookgate_duplicates",
            "Duplicate deliveries by the layer that detected them.",
            ["layer"],
            registry=registry,
        )
        self._processed = Counter(
            "hookgate_events_processed",
            "Events that reached processed, by outcome.",
            ["outcome"],
            registry=registry,
        )
        self._retried = Counter(
            "hookgate_events_retried",
            "Events scheduled for another attempt (transient error or expired lease).",
            registry=registry,
        )
        self._dead = Counter(
            "hookgate_events_dead",
            "Events moved to dead, by the trigger that killed them.",
            ["reason"],
            registry=registry,
        )
        self._cache_errors = Counter(
            "hookgate_cache_errors",
            "Redis dedupe cache errors (the request continues on PostgreSQL).",
            ["op"],
            registry=registry,
        )
        self._processing = Histogram(
            "hookgate_processing_seconds",
            "Time spent on a claimed event: handler plus effect or failure transaction.",
            buckets=PROCESSING_BUCKETS,
            registry=registry,
        )
        for outcome in ("accepted", "duplicate", "rejected"):
            self._received.labels(outcome=outcome)
        for layer in ("redis", "postgres"):
            self._duplicates.labels(layer=layer)
        for event_outcome in EventOutcome:
            self._processed.labels(outcome=event_outcome.value)
        for reason in DEAD_REASONS:
            self._dead.labels(reason=reason)
        for op in ("get", "set"):
            self._cache_errors.labels(op=op)

    def received(self, outcome: ReceivedOutcome) -> None:
        self._received.labels(outcome=outcome).inc()

    def duplicate(self, layer: DuplicateLayer) -> None:
        self._duplicates.labels(layer=layer).inc()

    def cache_error(self, op: CacheOp) -> None:
        self._cache_errors.labels(op=op).inc()

    def processed(self, outcome: EventOutcome) -> None:
        self._processed.labels(outcome=outcome.value).inc()

    def retried(self) -> None:
        self._retried.inc()

    def dead(self, reason: str) -> None:
        self._dead.labels(reason=reason).inc()

    def processing_seconds(self, seconds: float) -> None:
        self._processing.observe(seconds)


class EventsByStatusCollector(Collector):
    """``hookgate_events_by_status``: computed from the database at scrape time."""

    def __init__(self, counts: StatusCounts) -> None:
        self._counts = counts

    def collect(self) -> Iterator[Metric]:
        try:
            counts = self._counts()
        except Exception as exc:
            _log.warning("events_by_status_failed", error=type(exc).__name__)
            return
        gauge = GaugeMetricFamily(
            "hookgate_events_by_status",
            "Stored events by current status (one database query per scrape).",
            labels=["status"],
        )
        for status in EventStatus:
            gauge.add_metric([status.value], counts.get(status, 0))
        yield gauge

    def describe(self) -> Iterator[Metric]:
        # Registered without describing, so registration never queries the database.
        return iter(())
