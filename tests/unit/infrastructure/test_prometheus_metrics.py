"""Prometheus adapter and the scrape-time status gauge (SDD 15.2, FR-OBS-3)."""

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.metrics import EventsByStatusCollector, PrometheusMetrics


def value(registry: CollectorRegistry, name: str, **labels: str) -> float | None:
    return registry.get_sample_value(name, labels)


def test_every_labelled_series_starts_at_zero() -> None:
    registry = CollectorRegistry()
    PrometheusMetrics(registry)

    for outcome in ("accepted", "duplicate", "rejected"):
        assert value(registry, "hookgate_events_received_total", outcome=outcome) == 0
    for layer in ("redis", "postgres"):
        assert value(registry, "hookgate_duplicates_total", layer=layer) == 0
    for processed in ("applied", "ignored_stale"):
        assert value(registry, "hookgate_events_processed_total", outcome=processed) == 0
    for reason in ("permanent_error", "transient_error", "lease_expired"):
        assert value(registry, "hookgate_events_dead_total", reason=reason) == 0
    for op in ("get", "set"):
        assert value(registry, "hookgate_cache_errors_total", op=op) == 0
    assert value(registry, "hookgate_events_retried_total") == 0
    assert value(registry, "hookgate_processing_seconds_count") == 0


def test_calls_increment_the_matching_series() -> None:
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry)

    metrics.received("accepted")
    metrics.received("duplicate")
    metrics.duplicate("redis")
    metrics.cache_error("set")
    metrics.processed(EventOutcome.IGNORED_STALE)
    metrics.retried()
    metrics.retried()
    metrics.dead("permanent_error")
    metrics.processing_seconds(0.02)
    metrics.processing_seconds(3.0)

    assert value(registry, "hookgate_events_received_total", outcome="accepted") == 1
    assert value(registry, "hookgate_events_received_total", outcome="duplicate") == 1
    assert value(registry, "hookgate_duplicates_total", layer="redis") == 1
    assert value(registry, "hookgate_cache_errors_total", op="set") == 1
    assert value(registry, "hookgate_events_processed_total", outcome="ignored_stale") == 1
    assert value(registry, "hookgate_events_retried_total") == 2
    assert value(registry, "hookgate_events_dead_total", reason="permanent_error") == 1
    assert value(registry, "hookgate_processing_seconds_count") == 2
    assert value(registry, "hookgate_processing_seconds_bucket", le="0.025") == 1
    assert value(registry, "hookgate_processing_seconds_sum") == pytest.approx(3.02)


def test_two_registries_are_independent() -> None:
    first, second = CollectorRegistry(), CollectorRegistry()
    PrometheusMetrics(first).received("accepted")
    PrometheusMetrics(second)

    assert value(first, "hookgate_events_received_total", outcome="accepted") == 1
    assert value(second, "hookgate_events_received_total", outcome="accepted") == 0


def test_status_gauge_reports_every_status_including_zero() -> None:
    registry = CollectorRegistry()
    calls: list[int] = []

    def counts() -> dict[EventStatus, int]:
        calls.append(1)
        return {EventStatus.PROCESSED: 7, EventStatus.DEAD: 2}

    registry.register(EventsByStatusCollector(counts))
    assert calls == []  # registering never queries the database

    assert value(registry, "hookgate_events_by_status", status="processed") == 7
    assert value(registry, "hookgate_events_by_status", status="dead") == 2
    assert value(registry, "hookgate_events_by_status", status="received") == 0
    assert value(registry, "hookgate_events_by_status", status="failed") == 0
    assert value(registry, "hookgate_events_by_status", status="processing") == 0


def test_a_failing_status_query_omits_the_gauge_but_not_the_scrape() -> None:
    registry = CollectorRegistry()
    PrometheusMetrics(registry)

    def counts() -> dict[EventStatus, int]:
        raise ConnectionError("database down")

    registry.register(EventsByStatusCollector(counts))
    text = generate_latest(registry).decode()

    assert "hookgate_events_by_status" not in text
    assert "hookgate_events_received_total" in text
