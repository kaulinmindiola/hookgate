"""Ingestion counters (SDD 15.2): every delivery is counted exactly once by result."""

import json
from datetime import UTC, datetime

import pytest
from tests.fakes import (
    FaultyCache,
    FixedClock,
    InMemoryCache,
    InMemoryEvents,
    RecordingMetrics,
    RecordingPublisher,
)

from hookgate.application.ingest_event import IngestCommand, IngestEvent, InvalidPayload
from hookgate.application.ports import DedupeCache
from hookgate.domain.errors import SignatureError
from hookgate.domain.signature import build_signature_header

SECRET = "metrics-secret"
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)


def command(
    event_id: str = "evt_1", *, raw: bytes | None = None, signed: bool = True
) -> IngestCommand:
    body = (
        raw
        if raw is not None
        else json.dumps(
            {
                "id": event_id,
                "type": "payment.succeeded",
                "occurred_at": "2026-11-03T11:59:00Z",
                "resource": {"type": "payment", "id": "pay_1"},
                "data": {"amount": 1, "currency": "USD"},
            }
        ).encode()
    )
    header = build_signature_header(SECRET, int(NOW.timestamp()), body) if signed else "garbage"
    return IngestCommand(raw_body=body, signature_header=header, headers={}, trace_id="t")


def ingest(metrics: RecordingMetrics, cache: DedupeCache | None) -> IngestEvent:
    return IngestEvent(
        secret=SECRET,
        tolerance_s=300,
        clock=FixedClock(NOW),
        events=InMemoryEvents().scope,
        publisher=RecordingPublisher(),
        cache=cache,
        metrics=metrics,
    )


def test_accepted_then_duplicates_by_layer() -> None:
    metrics = RecordingMetrics()
    with_cache = ingest(metrics, InMemoryCache())

    with_cache(command())
    with_cache(command())

    assert metrics.received_outcomes == {"accepted": 1, "duplicate": 1}
    assert metrics.duplicates == {"redis": 1}


def test_duplicate_detected_by_postgres_when_there_is_no_marker() -> None:
    metrics = RecordingMetrics()
    without_cache = ingest(metrics, None)

    without_cache(command())
    without_cache(command())

    assert metrics.received_outcomes == {"accepted": 1, "duplicate": 1}
    assert metrics.duplicates == {"postgres": 1}


def test_rejections_are_counted() -> None:
    metrics = RecordingMetrics()
    use_case = ingest(metrics, InMemoryCache())

    with pytest.raises(SignatureError):
        use_case(command(signed=False))
    with pytest.raises(InvalidPayload):
        use_case(command(raw=b"not json"))

    assert metrics.received_outcomes == {"rejected": 2}


def test_cache_errors_are_counted_and_the_delivery_still_accepted() -> None:
    metrics = RecordingMetrics()

    ingest(metrics, FaultyCache())(command())

    assert metrics.cache_errors == {"get": 1, "set": 1}
    assert metrics.received_outcomes == {"accepted": 1}
