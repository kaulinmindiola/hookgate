"""Redis fast path of ingestion with a healthy cache (FR-IDE-3/5, ADR-003)."""

import json
from datetime import UTC, datetime

import pytest
from structlog.testing import capture_logs
from tests.fakes import FixedClock, InMemoryCache, InMemoryEvents, RecordingPublisher

from hookgate.application.ingest_event import IngestCommand, IngestEvent, IngestStatus
from hookgate.domain.errors import SignatureError
from hookgate.domain.events import EventIdentity
from hookgate.domain.signature import build_signature_header

SECRET = "cache-test-secret"
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)
IDENTITY = EventIdentity("sim", "evt_1")


def body(amount: int = 1500) -> bytes:
    return json.dumps(
        {
            "id": "evt_1",
            "type": "payment.created",
            "occurred_at": "2026-11-03T11:59:00Z",
            "resource": {"type": "payment", "id": "pay_1"},
            "data": {"amount": amount, "currency": "USD"},
        }
    ).encode()


def command(raw: bytes, *, signed: bool = True) -> IngestCommand:
    header = build_signature_header(SECRET, int(NOW.timestamp()), raw) if signed else None
    return IngestCommand(raw_body=raw, signature_header=header, headers={}, trace_id="t-1")


def make_use_case(
    events: InMemoryEvents, cache: InMemoryCache, publisher: RecordingPublisher | None = None
) -> IngestEvent:
    return IngestEvent(
        secret=SECRET,
        tolerance_s=300,
        clock=FixedClock(NOW),
        events=events.scope,
        publisher=publisher or RecordingPublisher(),
        cache=cache,
    )


def test_marker_is_written_after_the_commit_and_before_publishing() -> None:
    journal: list[str] = []
    events = InMemoryEvents(journal)
    cache = InMemoryCache(journal)

    make_use_case(events, cache, RecordingPublisher(journal))(command(body()))

    assert journal == ["commit", "remember", "publish"]
    assert IDENTITY in cache.markers


def test_marker_hit_returns_duplicate_without_touching_the_database() -> None:
    events = InMemoryEvents()
    cache = InMemoryCache()
    publisher = RecordingPublisher()
    ingest = make_use_case(events, cache, publisher)
    ingest(command(body()))

    with capture_logs() as logs:
        result = ingest(command(body()))

    assert result.status is IngestStatus.DUPLICATE
    assert events.inserts == 1
    assert len(publisher.published) == 1
    assert [(entry["event"], entry.get("layer")) for entry in logs] == [
        ("webhook_duplicate", "redis")
    ]


def test_missing_marker_falls_back_to_postgres_and_restores_the_marker() -> None:
    events = InMemoryEvents()
    cache = InMemoryCache()
    ingest = make_use_case(events, cache)
    ingest(command(body()))
    cache.markers.clear()  # evicted or expired (SDD 11)

    with capture_logs() as logs:
        result = ingest(command(body()))

    assert result.status is IngestStatus.DUPLICATE
    assert events.inserts == 2
    assert IDENTITY in cache.markers
    assert [(entry["event"], entry.get("layer")) for entry in logs] == [
        ("webhook_duplicate", "postgres")
    ]


def test_changed_body_is_detected_only_on_the_postgres_path() -> None:
    events = InMemoryEvents()
    cache = InMemoryCache()
    ingest = make_use_case(events, cache)
    ingest(command(body(amount=1500)))

    with capture_logs() as redis_path:
        ingest(command(body(amount=9999)))
    cache.markers.clear()
    with capture_logs() as postgres_path:
        ingest(command(body(amount=9999)))

    assert "payload_mismatch" not in [entry["event"] for entry in redis_path]
    assert "payload_mismatch" in [entry["event"] for entry in postgres_path]
    assert events.stored("evt_1").raw_body == body(amount=1500)


def test_rejected_delivery_never_reads_or_writes_the_cache() -> None:
    events = InMemoryEvents()
    cache = InMemoryCache()
    cache.markers.add(IDENTITY)

    with pytest.raises(SignatureError):
        make_use_case(events, cache)(command(body(), signed=False))

    assert cache.journal == []
    assert events.inserts == 0
