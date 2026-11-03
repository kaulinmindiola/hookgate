"""VR-3 (unit): Redis failing in the middle of a delivery sequence (FR-IDE-4, NFR-1).

The provider never sees an error, every event is stored exactly once, duplicates are
still detected (by PostgreSQL) and every failed cache call is counted and logged.
"""

import json
from datetime import UTC, datetime

import pytest
from structlog.testing import capture_logs
from tests.fakes import (
    FaultyCache,
    FixedClock,
    InMemoryEvents,
    RecordingMetrics,
    RecordingPublisher,
)

from hookgate.application.ingest_event import IngestCommand, IngestEvent, IngestStatus
from hookgate.application.ports import CacheOp
from hookgate.domain.signature import build_signature_header

SECRET = "degradation-test-secret"
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)

# evt_1 twice, evt_2 three times, evt_3 once, interleaved.
SEQUENCE = ["evt_1", "evt_2", "evt_1", "evt_3", "evt_2", "evt_2"]
EXPECTED = [
    IngestStatus.ACCEPTED,
    IngestStatus.ACCEPTED,
    IngestStatus.DUPLICATE,
    IngestStatus.ACCEPTED,
    IngestStatus.DUPLICATE,
    IngestStatus.DUPLICATE,
]


def command(event_id: str) -> IngestCommand:
    raw = json.dumps(
        {
            "id": event_id,
            "type": "payment.created",
            "occurred_at": "2026-11-03T11:59:00Z",
            "resource": {"type": "payment", "id": f"pay_{event_id}"},
            "data": {},
        }
    ).encode()
    return IngestCommand(
        raw_body=raw,
        signature_header=build_signature_header(SECRET, int(NOW.timestamp()), raw),
        headers={},
        trace_id=f"trace-{event_id}",
    )


def deliver_all(
    cache: FaultyCache,
) -> tuple[list[IngestStatus], InMemoryEvents, RecordingMetrics, RecordingPublisher]:
    events = InMemoryEvents()
    metrics = RecordingMetrics()
    publisher = RecordingPublisher()
    ingest = IngestEvent(
        secret=SECRET,
        tolerance_s=300,
        clock=FixedClock(NOW),
        events=events.scope,
        publisher=publisher,
        cache=cache,
        metrics=metrics,
    )
    statuses = [ingest(command(event_id)).status for event_id in SEQUENCE]
    return statuses, events, metrics, publisher


@pytest.mark.parametrize("healthy_calls", [0, 1, 3, 5, 8, 100])
def test_redis_dying_mid_sequence_never_fails_a_delivery(healthy_calls: int) -> None:
    cache = FaultyCache(healthy_calls)

    statuses, events, metrics, publisher = deliver_all(cache)

    assert statuses == EXPECTED
    assert sorted(identity.event_id for identity in events.rows) == ["evt_1", "evt_2", "evt_3"]
    assert len(publisher.published) == 3
    assert sum(metrics.cache_errors.values()) == max(0, cache.calls - healthy_calls)


@pytest.mark.parametrize("op", ["get", "set"])
def test_each_failing_operation_is_counted_and_logged(op: CacheOp) -> None:
    cache = FaultyCache(0, failing_ops=frozenset({op}))

    with capture_logs() as logs:
        statuses, events, metrics, _ = deliver_all(cache)

    assert statuses == EXPECTED
    assert len(events.rows) == 3
    failures = [entry for entry in logs if entry["event"] == "cache_error"]
    assert failures
    assert {entry["op"] for entry in failures} == {op}
    assert {entry["error"] for entry in failures} == {"TimeoutError"}
    assert metrics.cache_errors[op] == len(failures)
    assert set(metrics.cache_errors) == {op}


def test_a_dead_cache_sends_every_delivery_to_postgres() -> None:
    cache = FaultyCache(0)

    _, events, _, _ = deliver_all(cache)

    assert events.inserts == len(SEQUENCE)
    assert cache.markers == set()


def test_set_failing_after_commit_keeps_the_event_and_postgres_catches_the_duplicate() -> None:
    cache = FaultyCache(0, failing_ops=frozenset({"set"}))

    statuses, events, _, publisher = deliver_all(cache)

    assert statuses == EXPECTED
    assert len(events.rows) == 3
    assert len(publisher.published) == 3
    assert cache.markers == set()
