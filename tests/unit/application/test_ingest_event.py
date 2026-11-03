"""Use case rules that do not need HTTP or a database."""

import json
from datetime import UTC, datetime

import pytest
from structlog.testing import capture_logs
from tests.fakes import FixedClock, InMemoryEvents, RecordingPublisher

from hookgate.application.ingest_event import (
    IngestCommand,
    IngestEvent,
    IngestStatus,
    InvalidPayload,
    parse_envelope,
)
from hookgate.domain.errors import SignatureError, SignatureFailure
from hookgate.domain.signature import build_signature_header

SECRET = "unit-test-secret"
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)


def body(**overrides: object) -> bytes:
    envelope: dict[str, object] = {
        "id": "evt_1",
        "type": "payment.created",
        "occurred_at": "2026-11-03T11:59:00.123456Z",
        "resource": {"type": "payment", "id": "pay_1"},
        "data": {"amount": 1500, "currency": "USD"},
    }
    envelope.update(overrides)
    return json.dumps(envelope).encode()


def command(raw: bytes, *, signed: bool = True, trace_id: str = "trace-1") -> IngestCommand:
    header = build_signature_header(SECRET, int(NOW.timestamp()), raw) if signed else None
    return IngestCommand(
        raw_body=raw,
        signature_header=header,
        headers={"content-type": "application/json", "cookie": "session=abc"},
        trace_id=trace_id,
    )


def make_use_case(
    events: InMemoryEvents, publisher: RecordingPublisher | None = None
) -> IngestEvent:
    return IngestEvent(
        secret=SECRET,
        tolerance_s=300,
        clock=FixedClock(NOW),
        events=events.scope,
        publisher=publisher or RecordingPublisher(),
    )


def test_accepted_event_is_stored_with_receive_time_trace_and_filtered_headers() -> None:
    events = InMemoryEvents()
    raw = body()

    result = make_use_case(events)(command(raw))

    stored = events.stored("evt_1")
    assert result.status is IngestStatus.ACCEPTED
    assert result.stored_id is not None
    assert stored.raw_body == raw
    assert stored.received_at == NOW
    assert stored.trace_id == "trace-1"
    assert stored.occurred_at == datetime(2026, 11, 3, 11, 59, 0, 123456, tzinfo=UTC)
    assert (stored.event_type, stored.resource_type, stored.resource_id) == (
        "payment.created",
        "payment",
        "pay_1",
    )
    assert dict(stored.headers) == {"content-type": "application/json"}


def test_publish_happens_after_the_commit() -> None:
    journal: list[str] = []
    events = InMemoryEvents(journal)
    publisher = RecordingPublisher(journal)

    result = make_use_case(events, publisher)(command(body()))

    assert journal == ["commit", "publish"]
    assert publisher.published == [(result.stored_id, "trace-1")]


def test_publish_failure_still_accepts_the_stored_event() -> None:
    events = InMemoryEvents()

    with capture_logs() as logs:
        result = make_use_case(events, RecordingPublisher(fail=True))(command(body()))

    assert result.status is IngestStatus.ACCEPTED
    assert "evt_1" in {identity.event_id for identity in events.rows}
    assert any(entry["event"] == "publish_failed" for entry in logs)


def test_duplicate_is_not_published_again() -> None:
    events = InMemoryEvents()
    publisher = RecordingPublisher()
    ingest = make_use_case(events, publisher)

    first = ingest(command(body()))
    second = ingest(command(body()))

    assert first.status is IngestStatus.ACCEPTED
    assert second.status is IngestStatus.DUPLICATE
    assert second.stored_id is None
    assert len(publisher.published) == 1


def test_duplicate_with_a_different_body_keeps_the_original_and_warns() -> None:
    events = InMemoryEvents()
    ingest = make_use_case(events)
    original = body()
    ingest(command(original))

    with capture_logs() as logs:
        result = ingest(command(body(data={"amount": 9999, "currency": "USD"})))

    assert result.status is IngestStatus.DUPLICATE
    assert events.stored("evt_1").raw_body == original
    assert [entry["event"] for entry in logs] == ["payload_mismatch", "webhook_duplicate"]


def test_signature_is_checked_before_the_body_is_parsed() -> None:
    events = InMemoryEvents()

    with pytest.raises(SignatureError) as exc_info:
        make_use_case(events)(command(b"not json at all", signed=False))

    assert exc_info.value.reason is SignatureFailure.MISSING
    assert events.rows == {}


def test_invalid_payload_is_rejected_and_nothing_is_stored() -> None:
    events = InMemoryEvents()

    with pytest.raises(InvalidPayload):
        make_use_case(events)(command(b"{not json"))

    assert events.rows == {}


@pytest.mark.parametrize(
    ("raw", "location"),
    [
        (body(id=""), "id"),
        (body(type=7), "type"),
        (body(occurred_at="2026-11-03T11:59:00"), "occurred_at"),
        (body(occurred_at=1_762_170_000), "occurred_at"),
        (body(resource={"type": "payment"}), "resource.id"),
        (b"[]", "body"),
    ],
)
def test_parse_envelope_reports_the_failing_field(raw: bytes, location: str) -> None:
    with pytest.raises(InvalidPayload) as exc_info:
        parse_envelope(raw)

    assert any(problem.startswith(f"{location}:") for problem in exc_info.value.problems)


def test_problems_never_echo_the_submitted_values() -> None:
    marker = "SENSITIVE-VALUE-123"

    with pytest.raises(InvalidPayload) as exc_info:
        parse_envelope(body(id=marker, occurred_at=marker))

    assert marker not in " ".join(exc_info.value.problems)


def test_unknown_fields_and_data_are_ignored_by_the_envelope() -> None:
    envelope = parse_envelope(body(extra_field=True, data="anything"))

    assert envelope.id == "evt_1"
