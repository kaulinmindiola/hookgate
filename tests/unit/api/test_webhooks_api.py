"""HTTP contract of POST /webhooks/payments with in-memory fakes (no database)."""

import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from structlog.testing import capture_logs
from tests.fakes import FixedClock, InMemoryCache, InMemoryEvents, RecordingPublisher

from hookgate.api.deps import MAX_BODY_BYTES
from hookgate.api.main import create_app
from hookgate.config import Settings
from hookgate.domain.signature import build_signature_header

SECRET = "api-test-secret"
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)
NOW_TS = int(NOW.timestamp())
URL = "/webhooks/payments"
BODY_MARKER = "BODY-MARKER-7f3a"


@dataclass
class Harness:
    client: TestClient
    events: InMemoryEvents
    publisher: RecordingPublisher


def settings() -> Settings:
    return Settings(
        database_url="postgresql+psycopg://unused:unused@127.0.0.1:5432/unused",
        redis_url="redis://127.0.0.1:6379/0",
        broker_url="amqp://unused:unused@127.0.0.1:5672//",
        webhook_secret=SecretStr(SECRET),
        admin_api_key=SecretStr("unused-admin-key"),
    )


@pytest.fixture
def harness() -> Iterator[Harness]:
    events = InMemoryEvents()
    publisher = RecordingPublisher()
    app = create_app(
        settings(),
        events=events.scope,
        clock=FixedClock(NOW),
        publisher=publisher,
        cache=InMemoryCache(),
    )
    with TestClient(app) as client:
        yield Harness(client, events, publisher)


def body(event_id: str = "evt_1", **overrides: object) -> bytes:
    envelope: dict[str, object] = {
        "id": event_id,
        "type": "payment.succeeded",
        "occurred_at": "2026-11-03T11:59:00Z",
        "resource": {"type": "payment", "id": "pay_1"},
        "data": {"amount": 1500, "currency": "USD", "note": BODY_MARKER},
    }
    envelope.update(overrides)
    return json.dumps(envelope).encode()


def signed(raw: bytes, timestamp: int = NOW_TS, secret: str = SECRET) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Hookgate-Signature": build_signature_header(secret, timestamp, raw),
    }


# -- success and duplicates -----------------------------------------------------------


def test_valid_delivery_is_accepted_and_stored_byte_for_byte(harness: Harness) -> None:
    raw = b'{"id":"evt_1",  "type":"payment.created","occurred_at":"2026-11-03T11:59:00Z",\n'
    raw += b'"resource":{"type":"payment","id":"pay_1"},"data":{}}'

    response = harness.client.post(URL, content=raw, headers=signed(raw))

    assert response.status_code == 202
    assert response.json() == {"status": "accepted"}
    stored = harness.events.stored("evt_1")
    assert stored.raw_body == raw
    assert stored.received_at == NOW
    assert stored.headers["hookgate-signature"].startswith(f"t={NOW_TS},v1=")
    assert stored.trace_id == response.headers["X-Trace-Id"]
    assert len(harness.publisher.published) == 1


def test_caller_trace_id_is_kept_and_echoed(harness: Harness) -> None:
    raw = body()
    headers = {**signed(raw), "X-Trace-Id": "req-42.abc"}

    response = harness.client.post(URL, content=raw, headers=headers)

    assert response.headers["X-Trace-Id"] == "req-42.abc"
    assert harness.events.stored("evt_1").trace_id == "req-42.abc"
    assert harness.publisher.published[0][1] == "req-42.abc"


@pytest.mark.parametrize("bad_trace_id", ["has spaces", "x" * 129, "semi;colon"])
def test_unsafe_trace_id_is_replaced(harness: Harness, bad_trace_id: str) -> None:
    raw = body()
    headers = {**signed(raw), "X-Trace-Id": bad_trace_id}

    response = harness.client.post(URL, content=raw, headers=headers)

    generated = response.headers["X-Trace-Id"]
    assert generated != bad_trace_id
    assert len(generated) == 32
    assert harness.events.stored("evt_1").trace_id == generated


def test_duplicate_delivery_returns_202_duplicate_and_is_not_published(
    harness: Harness,
) -> None:
    raw = body()
    first = harness.client.post(URL, content=raw, headers=signed(raw))
    second = harness.client.post(URL, content=raw, headers=signed(raw))

    assert first.json() == {"status": "accepted"}
    assert second.status_code == 202
    assert second.json() == {"status": "duplicate"}
    assert len(harness.events.rows) == 1
    assert len(harness.publisher.published) == 1


def test_duplicate_with_a_different_body_keeps_the_original(harness: Harness) -> None:
    original = body()
    changed = body(data={"amount": 1, "currency": "EUR"})
    harness.client.post(URL, content=original, headers=signed(original))

    response = harness.client.post(URL, content=changed, headers=signed(changed))

    assert response.json() == {"status": "duplicate"}
    assert harness.events.stored("evt_1").raw_body == original


def test_publish_failure_still_returns_202(harness: Harness) -> None:
    harness.publisher.fail = True
    raw = body()

    response = harness.client.post(URL, content=raw, headers=signed(raw))

    assert response.status_code == 202
    assert len(harness.events.rows) == 1


# -- rejections ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("headers", "reason"),
    [
        ({}, "missing_signature"),
        ({"Hookgate-Signature": "garbage"}, "malformed_signature"),
        ({"Hookgate-Signature": f"t={NOW_TS},v1={'0' * 64}"}, "invalid_signature"),
    ],
)
def test_bad_signature_is_401_and_nothing_is_stored(
    harness: Harness, headers: dict[str, str], reason: str
) -> None:
    response = harness.client.post(URL, content=body(), headers=headers)

    assert response.status_code == 401
    assert response.json() == {"error": reason}
    assert harness.events.rows == {}
    assert harness.publisher.published == []


def test_signature_with_the_wrong_secret_is_401(harness: Harness) -> None:
    raw = body()

    response = harness.client.post(URL, content=raw, headers=signed(raw, secret="other"))

    assert response.status_code == 401
    assert response.json() == {"error": "invalid_signature"}


@pytest.mark.parametrize("skew", [-301, 301])
def test_timestamp_outside_the_window_is_401(harness: Harness, skew: int) -> None:
    raw = body()

    response = harness.client.post(URL, content=raw, headers=signed(raw, NOW_TS + skew))

    assert response.status_code == 401
    assert response.json() == {"error": "timestamp_out_of_tolerance"}
    assert harness.events.rows == {}


@pytest.mark.parametrize("skew", [-300, 300])
def test_timestamp_at_the_window_edge_is_accepted(harness: Harness, skew: int) -> None:
    raw = body()

    response = harness.client.post(URL, content=raw, headers=signed(raw, NOW_TS + skew))

    assert response.status_code == 202


@pytest.mark.parametrize(
    "raw",
    [
        b"{not json",
        b"",
        body(id=""),
        body(occurred_at="2026-11-03T11:59:00"),
        json.dumps({"id": "evt_1", "type": "payment.created"}).encode(),
    ],
)
def test_invalid_payload_is_400_and_nothing_is_stored(harness: Harness, raw: bytes) -> None:
    response = harness.client.post(URL, content=raw, headers=signed(raw))

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_payload"
    assert response.json()["detail"]
    assert harness.events.rows == {}


def test_body_over_one_mib_is_413(harness: Harness) -> None:
    raw = b"x" * (MAX_BODY_BYTES + 1)

    response = harness.client.post(URL, content=raw, headers=signed(raw))

    assert response.status_code == 413
    assert response.json() == {"error": "payload_too_large"}
    assert "X-Trace-Id" in response.headers
    assert harness.events.rows == {}


def test_body_of_exactly_one_mib_passes_the_size_check(harness: Harness) -> None:
    raw = b"x" * MAX_BODY_BYTES

    response = harness.client.post(URL, content=raw, headers=signed(raw))

    assert response.status_code == 400  # signed and within the limit, but not JSON


def test_streamed_body_over_the_limit_is_413_without_content_length(harness: Harness) -> None:
    def chunks() -> Iterator[bytes]:
        for _ in range(17):
            yield b"x" * 65_536

    response = harness.client.post(URL, content=chunks(), headers={"Hookgate-Signature": "x"})

    assert response.status_code == 413


# -- logging and health ----------------------------------------------------------------


def test_logs_never_contain_the_body_or_the_secret(harness: Harness) -> None:
    raw = body()

    with capture_logs() as logs:
        harness.client.post(URL, content=raw, headers=signed(raw))
        harness.client.post(URL, content=raw, headers=signed(raw))
        harness.client.post(URL, content=raw, headers={"Hookgate-Signature": "garbage"})

    text = repr(logs)
    assert [entry["event"] for entry in logs] == [
        "webhook_accepted",
        "webhook_duplicate",
        "webhook_rejected",
    ]
    assert all("trace_id" in entry for entry in logs)
    assert BODY_MARKER not in text
    assert SECRET not in text


def test_health_is_200(harness: Harness) -> None:
    response = harness.client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
