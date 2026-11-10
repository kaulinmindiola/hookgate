"""POST /webhooks/payments against a real PostgreSQL and Redis (FR-ING-5, FR-IDE-2..6)."""

import json
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker
from tests.fakes import FixedClock, RecordingPublisher

from hookgate.api.main import create_app
from hookgate.application.records import EventQuery
from hookgate.config import Settings
from hookgate.domain.events import EventIdentity
from hookgate.domain.signature import build_signature_header
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.cache import RedisDedupeCache
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.repositories import EventRepository

pytestmark = pytest.mark.integration

SECRET = "integration-test-secret"
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)
URL = "/webhooks/payments"


def make_client(database_url: str, engine: Engine, cache: RedisDedupeCache) -> TestClient:
    settings = Settings(
        database_url=database_url,
        redis_url="redis://unused",
        broker_url="amqp://unused:unused@127.0.0.1:5672//",
        webhook_secret=SecretStr(SECRET),
        admin_api_key=SecretStr("unused-admin-key"),
    )
    app = create_app(
        settings, engine=engine, clock=FixedClock(NOW), cache=cache, publisher=RecordingPublisher()
    )
    return TestClient(app)


@pytest.fixture
def client(
    database_url: str, engine: Engine, dedupe_cache: RedisDedupeCache
) -> Iterator[TestClient]:
    with make_client(database_url, engine, dedupe_cache) as test_client:
        yield test_client


def body(event_id: str = "evt_1", amount: int = 1500) -> bytes:
    return json.dumps(
        {
            "id": event_id,
            "type": "payment.succeeded",
            "occurred_at": "2026-11-03T11:59:00.123456Z",
            "resource": {"type": "payment", "id": "pay_1"},
            "data": {"amount": amount, "currency": "USD"},
        }
    ).encode()


def post(client: TestClient, raw: bytes, **extra_headers: str) -> tuple[int, str | None]:
    """Signed delivery. Returns the status code and the ``status`` field of a 202."""
    headers = {
        "Content-Type": "application/json",
        "Hookgate-Signature": build_signature_header(SECRET, int(NOW.timestamp()), raw),
        **extra_headers,
    }
    response = client.post(URL, content=raw, headers=headers)
    ack: str | None = response.json()["status"] if response.status_code == 202 else None
    return response.status_code, ack


def count_events(session_factory: sessionmaker[Session]) -> int:
    with session_scope(session_factory) as session:
        return int(session.execute(text("SELECT count(*) FROM webhook_events")).scalar_one())


def test_accepted_delivery_is_durable_with_exact_bytes_and_audit_row(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    raw = body()[:-1] + b' , "padding":  "  keeps  spacing "}'

    assert post(client, raw, **{"X-Trace-Id": "it-trace-1"}) == (202, "accepted")
    with session_scope(session_factory) as session:
        repository = EventRepository(session)
        (stored,) = repository.list_events(EventQuery(statuses=(EventStatus.RECEIVED,)), limit=10)
        transitions = repository.list_transitions(stored.id)
    assert stored.raw_body == raw
    assert (stored.provider, stored.event_id) == ("sim", "evt_1")
    assert stored.received_at == NOW
    assert stored.occurred_at == datetime(2026, 11, 3, 11, 59, 0, 123456, tzinfo=UTC)
    assert stored.trace_id == "it-trace-1"
    assert stored.headers["content-type"] == "application/json"
    assert [(t.from_status, t.to_status, t.reason) for t in transitions] == [
        (None, EventStatus.RECEIVED, "ingested")
    ]


def test_duplicate_is_202_and_keeps_one_row_with_the_original_body(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    original = body(amount=1500)

    results = [post(client, original), post(client, original), post(client, body(amount=9999))]

    assert results == [(202, "accepted"), (202, "duplicate"), (202, "duplicate")]
    assert count_events(session_factory) == 1
    with session_scope(session_factory) as session:
        stored_body = EventRepository(session).find_raw_body(EventIdentity("sim", "evt_1"))
    assert stored_body == original


def test_rejected_deliveries_persist_nothing(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    raw = body()

    unsigned = client.post(URL, content=raw).status_code
    not_json, _ = post(client, b"{broken")

    assert (unsigned, not_json) == (401, 400)
    assert count_events(session_factory) == 0


def test_accepted_event_leaves_a_redis_marker(
    client: TestClient, dedupe_cache: RedisDedupeCache
) -> None:
    assert post(client, body()) == (202, "accepted")

    assert dedupe_cache.seen(EventIdentity("sim", "evt_1")) is True


def test_unreachable_redis_degrades_to_postgres(
    database_url: str, engine: Engine, session_factory: sessionmaker[Session]
) -> None:
    unreachable = RedisDedupeCache.from_url(
        "redis://127.0.0.1:1/0", ttl_s=60, socket_timeout_s=0.05
    )
    try:
        with make_client(database_url, engine, unreachable) as degraded:
            first = post(degraded, body())
            second = post(degraded, body())
    finally:
        unreachable.close()

    assert (first, second) == ((202, "accepted"), (202, "duplicate"))
    assert count_events(session_factory) == 1
