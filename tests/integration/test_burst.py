"""VR-7: 32 simultaneous deliveries of one event (FR-IDE-2/5, P9).

Real uvicorn, real HTTP from 32 threads released together by a barrier, real
PostgreSQL. Run with Redis up and with Redis unreachable: in both cases one row, one
audit row, one publish, one ``accepted`` and 31 ``duplicate`` responses.
"""

import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker
from tests.fakes import FixedClock, RecordingPublisher
from tests.live_server import live_server

from hookgate.api.main import create_app
from hookgate.application.records import EventQuery
from hookgate.config import Settings
from hookgate.domain.signature import build_signature_header
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.cache import RedisDedupeCache
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.repositories import EventRepository

pytestmark = pytest.mark.integration

WORKERS = 32
SECRET = "burst-test-secret"
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)
RAW = json.dumps(
    {
        "id": "evt_burst",
        "type": "payment.created",
        "occurred_at": "2026-11-03T11:59:00Z",
        "resource": {"type": "payment", "id": "pay_burst"},
        "data": {"amount": 1500, "currency": "USD"},
    }
).encode()
HEADERS = {
    "Content-Type": "application/json",
    "Hookgate-Signature": build_signature_header(SECRET, int(NOW.timestamp()), RAW),
}


@pytest.fixture(params=["redis_up", "redis_down"])
def cache(
    request: pytest.FixtureRequest, dedupe_cache: RedisDedupeCache
) -> Iterator[RedisDedupeCache]:
    if request.param == "redis_up":
        yield dedupe_cache
        return
    unreachable = RedisDedupeCache.from_url(
        "redis://127.0.0.1:1/0", ttl_s=60, socket_timeout_s=0.05
    )
    yield unreachable
    unreachable.close()


def deliver_concurrently(url: str) -> list[tuple[int, str]]:
    barrier = Barrier(WORKERS)

    def deliver(_: int) -> tuple[int, str]:
        with httpx.Client(timeout=30) as client:
            barrier.wait(timeout=10)
            response = client.post(f"{url}/webhooks/payments", content=RAW, headers=HEADERS)
            return response.status_code, str(response.json()["status"])

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        return list(pool.map(deliver, range(WORKERS)))


def test_simultaneous_deliveries_store_and_publish_exactly_once(
    database_url: str,
    engine: Engine,
    session_factory: sessionmaker[Session],
    cache: RedisDedupeCache,
) -> None:
    settings = Settings(
        database_url=database_url,
        redis_url="redis://unused",
        broker_url="amqp://unused",
        webhook_secret=SecretStr(SECRET),
        admin_api_key=SecretStr("unused-admin-key"),
    )
    publisher = RecordingPublisher()
    app = create_app(
        settings, engine=engine, clock=FixedClock(NOW), publisher=publisher, cache=cache
    )

    with live_server(app) as url:
        results = deliver_concurrently(url)

    assert [status_code for status_code, _ in results] == [202] * WORKERS
    acks = sorted(ack for _, ack in results)
    assert acks.count("accepted") == 1
    assert acks.count("duplicate") == WORKERS - 1
    assert len(publisher.published) == 1

    with session_scope(session_factory) as session:
        rows = int(session.execute(text("SELECT count(*) FROM webhook_events")).scalar_one())
        repository = EventRepository(session)
        (stored,) = repository.list_events(EventQuery(statuses=(EventStatus.RECEIVED,)), limit=10)
        transitions = repository.list_transitions(stored.id)
    assert rows == 1
    assert publisher.published[0][0] == stored.id
    assert [(t.from_status, t.to_status, t.reason) for t in transitions] == [
        (None, EventStatus.RECEIVED, "ingested")
    ]
