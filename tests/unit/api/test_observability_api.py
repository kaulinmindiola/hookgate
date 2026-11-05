"""Health and metrics endpoints (FR-OBS-2, FR-OBS-3). No database, Redis or broker."""

import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry
from pydantic import SecretStr
from tests.fakes import FixedClock, InMemoryCache, InMemoryEvents, RecordingPublisher

from hookgate.api.main import create_app
from hookgate.application.health import Readiness
from hookgate.config import Settings
from hookgate.domain.signature import build_signature_header
from hookgate.domain.states import EventStatus

SECRET = "observability-secret"
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)


class Switch:
    """A health check whose result a test can flip."""

    def __init__(self) -> None:
        self.up = True

    def __call__(self) -> None:
        if not self.up:
            raise ConnectionError("down")


@dataclass
class Harness:
    client: TestClient
    registry: CollectorRegistry
    db: Switch
    redis: Switch
    broker: Switch


@pytest.fixture
def harness() -> Iterator[Harness]:
    db, redis, broker = Switch(), Switch(), Switch()
    registry = CollectorRegistry()
    settings = Settings(
        database_url="postgresql+psycopg://unused:unused@127.0.0.1:5432/unused",
        redis_url="redis://127.0.0.1:6379/0",
        broker_url="amqp://unused:unused@127.0.0.1:5672//",
        webhook_secret=SecretStr(SECRET),
        admin_api_key=SecretStr("unit-admin-key"),
    )
    app = create_app(
        settings,
        events=InMemoryEvents().scope,
        admin_events=lambda: pytest.fail("admin store not expected"),
        clock=FixedClock(NOW),
        publisher=RecordingPublisher(),
        cache=InMemoryCache(),
        metrics_registry=registry,
        event_counts=lambda: {EventStatus.PROCESSED: 3, EventStatus.DEAD: 1},
        readiness=Readiness(
            {"db": db, "redis": redis, "broker": broker}, critical=frozenset({"db"})
        ),
    )
    with TestClient(app) as client:
        yield Harness(client, registry, db, redis, broker)


def post(client: TestClient, event_id: str, *, signed: bool = True) -> int:
    raw = json.dumps(
        {
            "id": event_id,
            "type": "payment.succeeded",
            "occurred_at": "2026-11-03T11:59:00Z",
            "resource": {"type": "payment", "id": "pay_1"},
            "data": {"amount": 1, "currency": "USD"},
        }
    ).encode()
    header = build_signature_header(SECRET, int(NOW.timestamp()), raw) if signed else "bad"
    response = client.post(
        "/webhooks/payments",
        content=raw,
        headers={"Content-Type": "application/json", "Hookgate-Signature": header},
    )
    return int(response.status_code)


# -- health ------------------------------------------------------------------------


def test_liveness_ignores_dependencies(harness: Harness) -> None:
    harness.db.up = harness.redis.up = harness.broker.up = False

    response = harness.client.get("/health/live")

    assert (response.status_code, response.json()) == (200, {"status": "ok"})


def test_readiness_ok(harness: Harness) -> None:
    response = harness.client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "checks": {"db": "ok", "redis": "ok", "broker": "ok"},
    }


@pytest.mark.parametrize("failing", ["redis", "broker"])
def test_redis_or_broker_down_is_degraded_but_200(harness: Harness, failing: str) -> None:
    getattr(harness, failing).up = False

    response = harness.client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "degraded"
    assert response.json()["checks"][failing] == "down"


def test_database_down_is_503(harness: Harness) -> None:
    harness.db.up = False

    response = harness.client.get("/health")

    assert response.status_code == 503
    assert response.json() == {
        "status": "down",
        "checks": {"db": "down", "redis": "ok", "broker": "ok"},
    }


# -- metrics -----------------------------------------------------------------------


def test_metrics_endpoint_serves_the_text_format(harness: Harness) -> None:
    response = harness.client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert 'hookgate_events_by_status{status="processed"} 3.0' in response.text
    assert 'hookgate_events_by_status{status="received"} 0.0' in response.text


def test_counters_follow_a_known_sequence_of_deliveries(harness: Harness) -> None:
    assert post(harness.client, "evt_1") == 202
    assert post(harness.client, "evt_1") == 202  # duplicate (Redis marker)
    assert post(harness.client, "evt_2") == 202
    assert post(harness.client, "evt_3", signed=False) == 401
    too_large = harness.client.post(
        "/webhooks/payments", content=b"x" * (1024 * 1024 + 1), headers={"Hookgate-Signature": "x"}
    )
    assert too_large.status_code == 413

    def sample(name: str, **labels: str) -> float | None:
        return harness.registry.get_sample_value(name, labels)

    assert sample("hookgate_events_received_total", outcome="accepted") == 2
    assert sample("hookgate_events_received_total", outcome="duplicate") == 1
    assert sample("hookgate_events_received_total", outcome="rejected") == 2
    assert sample("hookgate_duplicates_total", layer="redis") == 1
    assert sample("hookgate_duplicates_total", layer="postgres") == 0
    assert (
        'hookgate_events_received_total{outcome="accepted"} 2.0'
        in harness.client.get("/metrics").text
    )
