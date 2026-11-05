"""The simulator against the real API and database (FR-SIM-1..3).

A real uvicorn server runs the production app on PostgreSQL and Redis. Instead of
Celery, the publisher processes each event right after its commit with the real
``process_event`` use case, so every scenario can be verified end to end without a
broker. The compose smoke run with a real worker comes in phase F11.
"""

from collections.abc import Iterator
from uuid import UUID

import httpx
import pytest
from prometheus_client import CollectorRegistry
from pydantic import SecretStr
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker
from tests.fakes import FixedRng
from tests.live_server import live_server

from hookgate.api.main import create_app
from hookgate.application.health import Readiness
from hookgate.application.process_event import ProcessEvent
from hookgate.config import Settings
from hookgate.domain.retry import RetryPolicy
from hookgate.infrastructure.cache import RedisDedupeCache
from hookgate.infrastructure.db.transactions import processing_scope
from hookgate.infrastructure.health import database_check
from simulator.client import AdminClient, ProviderClient
from simulator.oracle import check_responses
from simulator.runner import send, verify
from simulator.scenarios import SCENARIOS, build_plan

pytestmark = pytest.mark.integration

SECRET = "simulator-integration-secret"
ADMIN_KEY = "simulator-integration-key"


class InlineProcessing:
    """Publisher that processes the event at once (stands in for broker plus worker)."""

    def __init__(self, process: ProcessEvent) -> None:
        self._process = process

    def publish(self, event_id: UUID, *, trace_id: str) -> None:
        self._process(event_id)


@pytest.fixture
def base_url(
    database_url: str,
    engine: Engine,
    session_factory: sessionmaker[Session],
    dedupe_cache: RedisDedupeCache,
) -> Iterator[str]:
    settings = Settings(
        database_url=database_url,
        redis_url="redis://unused",
        broker_url="amqp://unused:unused@127.0.0.1:1//",
        webhook_secret=SecretStr(SECRET),
        admin_api_key=SecretStr(ADMIN_KEY),
    )
    process = ProcessEvent(
        units=processing_scope(session_factory),
        lease_s=60,
        policy=RetryPolicy(max_attempts=3, base_s=0.1, cap_s=0.5),
        rng=FixedRng(0.5),
    )
    app = create_app(
        settings,
        engine=engine,
        cache=dedupe_cache,
        publisher=InlineProcessing(process),
        metrics_registry=CollectorRegistry(),
        readiness=Readiness({"db": database_check(engine)}, critical=frozenset({"db"})),
    )
    with live_server(app) as url:
        yield url


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_every_scenario_passes_send_and_verify(base_url: str, scenario: str) -> None:
    plan = build_plan(scenario, seed=7, run_id=f"it_{scenario}")

    with httpx.Client(base_url=base_url, timeout=30) as http:
        responses = send(plan, ProviderClient(http, secret=SECRET), sleep=lambda _: None)
        report = verify(plan, AdminClient(http, api_key=ADMIN_KEY), timeout_s=10)

    assert check_responses(plan, responses) == []
    assert report.problems == []


def test_the_same_plan_can_be_verified_again_later(base_url: str) -> None:
    plan = build_plan("chaos", seed=11, run_id="it_again")
    with httpx.Client(base_url=base_url, timeout=30) as http:
        send(plan, ProviderClient(http, secret=SECRET), sleep=lambda _: None)
        rebuilt = build_plan("chaos", seed=11, run_id="it_again")
        report = verify(rebuilt, AdminClient(http, api_key=ADMIN_KEY), timeout_s=10)

    assert report.ok
    assert len(report.observed) == len(
        {d.event.event_id for d in plan.deliveries if d.signing.value == "valid"}
    )


def test_verify_reports_events_the_system_never_received(base_url: str) -> None:
    """The oracle against a real database: nothing sent, so everything is missing."""
    plan = build_plan("happy", seed=3, run_id="it_never_sent")
    with httpx.Client(base_url=base_url, timeout=30) as http:
        report = verify(plan, AdminClient(http, api_key=ADMIN_KEY), timeout_s=0)

    assert not report.ok
    assert report.problems[0].endswith("not final after 0 s")
    assert sum("not stored" in p for p in report.problems) == len(plan.deliveries)


def test_a_wrong_secret_is_rejected_by_the_real_api(base_url: str) -> None:
    plan = build_plan("happy", seed=5, run_id="it_wrong_secret")
    with httpx.Client(base_url=base_url, timeout=30) as http:
        responses = send(plan, ProviderClient(http, secret="not-it"), sleep=lambda _: None)

    assert {(r.status_code, r.result) for r in responses} == {(401, "invalid_signature")}
    assert check_responses(plan, responses)  # the oracle flags every delivery
