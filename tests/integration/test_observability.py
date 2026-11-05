"""Observability against real services (FR-OBS-2, FR-OBS-3, SDD 15.2).

Processing and sweeper counters are checked through the real use cases and database;
readiness probes run against the real PostgreSQL and Redis of the test environment and
against unreachable addresses for the failure cases.
"""

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry
from pydantic import SecretStr
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker
from tests.fakes import FixedClock, FixedRng, InMemoryCache, RecordingMetrics, RecordingPublisher

from hookgate.api.main import create_app
from hookgate.application.health import CheckStatus, HealthStatus, Readiness
from hookgate.application.process_event import EffectHandler, ProcessEvent, ProcessOutcome
from hookgate.application.records import NewEvent
from hookgate.application.sweep import SweepEvents
from hookgate.config import Settings
from hookgate.domain.errors import TransientError
from hookgate.domain.events import Event, EventIdentity
from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.payments import Payment, project_event
from hookgate.domain.retry import RetryPolicy
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.cache import RedisDedupeCache
from hookgate.infrastructure.celery_app import create_celery
from hookgate.infrastructure.db.engine import create_db_engine, session_scope
from hookgate.infrastructure.db.repositories import EventRepository
from hookgate.infrastructure.db.transactions import processing_scope, status_counts, sweep_scope
from hookgate.infrastructure.health import broker_check, database_check, redis_check

pytestmark = pytest.mark.integration

Factory = sessionmaker[Session]
POLICY = RetryPolicy(max_attempts=2, base_s=2.0, cap_s=300.0)
BASE = datetime(2026, 11, 3, 11, 0, tzinfo=UTC)
UNREACHABLE_DB = "postgresql+psycopg://u:p@127.0.0.1:1/none?connect_timeout=2"


def insert_event(
    factory: Factory,
    event_id: str,
    *,
    event_type: str = "payment.succeeded",
    resource_id: str = "pay_1",
    occurred_at: datetime = BASE,
) -> UUID:
    raw = json.dumps(
        {
            "id": event_id,
            "type": event_type,
            "occurred_at": occurred_at.isoformat(),
            "resource": {"type": "payment", "id": resource_id},
            "data": {"amount": 1500, "currency": "USD"},
        }
    ).encode()
    new = NewEvent(
        identity=EventIdentity("sim", event_id),
        event_type=event_type,
        resource_type="payment",
        resource_id=resource_id,
        occurred_at=occurred_at,
        received_at=datetime.now(UTC),
        raw_body=raw,
        headers={},
        trace_id=f"trace-{event_id}",
    )
    with session_scope(factory) as session:
        stored_id = EventRepository(session).insert_if_new(new)
    assert stored_id is not None
    return stored_id


def run_sql(factory: Factory, sql: str, event_id: UUID) -> None:
    with session_scope(factory) as session:
        session.execute(text(sql), {"id": event_id})


def processor(
    factory: Factory, metrics: RecordingMetrics, handler: EffectHandler = project_event
) -> ProcessEvent:
    return ProcessEvent(
        units=processing_scope(factory),
        lease_s=60,
        policy=POLICY,
        rng=FixedRng(1.0),
        handler=handler,
        metrics=metrics,
    )


def always_transient(event: Event) -> Payment:
    raise TransientError("dependency unavailable")


# -- processing and sweeper counters (SDD 15.2) ------------------------------------


def test_processing_counts_applied_and_stale_and_times_each_claim(
    session_factory: Factory,
) -> None:
    metrics = RecordingMetrics()
    process = processor(session_factory, metrics)
    newer = insert_event(session_factory, "evt_new", occurred_at=BASE + timedelta(minutes=5))
    older = insert_event(session_factory, "evt_old")

    assert process(newer).event_outcome is EventOutcome.APPLIED
    assert process(older).event_outcome is EventOutcome.IGNORED_STALE
    assert process(older).outcome is ProcessOutcome.SKIPPED  # not claimed: not timed

    assert metrics.processed_outcomes == {
        EventOutcome.APPLIED: 1,
        EventOutcome.IGNORED_STALE: 1,
    }
    assert len(metrics.durations) == 2
    assert all(seconds > 0 for seconds in metrics.durations)
    assert (metrics.retries, sum(metrics.dead_reasons.values())) == (0, 0)


def test_retries_and_dead_are_counted_with_their_reason(session_factory: Factory) -> None:
    metrics = RecordingMetrics()
    flaky = processor(session_factory, metrics, handler=always_transient)
    poison = insert_event(session_factory, "evt_poison", event_type="payment.exploded")
    unlucky = insert_event(session_factory, "evt_unlucky", resource_id="pay_2")

    assert processor(session_factory, metrics)(poison).outcome is ProcessOutcome.DEAD
    assert flaky(unlucky).outcome is ProcessOutcome.RETRY_SCHEDULED
    run_sql(
        session_factory,
        "UPDATE webhook_events SET next_attempt_at = now() - interval '1 second' WHERE id = :id",
        unlucky,
    )
    assert flaky(unlucky).outcome is ProcessOutcome.DEAD  # max_attempts = 2

    assert metrics.retries == 1
    assert metrics.dead_reasons == {"permanent_error": 1, "transient_error": 1}


def test_sweeper_counts_expired_leases(session_factory: Factory) -> None:
    metrics = RecordingMetrics()
    sweep = SweepEvents(
        units=sweep_scope(session_factory),
        publisher=RecordingPublisher(),
        policy=POLICY,
        rng=FixedRng(1.0),
        metrics=metrics,
    )
    retried = insert_event(session_factory, "evt_crash_1")
    killed = insert_event(session_factory, "evt_crash_2", resource_id="pay_2")
    for event_id, attempts in ((retried, 1), (killed, 2)):
        run_sql(
            session_factory,
            f"UPDATE webhook_events SET status = 'processing', attempts = {attempts}, "
            "locked_until = now() - interval '1 second' WHERE id = :id",
            event_id,
        )

    report = sweep()

    assert (report.expired_to_failed, report.expired_to_dead) == (1, 1)
    assert metrics.retries == 1
    assert metrics.dead_reasons == {"lease_expired": 1}


def test_status_counts_and_the_scraped_gauge(
    session_factory: Factory, engine: Engine, database_url: str
) -> None:
    first = insert_event(session_factory, "evt_a")
    insert_event(session_factory, "evt_b", resource_id="pay_2")
    run_sql(session_factory, "UPDATE webhook_events SET status = 'dead' WHERE id = :id", first)

    assert status_counts(session_factory)() == {EventStatus.RECEIVED: 1, EventStatus.DEAD: 1}

    settings = Settings(
        database_url=database_url,
        redis_url="redis://unused",
        broker_url="amqp://unused:unused@127.0.0.1:1//",
        webhook_secret=SecretStr("s"),
        admin_api_key=SecretStr("k"),
    )
    app = create_app(
        settings,
        engine=engine,
        clock=FixedClock(datetime.now(UTC)),
        cache=InMemoryCache(),
        publisher=RecordingPublisher(),
        metrics_registry=CollectorRegistry(),
        readiness=Readiness({"db": database_check(engine)}, critical=frozenset({"db"})),
    )
    with TestClient(app) as client:
        text_format = client.get("/metrics").text

    assert 'hookgate_events_by_status{status="received"} 1.0' in text_format
    assert 'hookgate_events_by_status{status="dead"} 1.0' in text_format
    assert 'hookgate_events_by_status{status="processed"} 0.0' in text_format


# -- readiness probes (FR-OBS-2) ----------------------------------------------------


@pytest.fixture
def unreachable_engine() -> Iterator[Engine]:
    engine = create_db_engine(UNREACHABLE_DB)
    yield engine
    engine.dispose()


def test_probes_against_live_services_report_ok(
    engine: Engine, dedupe_cache: RedisDedupeCache
) -> None:
    readiness = Readiness(
        {
            "db": database_check(engine),
            "redis": redis_check(dedupe_cache),
            "broker": broker_check(create_celery("memory://", sweep_interval_s=15)),
        },
        critical=frozenset({"db"}),
    )

    report = readiness()

    assert report.status is HealthStatus.OK
    assert set(report.checks.values()) == {CheckStatus.OK}


def test_unreachable_redis_and_broker_degrade_and_unreachable_database_is_down(
    engine: Engine, unreachable_engine: Engine
) -> None:
    dead_redis = RedisDedupeCache.from_url("redis://127.0.0.1:1/0", ttl_s=60, socket_timeout_s=0.05)
    dead_broker = broker_check(create_celery("amqp://u:p@127.0.0.1:1//", sweep_interval_s=15))

    degraded = Readiness(
        {"db": database_check(engine), "redis": redis_check(dead_redis), "broker": dead_broker},
        critical=frozenset({"db"}),
    )()
    down = Readiness({"db": database_check(unreachable_engine)}, critical=frozenset({"db"}))()

    assert degraded.status is HealthStatus.DEGRADED
    assert degraded.checks == {
        "db": CheckStatus.OK,
        "redis": CheckStatus.DOWN,
        "broker": CheckStatus.DOWN,
    }
    assert down.status is HealthStatus.DOWN
    dead_redis.close()
