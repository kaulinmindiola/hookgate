"""VR-3, real: Redis stopped in the middle of a delivery sequence (FR-IDE-4, NFR-1).

The real Redis service is stopped and started again between deliveries. The provider
never sees an error, duplicates are still detected (by PostgreSQL while Redis is down),
every event is stored and applied once, cache errors are counted, and readiness reports
``degraded`` during the outage and ``ok`` after it.
"""

from collections import Counter
from uuid import UUID

import pytest
from sqlalchemy import Engine
from tests.chaos.services import ServiceControl
from tests.chaos.support import Factory, count, deliver, live_api, spec
from tests.fakes import FixedRng, RecordingMetrics

from hookgate.application.health import Readiness
from hookgate.application.process_event import ProcessEvent
from hookgate.domain.retry import RetryPolicy
from hookgate.infrastructure.cache import RedisDedupeCache
from hookgate.infrastructure.db.transactions import processing_scope
from hookgate.infrastructure.health import database_check, redis_check

pytestmark = pytest.mark.chaos

PRODUCTION_REDIS_TIMEOUT_S = 0.05


class InlineProcessing:
    """Processes each event right after its commit (this test is about Redis, not Celery)."""

    def __init__(self, process: ProcessEvent) -> None:
        self._process = process

    def publish(self, event_id: UUID, *, trace_id: str) -> None:
        self._process(event_id)


def test_redis_down_mid_sequence_never_fails_a_delivery_nor_duplicates_an_effect(
    database_url: str,
    engine: Engine,
    session_factory: Factory,
    redis_url: str,
    redis_service: ServiceControl,
) -> None:
    cache = RedisDedupeCache.from_url(
        redis_url, ttl_s=600, socket_timeout_s=PRODUCTION_REDIS_TIMEOUT_S
    )
    metrics = RecordingMetrics()
    process = ProcessEvent(
        units=processing_scope(session_factory),
        lease_s=60,
        policy=RetryPolicy(max_attempts=3, base_s=0.1, cap_s=0.5),
        rng=FixedRng(0.5),
    )
    readiness = Readiness(
        {"db": database_check(engine), "redis": redis_check(cache)}, critical=frozenset({"db"})
    )
    results: dict[str, list[tuple[int, str]]] = {}

    with live_api(
        database_url,
        engine,
        cache=cache,
        publisher=InlineProcessing(process),
        readiness=readiness,
        metrics=metrics,
    ) as api:

        def send(*event_ids: str) -> None:
            for event_id in event_ids:
                code, result, elapsed = deliver(api, spec(event_id))
                assert elapsed < 2.0, f"{event_id} took {elapsed:.2f} s"
                results.setdefault(event_id, []).append((code, result))

        send("e1", "e2", "e1", "e2")  # Redis up: duplicates caught by the marker
        assert api.get("/health").json()["status"] == "ok"

        redis_service.stop()
        send("e1", "e3", "e3", "e4", "e2")  # Redis down: PostgreSQL catches duplicates
        health = api.get("/health")
        assert (health.status_code, health.json()["status"]) == (200, "degraded")

        redis_service.start()
        send("e3", "e5", "e5", "e1", "e4")  # Redis back
        assert api.get("/health").json()["status"] == "ok"
    cache.close()

    for event_id, answers in results.items():
        assert all(code == 202 for code, _ in answers), (event_id, answers)
        outcomes = Counter(result for _, result in answers)
        assert outcomes["accepted"] == 1, (event_id, answers)
        assert outcomes["duplicate"] == len(answers) - 1, (event_id, answers)
    assert count(session_factory, "webhook_events") == 5
    assert count(session_factory, "applied_effects") == 5
    assert sum(metrics.cache_errors.values()) > 0
    assert metrics.duplicates["postgres"] >= 3  # e1, e3 and e2 during the outage
    assert metrics.duplicates["redis"] >= 2
