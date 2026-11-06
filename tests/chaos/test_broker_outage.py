"""VR-9, real: RabbitMQ stopped during ingestion (FR-ING-7, FR-PRO-7, NFR-1).

A real worker process consumes from the real RabbitMQ. While the broker is stopped,
deliveries are still acknowledged (202) quickly: the event is durable and the failed
publish is only logged. After the broker returns, new deliveries flow again (the
publisher reconnects, regression of the F7 finding) and the sweeper enqueues the event
whose message was never sent. Every event is applied exactly once.
"""

import pytest
from sqlalchemy import Engine, text
from tests.chaos.services import ServiceControl, wait_until
from tests.chaos.support import (
    Factory,
    count,
    deliver,
    find,
    insert_event,
    live_api,
    spec,
    wait_for_status,
)
from tests.chaos.workers import StartWorker
from tests.fakes import FixedRng, InMemoryCache

from hookgate.application.health import Readiness
from hookgate.application.sweep import SweepEvents
from hookgate.domain.retry import RetryPolicy
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.transactions import sweep_scope
from hookgate.infrastructure.health import database_check
from hookgate.infrastructure.queue import CeleryPublisher

pytestmark = pytest.mark.chaos

S = EventStatus


def test_broker_down_at_ingestion_still_acknowledges_and_nothing_is_lost(
    database_url: str,
    engine: Engine,
    session_factory: Factory,
    start_worker: StartWorker,
    publisher: CeleryPublisher,
    rabbitmq_service: ServiceControl,
) -> None:
    start_worker()
    sweep = SweepEvents(
        units=sweep_scope(session_factory),
        publisher=publisher,
        policy=RetryPolicy(max_attempts=5, base_s=0.2, cap_s=0.5),
        rng=FixedRng(1.0),
        received_grace_s=1,
    )
    readiness = Readiness({"db": database_check(engine)}, critical=frozenset({"db"}))

    with live_api(
        database_url, engine, cache=InMemoryCache(), publisher=publisher, readiness=readiness
    ) as api:
        assert deliver(api, spec("b1"))[:2] == (202, "accepted")
        wait_for_status(session_factory, "b1", S.PROCESSED)

        rabbitmq_service.stop()
        code, result, elapsed = deliver(api, spec("b2"))
        assert (code, result) == (202, "accepted")
        assert elapsed < 5.0, f"ingestion blocked {elapsed:.2f} s on a dead broker"
        stored = find(session_factory, "b2")
        assert stored is not None
        assert stored.status is S.RECEIVED  # durable, waiting for the sweeper

        rabbitmq_service.start()
        assert deliver(api, spec("b3"))[:2] == (202, "accepted")
        wait_for_status(session_factory, "b3", S.PROCESSED, timeout_s=90)  # no sweeper needed

        def b2_recovered() -> bool:
            sweep()
            event = find(session_factory, "b2")
            return event is not None and event.status is S.PROCESSED

        wait_until(b2_recovered, timeout_s=60, poll_s=0.5, what="b2 to be recovered")

    assert count(session_factory, "webhook_events") == 3
    assert count(session_factory, "applied_effects") == 3


def test_publisher_recovers_after_an_idle_broker_restart(
    database_url: str,
    engine: Engine,
    session_factory: Factory,
    start_worker: StartWorker,
    publisher: CeleryPublisher,
    rabbitmq_service: ServiceControl,
) -> None:
    """F7 finding: without traffic during the outage, the API's first publish after a
    broker restart used the dead channel and was lost. It must go through at once."""
    start_worker()
    readiness = Readiness({"db": database_check(engine)}, critical=frozenset({"db"}))

    with live_api(
        database_url, engine, cache=InMemoryCache(), publisher=publisher, readiness=readiness
    ) as api:
        assert deliver(api, spec("c1"))[:2] == (202, "accepted")
        wait_for_status(session_factory, "c1", S.PROCESSED)

        rabbitmq_service.stop()
        rabbitmq_service.start()  # nothing was sent while it was down

        assert deliver(api, spec("c2"))[:2] == (202, "accepted")
        wait_for_status(session_factory, "c2", S.PROCESSED, timeout_s=90)  # no sweeper


def lost_message(factory: Factory, event_id: str) -> None:
    """An event whose first message was lost, already past the sweeper's grace period
    (time is moved by back-dating, decision P3). Only beat -> sweep -> publish saves it."""
    stored_id = insert_event(factory, event_id)
    with session_scope(factory) as session:
        session.execute(
            text(
                "UPDATE webhook_events SET received_at = now() - interval '5 minutes' "
                "WHERE id = :id"
            ),
            {"id": stored_id},
        )


def test_beat_keeps_sending_sweeps_after_a_broker_restart(
    session_factory: Factory,
    start_worker: StartWorker,
    rabbitmq_service: ServiceControl,
) -> None:
    """F7 finding: Celery beat keeps one cached producer; after a broker restart it must
    reconnect, or no sweep is ever sent again and unpublished events stay ``received``."""
    start_worker()
    start_worker(role="beat", sweep_interval_s=1)
    lost_message(session_factory, "d1")
    wait_for_status(session_factory, "d1", S.PROCESSED)  # beat's producer is now in use

    rabbitmq_service.stop()
    rabbitmq_service.start()

    lost_message(session_factory, "d2")
    wait_for_status(session_factory, "d2", S.PROCESSED, timeout_s=90)
