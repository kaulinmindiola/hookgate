"""Builds use cases and the worker from settings with their production adapters.

The API factory (``hookgate.api.main``) wires ingestion; this module wires processing,
the sweeper and the Celery worker, so tasks and manual runs share one construction path.
"""

import random

from celery import Celery
from sqlalchemy import Engine

from hookgate.application.ports import EventPublisher
from hookgate.application.process_event import EffectHandler, ProcessEvent
from hookgate.application.sweep import SweepEvents
from hookgate.config import Settings
from hookgate.domain.payments import project_event
from hookgate.domain.retry import RetryPolicy, SupportsUniform
from hookgate.infrastructure.celery_app import create_celery
from hookgate.infrastructure.db.engine import create_db_engine, create_session_factory
from hookgate.infrastructure.db.transactions import processing_scope, sweep_scope
from hookgate.infrastructure.logging import configure_logging
from hookgate.infrastructure.queue import CeleryPublisher
from hookgate.infrastructure.tasks import register_tasks


def retry_policy(settings: Settings) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=settings.max_attempts,
        base_s=settings.retry_base_s,
        cap_s=settings.retry_cap_s,
    )


def build_process_event(
    settings: Settings,
    engine: Engine,
    *,
    handler: EffectHandler = project_event,
    rng: SupportsUniform | None = None,
) -> ProcessEvent:
    return ProcessEvent(
        units=processing_scope(create_session_factory(engine)),
        lease_s=settings.lease_s,
        policy=retry_policy(settings),
        rng=rng or random.Random(),
        handler=handler,
    )


def build_sweep(
    settings: Settings,
    engine: Engine,
    publisher: EventPublisher,
    *,
    rng: SupportsUniform | None = None,
) -> SweepEvents:
    return SweepEvents(
        units=sweep_scope(create_session_factory(engine)),
        publisher=publisher,
        policy=retry_policy(settings),
        rng=rng or random.Random(),
    )


def build_celery_app(
    settings: Settings,
    engine: Engine,
    *,
    broker_url: str | None = None,
    handler: EffectHandler = project_event,
) -> Celery:
    """The Celery app with its tasks registered (used by the worker, beat and tests)."""
    app = create_celery(
        broker_url or settings.broker_url, sweep_interval_s=settings.sweep_interval_s
    )
    publisher = CeleryPublisher(app)
    register_tasks(
        app,
        process_event=build_process_event(settings, engine, handler=handler),
        sweep=build_sweep(settings, engine, publisher),
        publisher=publisher,
    )
    return app


def build_worker_app(settings: Settings) -> Celery:
    configure_logging()
    return build_celery_app(settings, create_db_engine(settings.database_url))
