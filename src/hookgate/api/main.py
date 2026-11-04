"""Application factory.

Run locally with: ``uv run uvicorn hookgate.api.main:create_app --factory``.
The factory takes optional collaborators so tests can inject fakes or a test database.
By default it connects to PostgreSQL, Redis and the broker from the settings; the
Prometheus metrics adapter arrives in phase F9 (until then cache errors are logged only).
The engine is lazy: it opens no connection until a request uses the database.
"""

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from sqlalchemy import Engine

from hookgate.api.deps import TRACE_HEADER, resolve_trace_id
from hookgate.api.errors import register_error_handlers
from hookgate.api.routers import admin, health, webhooks
from hookgate.application.ingest_event import IngestEvent
from hookgate.application.inspect_events import InspectEvents
from hookgate.application.ports import (
    AdminScope,
    Clock,
    DedupeCache,
    EventPublisher,
    EventStoreScope,
    IngestionMetrics,
)
from hookgate.application.replay_event import ReplayEvent
from hookgate.config import Settings, get_settings
from hookgate.infrastructure.cache import RedisDedupeCache
from hookgate.infrastructure.celery_app import create_celery
from hookgate.infrastructure.clock import SystemClock
from hookgate.infrastructure.db.engine import create_db_engine, create_session_factory
from hookgate.infrastructure.db.transactions import admin_scope, event_store_scope
from hookgate.infrastructure.logging import configure_logging
from hookgate.infrastructure.queue import CeleryPublisher


async def _trace_id_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    trace_id = resolve_trace_id(request.headers.get(TRACE_HEADER))
    request.state.trace_id = trace_id
    response = await call_next(request)
    response.headers[TRACE_HEADER] = trace_id
    return response


def create_app(
    settings: Settings | None = None,
    *,
    engine: Engine | None = None,
    events: EventStoreScope | None = None,
    admin_events: AdminScope | None = None,
    clock: Clock | None = None,
    publisher: EventPublisher | None = None,
    cache: DedupeCache | None = None,
    metrics: IngestionMetrics | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    configure_logging()

    owned_engine: Engine | None = None
    if events is None or admin_events is None:
        if engine is None:
            engine = owned_engine = create_db_engine(settings.database_url)
        factory = create_session_factory(engine)
        events = events or event_store_scope(factory)
        admin_events = admin_events or admin_scope(factory)

    owned_cache: RedisDedupeCache | None = None
    if cache is None:
        cache = owned_cache = RedisDedupeCache.from_url(
            settings.redis_url,
            ttl_s=settings.dedupe_ttl_s,
            socket_timeout_s=settings.redis_socket_timeout_s,
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        if owned_cache is not None:
            owned_cache.close()
        if owned_engine is not None:
            owned_engine.dispose()

    publisher = publisher or CeleryPublisher(
        create_celery(settings.broker_url, sweep_interval_s=settings.sweep_interval_s)
    )

    app = FastAPI(title="Hookgate", version="0.1.0", lifespan=lifespan)
    app.state.admin_api_key = settings.admin_api_key.get_secret_value().encode("utf-8")
    app.state.ingest_event = IngestEvent(
        secret=settings.webhook_secret.get_secret_value(),
        tolerance_s=settings.timestamp_tolerance_s,
        clock=clock or SystemClock(),
        events=events,
        publisher=publisher,
        cache=cache,
        metrics=metrics,
    )
    app.state.inspect_events = InspectEvents(units=admin_events)
    app.state.replay_event = ReplayEvent(units=admin_events, publisher=publisher)
    app.middleware("http")(_trace_id_middleware)
    register_error_handlers(app)
    app.include_router(health.router)
    app.include_router(webhooks.router)
    app.include_router(admin.router)
    return app
