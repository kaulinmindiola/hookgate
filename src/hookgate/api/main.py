"""Application factory.

Run locally with: ``uv run uvicorn hookgate.api.main:create_app --factory``.
The factory takes optional collaborators so tests can inject fakes or a test database.
"""

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from sqlalchemy import Engine

from hookgate.api.deps import TRACE_HEADER, resolve_trace_id
from hookgate.api.errors import register_error_handlers
from hookgate.api.routers import health, webhooks
from hookgate.application.ingest_event import IngestEvent
from hookgate.application.ports import Clock, EventPublisher, EventStoreScope
from hookgate.config import Settings, get_settings
from hookgate.infrastructure.clock import SystemClock
from hookgate.infrastructure.db.engine import create_db_engine, create_session_factory
from hookgate.infrastructure.db.transactions import event_store_scope
from hookgate.infrastructure.logging import configure_logging
from hookgate.infrastructure.queue import NullPublisher


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
    clock: Clock | None = None,
    publisher: EventPublisher | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    configure_logging()

    owned_engine: Engine | None = None
    if events is None:
        if engine is None:
            engine = owned_engine = create_db_engine(settings.database_url)
        events = event_store_scope(create_session_factory(engine))

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        if owned_engine is not None:
            owned_engine.dispose()

    app = FastAPI(title="Hookgate", version="0.1.0", lifespan=lifespan)
    app.state.ingest_event = IngestEvent(
        secret=settings.webhook_secret.get_secret_value(),
        tolerance_s=settings.timestamp_tolerance_s,
        clock=clock or SystemClock(),
        events=events,
        publisher=publisher or NullPublisher(),
    )
    app.middleware("http")(_trace_id_middleware)
    register_error_handlers(app)
    app.include_router(health.router)
    app.include_router(webhooks.router)
    return app
