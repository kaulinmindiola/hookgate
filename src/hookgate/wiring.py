"""Builds use cases from settings with their production adapters.

The API factory (``hookgate.api.main``) wires ingestion; this module wires processing,
so the Celery task (phase F7) and manual runs share one construction path.
"""

import random

from sqlalchemy import Engine

from hookgate.application.process_event import EffectHandler, ProcessEvent
from hookgate.config import Settings
from hookgate.domain.payments import project_event
from hookgate.domain.retry import RetryPolicy, SupportsUniform
from hookgate.infrastructure.db.engine import create_session_factory
from hookgate.infrastructure.db.transactions import processing_scope


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
