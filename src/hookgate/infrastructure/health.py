"""Readiness probes for PostgreSQL, Redis and the broker (FR-OBS-2).

Each probe raises when its dependency does not answer within its own short timeout:
the database through the engine's ``connect_timeout``, Redis through the 50 ms socket
timeout of the dedupe client, the broker through ``broker_connection_timeout`` with a
single connection attempt.
"""

from celery import Celery
from sqlalchemy import Engine, text

from hookgate.application.health import HealthCheck
from hookgate.infrastructure.cache import RedisDedupeCache


def database_check(engine: Engine) -> HealthCheck:
    def check() -> None:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))

    return check


def redis_check(cache: RedisDedupeCache) -> HealthCheck:
    def check() -> None:
        cache.ping()

    return check


def broker_check(app: Celery) -> HealthCheck:
    def check() -> None:
        with app.connection_for_write() as connection:
            connection.ensure_connection(max_retries=0)

    return check
