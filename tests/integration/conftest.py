"""Fixtures for tests that need a real PostgreSQL (start it with `make up`).

The tests use their own databases (``hookgate_test`` by default), created on demand,
so development data is never touched. Alembic is configured from ``alembic.ini``
exactly as the ``alembic`` command line does, so a broken ini file fails here too.
"""

import os
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from hookgate.infrastructure.db.engine import create_db_engine, create_session_factory

REPO_ROOT = Path(__file__).resolve().parents[2]
# 127.0.0.1, not localhost: on Windows "localhost" resolves to ::1 first, and the compose
# ports are published on IPv4 only. connect_timeout turns a hang into a clear error.
DEFAULT_TEST_DATABASE_URL = (
    "postgresql+psycopg://hookgate:hookgate@127.0.0.1:5432/hookgate_test?connect_timeout=5"
)
TABLES = "applied_effects, event_attempts, event_transitions, payments, webhook_events"


def _ensure_database(url: str) -> None:
    target = make_url(url)
    admin = create_engine(target.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as connection:
            exists = connection.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": target.database},
            ).scalar()
            if not exists:
                quoted = admin.dialect.identifier_preparer.quote(str(target.database))
                connection.execute(text(f"CREATE DATABASE {quoted}"))
    finally:
        admin.dispose()


def _alembic_config(url: str) -> Config:
    config = Config(str(REPO_ROOT / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", url)
    return config


@pytest.fixture(scope="session")
def ensure_database() -> Callable[[str], None]:
    return _ensure_database


@pytest.fixture(scope="session")
def alembic_config() -> Callable[[str], Config]:
    return _alembic_config


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)
    try:
        _ensure_database(url)
    except OperationalError as exc:
        shown = make_url(url).render_as_string(hide_password=True)
        pytest.fail(f"PostgreSQL is not reachable at {shown}. Run `make up`.\n{exc}", pytrace=False)
    return url


@pytest.fixture(scope="session")
def engine(database_url: str) -> Iterator[Engine]:
    command.upgrade(_alembic_config(database_url), "head")
    db_engine = create_db_engine(database_url, pool_size=40, max_overflow=0)
    yield db_engine
    db_engine.dispose()


@pytest.fixture(scope="session")
def session_factory(engine: Engine) -> sessionmaker[Session]:
    return create_session_factory(engine)


@pytest.fixture(autouse=True)
def clean_tables(engine: Engine) -> None:
    """Every integration test starts from empty tables."""
    with engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {TABLES} RESTART IDENTITY CASCADE"))
