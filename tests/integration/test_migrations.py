"""The migration is reversible and the ORM models describe the migrated schema."""

from collections.abc import Callable, Iterator

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import Engine, create_engine, inspect, text
from sqlalchemy.engine import make_url

from hookgate.infrastructure.db.models import Base

pytestmark = pytest.mark.integration

EXPECTED_TABLES = {
    "applied_effects",
    "event_attempts",
    "event_transitions",
    "payments",
    "webhook_events",
}
EXPECTED_ENUMS = {"event_outcome", "event_status"}


@pytest.fixture
def migration_db(
    database_url: str,
    ensure_database: Callable[[str], None],
    alembic_config: Callable[[str], Config],
) -> Iterator[tuple[Engine, Config]]:
    url = (
        make_url(database_url)
        .set(database="hookgate_test_migrations")
        .render_as_string(hide_password=False)
    )
    ensure_database(url)
    db_engine = create_engine(url)
    with db_engine.begin() as connection:
        connection.execute(text("DROP SCHEMA public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    yield db_engine, alembic_config(url)
    db_engine.dispose()


def user_tables(db_engine: Engine) -> set[str]:
    return set(inspect(db_engine).get_table_names()) - {"alembic_version"}


def enum_types(db_engine: Engine) -> set[str]:
    with db_engine.connect() as connection:
        rows = connection.execute(text("SELECT typname FROM pg_type WHERE typtype = 'e'"))
        return {row[0] for row in rows}


def test_upgrade_creates_the_schema_and_downgrade_removes_it(
    migration_db: tuple[Engine, Config],
) -> None:
    db_engine, config = migration_db

    command.upgrade(config, "head")
    assert user_tables(db_engine) == EXPECTED_TABLES
    assert enum_types(db_engine) == EXPECTED_ENUMS

    command.downgrade(config, "base")
    assert user_tables(db_engine) == set()
    assert enum_types(db_engine) == set()

    command.upgrade(config, "head")
    assert user_tables(db_engine) == EXPECTED_TABLES


def test_orm_models_match_the_migrated_schema(migration_db: tuple[Engine, Config]) -> None:
    db_engine, config = migration_db
    command.upgrade(config, "head")

    with db_engine.connect() as connection:
        context = MigrationContext.configure(connection, opts={"compare_type": True})
        differences = compare_metadata(context, Base.metadata)

    assert differences == []
