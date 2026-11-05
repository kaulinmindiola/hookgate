"""Fixtures for the chaos suite: real PostgreSQL, RabbitMQ and worker processes.

Start the services with ``make up``. The database fixtures are
the integration ones, so every test starts from empty tables.
"""

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from celery import Celery
from tests.chaos.services import REPO_ROOT, broker_is_up
from tests.chaos.workers import StartWorker, WorkerProcess
from tests.integration import conftest as integration

from hookgate.infrastructure.celery_app import create_celery
from hookgate.infrastructure.queue import CeleryPublisher

DEFAULT_TEST_BROKER_URL = "amqp://hookgate:hookgate@127.0.0.1:5672//"
QUEUE = "hookgate.chaos"

# The integration fixtures, reused here: real database, emptied before each test.
clean_tables = integration.clean_tables
database_url = integration.database_url
engine = integration.engine
session_factory = integration.session_factory


@pytest.fixture(scope="session")
def broker_url() -> str:
    url = os.environ.get("TEST_BROKER_URL", DEFAULT_TEST_BROKER_URL)
    if not broker_is_up(url)():
        pytest.fail(f"RabbitMQ is not reachable at {url}. Run `make up`.", pytrace=False)
    return url


@pytest.fixture
def chaos_app(broker_url: str) -> Celery:
    """The production Celery configuration, routed to the chaos queue (emptied first)."""
    app = create_celery(broker_url, sweep_interval_s=15)
    app.conf.task_default_queue = QUEUE
    # A connection of its own: Celery's pool is shared by every app in the process and
    # may still hold a connection to a broker that a previous test restarted.
    with app.connection_for_write() as connection:
        app.control.purge(connection=connection)  # leftovers would only add noise
    return app


@pytest.fixture
def publisher(chaos_app: Celery) -> CeleryPublisher:
    return CeleryPublisher(chaos_app)


@pytest.fixture
def start_worker(
    tmp_path: Path, database_url: str, broker_url: str, chaos_app: Celery
) -> Iterator[StartWorker]:
    """Start a worker process; all of them are stopped at the end of the test."""
    workers: list[WorkerProcess] = []

    def start(*, block_prefix: str = "", lease_s: int = 2) -> WorkerProcess:
        n = len(workers) + 1
        signal_dir = tmp_path / f"worker{n}"
        signal_dir.mkdir()
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join([str(REPO_ROOT / "src"), str(REPO_ROOT)]),
            "CHAOS_DATABASE_URL": database_url,
            "CHAOS_BROKER_URL": broker_url,
            "CHAOS_QUEUE": QUEUE,
            "CHAOS_SIGNAL_DIR": str(signal_dir),
            "CHAOS_BLOCK_PREFIX": block_prefix,
            "CHAOS_LEASE_S": str(lease_s),
        }
        worker = WorkerProcess(signal_dir, tmp_path / f"worker{n}.log", env)
        workers.append(worker)
        return worker

    yield start
    for n, worker in enumerate(workers, start=1):
        worker.stop()
        # Captured by pytest and shown only when the test fails.
        print(f"--- worker{n} log ---\n{worker.log()}")
