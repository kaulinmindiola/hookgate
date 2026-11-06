"""A real Celery worker process for the chaos suite (run as a subprocess, never imported).

It is the production worker (``build_celery_app``: tasks, delivery settings, thread
pool) with three test-only differences, all set through environment variables:

- ``CHAOS_DATABASE_URL`` / ``CHAOS_BROKER_URL``: the test database and broker;
- ``CHAOS_QUEUE``: a queue of its own, so a running compose worker never takes its
  messages;
- ``CHAOS_BLOCK_PREFIX`` (optional): the handler hangs on events whose id starts with
  it, after writing ``<CHAOS_SIGNAL_DIR>/<event_id>.claimed``. The test then knows the
  worker holds a claimed event mid-task and can kill it (VR-4).

``<CHAOS_SIGNAL_DIR>/ready`` is written when the worker is consuming.

With ``CHAOS_ROLE=beat`` the same app runs Celery beat instead, sending the sweep task
every ``CHAOS_SWEEP_INTERVAL_S`` seconds (``ready`` is written when beat starts).
"""

import os
import time
from pathlib import Path
from typing import Any

from celery.signals import beat_init, worker_ready
from pydantic import SecretStr

from hookgate.config import Settings
from hookgate.domain.events import Event
from hookgate.domain.payments import Payment, project_event
from hookgate.infrastructure.db.engine import create_db_engine
from hookgate.infrastructure.logging import configure_logging
from hookgate.wiring import build_celery_app

SIGNAL_DIR = Path(os.environ["CHAOS_SIGNAL_DIR"])
BLOCK_PREFIX = os.environ.get("CHAOS_BLOCK_PREFIX", "")
QUEUE = os.environ["CHAOS_QUEUE"]


def handler(event: Event) -> Payment:
    event_id = event.identity.event_id
    if BLOCK_PREFIX and event_id.startswith(BLOCK_PREFIX):
        (SIGNAL_DIR / f"{event_id}.claimed").write_text("claimed")
        time.sleep(3600)  # hung mid-task, holding the claim, until the test kills us
    return project_event(event)


def signal_ready(**_: Any) -> None:
    (SIGNAL_DIR / "ready").write_text("ready")


def main() -> None:
    configure_logging()
    settings = Settings(
        database_url=os.environ["CHAOS_DATABASE_URL"],
        redis_url="redis://unused",
        broker_url=os.environ["CHAOS_BROKER_URL"],
        webhook_secret=SecretStr("unused-secret"),
        admin_api_key=SecretStr("unused-admin-key"),
        lease_s=int(os.environ.get("CHAOS_LEASE_S", "2")),
        sweep_interval_s=int(os.environ.get("CHAOS_SWEEP_INTERVAL_S", "15")),
        retry_base_s=0.2,
        retry_cap_s=0.5,
    )
    app = build_celery_app(settings, create_db_engine(settings.database_url), handler=handler)
    app.conf.task_default_queue = QUEUE
    if os.environ.get("CHAOS_ROLE") == "beat":
        beat_init.connect(signal_ready, weak=False)
        schedule = SIGNAL_DIR / "celerybeat-schedule"
        app.start(["beat", "--schedule", str(schedule), "--loglevel", "INFO"])
        return
    worker_ready.connect(signal_ready, weak=False)
    app.worker_main(
        [
            "worker",
            "--pool",
            "threads",
            "--concurrency",
            "2",
            "--queues",
            QUEUE,
            "--loglevel",
            "INFO",
            "--without-gossip",
            "--without-mingle",
            "--without-heartbeat",
        ]
    )


if __name__ == "__main__":
    main()
