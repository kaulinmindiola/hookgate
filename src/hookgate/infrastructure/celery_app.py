"""Celery application factory (ADR-002, ADR-010, ADR-012).

Delivery settings (ADR-010): ``acks_late`` + ``reject_on_worker_lost`` +
``prefetch_multiplier=1``, so a message is only acknowledged after the task ran and a
worker that dies mid-task gets its message redelivered. Duplicates are harmless: the
claim is a compare-and-set.

Pool (ADR-012): ``threads``. One process, so metrics live in one registry and SIGKILL
takes the whole worker down (VR-4).

Publishing (FR-ING-7) is bounded, never blocking:

- ``broker_transport_options={"max_retries": 0}``: opening a connection is tried once
  (kombu otherwise retries a refused connection until its timeout), so a broker that is
  down fails the publish in milliseconds, or after ``PUBLISH_CONNECT_TIMEOUT_S`` if it
  does not answer.
- ``task_publish_retry`` with ``PUBLISH_RETRY_POLICY``: one immediate reconnect when a
  connection that used to work is found dead (the broker restarted). Without it, a
  producer keeps its dead channel forever: Celery beat (one cached producer) would never
  send another sweep, and the API would lose its first publish after every restart.

A message that still cannot be published is recovered by the sweeper (ADR-007).
"""

from typing import Any

from celery import Celery

TASK_PROCESS_EVENT = "hookgate.process_event"
TASK_SWEEP = "hookgate.sweep"
PUBLISH_CONNECT_TIMEOUT_S = 1.0
# One reconnect, no sleeping between attempts (kombu Connection.ensure arguments).
PUBLISH_RETRY_POLICY: dict[str, Any] = {
    "max_retries": 1,
    "interval_start": 0,
    "interval_step": 0,
    "interval_max": 0,
}


def create_celery(broker_url: str, *, sweep_interval_s: int) -> Celery:
    app = Celery("hookgate", broker=broker_url)
    app.conf.update(
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        worker_prefetch_multiplier=1,
        worker_pool="threads",
        worker_hijack_root_logger=False,
        task_ignore_result=True,
        task_serializer="json",
        accept_content=["json"],
        task_publish_retry=True,
        task_publish_retry_policy=dict(PUBLISH_RETRY_POLICY),
        broker_connection_timeout=PUBLISH_CONNECT_TIMEOUT_S,
        broker_transport_options={"max_retries": 0},
        broker_connection_retry_on_startup=True,
        beat_schedule={
            "sweep": {"task": TASK_SWEEP, "schedule": float(sweep_interval_s)},
        },
    )
    return app
