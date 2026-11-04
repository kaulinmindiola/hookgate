"""Celery configuration and publisher (ADR-010, ADR-012, FR-ING-7). No broker needed."""

import time
from typing import Any
from uuid import uuid4

import pytest
from kombu.exceptions import OperationalError

from hookgate.infrastructure.celery_app import TASK_PROCESS_EVENT, TASK_SWEEP, create_celery
from hookgate.infrastructure.queue import CeleryPublisher


def test_delivery_settings_follow_adr_010_and_012() -> None:
    conf = create_celery("memory://", sweep_interval_s=15).conf

    assert conf.task_acks_late is True
    assert conf.task_reject_on_worker_lost is True
    assert conf.worker_prefetch_multiplier == 1
    assert conf.worker_pool == "threads"
    assert conf.task_ignore_result is True


def test_publishing_is_bounded_one_connect_attempt_and_one_reconnect() -> None:
    conf = create_celery("memory://", sweep_interval_s=15).conf

    assert conf.broker_transport_options == {"max_retries": 0}
    assert conf.broker_connection_timeout == 1.0
    # One reconnect so a producer whose broker restarted heals itself (beat, the API).
    assert conf.task_publish_retry is True
    assert conf.task_publish_retry_policy == {
        "max_retries": 1,
        "interval_start": 0,
        "interval_step": 0,
        "interval_max": 0,
    }


def test_beat_runs_the_sweeper_at_the_configured_interval() -> None:
    schedule = create_celery("memory://", sweep_interval_s=7).conf.beat_schedule

    assert schedule == {"sweep": {"task": TASK_SWEEP, "schedule": 7.0}}


class RecordingApp:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def send_task(self, name: str, **options: Any) -> None:
        self.calls.append((name, options))


def test_publisher_sends_the_event_id_and_trace_header() -> None:
    app = RecordingApp()
    event_id = uuid4()

    CeleryPublisher(app).publish(event_id, trace_id="trace-1")

    assert app.calls == [
        (
            TASK_PROCESS_EVENT,
            {"args": [str(event_id)], "headers": {"trace_id": "trace-1"}, "countdown": None},
        )
    ]


def test_publish_later_passes_the_countdown() -> None:
    app = RecordingApp()

    CeleryPublisher(app).publish_later(uuid4(), trace_id="t", countdown_s=2.5)

    assert app.calls[0][1]["countdown"] == 2.5


def test_unreachable_broker_raises_quickly() -> None:
    publisher = CeleryPublisher(create_celery("amqp://u:p@127.0.0.1:1//", sweep_interval_s=15))
    started = time.monotonic()

    with pytest.raises(OperationalError):
        publisher.publish(uuid4(), trace_id="t")

    assert time.monotonic() - started < 2.0
