"""Stop and start real services during a test (chaos suite, F11).

By default the services are the ones of the compose file (``docker compose stop redis``),
which is what ``make up`` starts locally and what the CI chaos job starts. Any other
setup can provide its own commands through environment variables, for example
``CHAOS_REDIS_STOP`` and ``CHAOS_REDIS_START`` (run through the shell).
"""

import os
import shlex
import subprocess
import time
from collections.abc import Callable
from pathlib import Path

from redis import Redis

from hookgate.infrastructure.celery_app import create_celery

REPO_ROOT = Path(__file__).resolve().parents[2]


def wait_until(
    condition: Callable[[], bool], *, timeout_s: float, poll_s: float = 0.2, what: str
) -> None:
    """Poll ``condition`` until it is true; fail with ``what`` after ``timeout_s``."""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            if condition():
                return
        except Exception:  # a dependency still coming up counts as "not yet"
            pass
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out after {timeout_s:.0f} s waiting for {what}")
        time.sleep(poll_s)


class ServiceControl:
    """One compose service (``redis``, ``rabbitmq``) that a test can stop and start."""

    def __init__(self, name: str, *, is_up: Callable[[], bool]) -> None:
        self.name = name
        self._is_up = is_up
        prefix = f"CHAOS_{name.upper()}"
        self._stop = os.environ.get(f"{prefix}_STOP")
        self._start = os.environ.get(f"{prefix}_START")

    def is_up(self) -> bool:
        try:
            return self._is_up()
        except Exception:
            return False

    def stop(self) -> None:
        self._run(self._stop, f"docker compose stop {self.name}")
        wait_until(lambda: not self.is_up(), timeout_s=30, what=f"{self.name} to stop")

    def start(self, *, timeout_s: float = 90) -> None:
        if not self.is_up():
            self._run(self._start, f"docker compose start {self.name}")
        wait_until(self.is_up, timeout_s=timeout_s, what=f"{self.name} to accept connections")

    def _run(self, custom: str | None, default: str) -> None:
        if custom:
            result = subprocess.run(custom, shell=True, cwd=REPO_ROOT, capture_output=True)
        else:
            result = subprocess.run(shlex.split(default), cwd=REPO_ROOT, capture_output=True)
        if result.returncode != 0:
            output = (result.stdout + result.stderr).decode(errors="replace")
            raise RuntimeError(f"{custom or default!r} failed ({result.returncode}): {output}")


def redis_is_up(url: str) -> Callable[[], bool]:
    def check() -> bool:
        client = Redis.from_url(url, socket_timeout=1, socket_connect_timeout=1)
        try:
            return bool(client.ping())
        finally:
            client.close()

    return check


def flush_redis(url: str) -> None:
    """Empty the test database: dedupe markers survive restarts (RDB snapshot) and runs."""
    client = Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)
    try:
        client.flushdb()
    finally:
        client.close()


def broker_is_up(url: str) -> Callable[[], bool]:
    app = create_celery(url, sweep_interval_s=15)

    def check() -> bool:
        with app.connection_for_write() as connection:
            connection.ensure_connection(max_retries=0)
            return True

    return check
