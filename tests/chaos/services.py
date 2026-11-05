"""Helpers for the chaos suite (F11): polling with a timeout and service checks."""

import time
from collections.abc import Callable
from pathlib import Path

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


def broker_is_up(url: str) -> Callable[[], bool]:
    app = create_celery(url, sweep_interval_s=15)

    def check() -> bool:
        with app.connection_for_write() as connection:
            connection.ensure_connection(max_retries=0)
            return True

    return check
