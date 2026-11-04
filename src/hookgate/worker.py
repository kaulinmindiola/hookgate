"""Celery entry point for the worker and beat processes (ADR-012: thread pool).

celery -A hookgate.worker worker --pool threads --concurrency 8
celery -A hookgate.worker beat
"""

from hookgate.config import get_settings
from hookgate.wiring import build_worker_app

app = build_worker_app(get_settings())
