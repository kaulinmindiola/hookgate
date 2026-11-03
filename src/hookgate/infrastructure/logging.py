"""Structured JSON logging (FR-OBS-1, FR-SEC-2).

Every line is one JSON object on stdout. Callers bind ``trace_id`` and ``event_id``;
raw bodies and secrets are never passed to the logger.
"""

import logging
import sys

import structlog


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        # Not cached, so structlog.testing.capture_logs() also sees module-level loggers.
        cache_logger_on_first_use=False,
    )
