"""Outcome of a processed event (``webhook_events.outcome``)."""

from enum import StrEnum


class EventOutcome(StrEnum):
    APPLIED = "applied"
    IGNORED_STALE = "ignored_stale"
