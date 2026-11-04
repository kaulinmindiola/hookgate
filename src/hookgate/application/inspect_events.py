"""Read side of the admin API: list and inspect events (FR-DLQ-1, FR-DLQ-2).

Each call runs in one short read transaction. Pages use keyset pagination on
``(received_at, id)``; one extra row is read to know whether another page exists.
"""

from dataclasses import dataclass
from uuid import UUID

from hookgate.application.ports import AdminScope
from hookgate.application.records import (
    AttemptRecord,
    EventQuery,
    PageKey,
    StoredEvent,
    TransitionRecord,
)

MAX_PAGE_SIZE = 200


@dataclass(frozen=True, slots=True)
class EventPage:
    items: list[StoredEvent]
    next_key: PageKey | None  # None: this is the last page


@dataclass(frozen=True, slots=True)
class EventDetail:
    event: StoredEvent
    transitions: list[TransitionRecord]
    attempts: list[AttemptRecord]


class InspectEvents:
    def __init__(self, *, units: AdminScope) -> None:
        self._units = units

    def page(self, query: EventQuery, *, limit: int, after: PageKey | None = None) -> EventPage:
        if not 1 <= limit <= MAX_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")
        with self._units() as events:
            rows = events.list_events(query, limit=limit + 1, after=after)
        items = rows[:limit]
        next_key = None
        if len(rows) > limit:
            last = items[-1]
            next_key = PageKey(received_at=last.received_at, id=last.id)
        return EventPage(items=items, next_key=next_key)

    def detail(self, event_id: UUID) -> EventDetail | None:
        with self._units() as events:
            stored = events.get(event_id)
            if stored is None:
                return None
            return EventDetail(
                event=stored,
                transitions=events.list_transitions(event_id),
                attempts=events.list_attempts(event_id),
            )
