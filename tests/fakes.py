"""In-memory fakes for the application ports (no database, no broker, no Redis)."""

from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from threading import Lock
from uuid import UUID, uuid4

from hookgate.application.ports import CacheOp, EventStore
from hookgate.application.records import NewEvent
from hookgate.domain.events import EventIdentity


class FixedClock:
    def __init__(self, now: datetime) -> None:
        self.current = now

    def now(self) -> datetime:
        return self.current


class InMemoryEvents:
    """Fake ``EventStore`` plus its scope. ``journal`` records commits for ordering checks."""

    def __init__(self, journal: list[str] | None = None) -> None:
        self.rows: dict[EventIdentity, tuple[UUID, NewEvent]] = {}
        self.journal = journal if journal is not None else []
        self.inserts = 0
        self._lock = Lock()

    def insert_if_new(self, new: NewEvent) -> UUID | None:
        with self._lock:
            self.inserts += 1
            if new.identity in self.rows:
                return None
            stored_id = uuid4()
            self.rows[new.identity] = (stored_id, new)
            return stored_id

    def find_raw_body(self, identity: EventIdentity) -> bytes | None:
        row = self.rows.get(identity)
        return None if row is None else row[1].raw_body

    def stored(self, event_id: str, provider: str = "sim") -> NewEvent:
        return self.rows[EventIdentity(provider, event_id)][1]

    @contextmanager
    def scope(self) -> Iterator[EventStore]:
        yield self
        self.journal.append("commit")


class RecordingPublisher:
    def __init__(self, journal: list[str] | None = None, *, fail: bool = False) -> None:
        self.published: list[tuple[UUID, str]] = []
        self.journal = journal if journal is not None else []
        self.fail = fail

    def publish(self, event_id: UUID, *, trace_id: str) -> None:
        self.journal.append("publish")
        if self.fail:
            raise ConnectionError("broker unreachable")
        self.published.append((event_id, trace_id))


class InMemoryCache:
    """Fake ``DedupeCache`` that never fails."""

    def __init__(self, journal: list[str] | None = None) -> None:
        self.markers: set[EventIdentity] = set()
        self.journal = journal if journal is not None else []

    def seen(self, identity: EventIdentity) -> bool:
        return identity in self.markers

    def remember(self, identity: EventIdentity) -> None:
        self.journal.append("remember")
        self.markers.add(identity)


class FaultyCache(InMemoryCache):
    """Works for the first ``healthy_calls`` calls, then every listed op raises.

    Models Redis going down in the middle of a delivery sequence (VR-3).
    """

    def __init__(
        self, healthy_calls: int = 0, *, failing_ops: frozenset[CacheOp] = frozenset({"get", "set"})
    ) -> None:
        super().__init__()
        self.healthy_calls = healthy_calls
        self.failing_ops = failing_ops
        self.calls = 0

    def _maybe_fail(self, op: CacheOp) -> None:
        self.calls += 1
        if self.calls > self.healthy_calls and op in self.failing_ops:
            raise TimeoutError(f"redis {op} timed out")

    def seen(self, identity: EventIdentity) -> bool:
        self._maybe_fail("get")
        return super().seen(identity)

    def remember(self, identity: EventIdentity) -> None:
        self._maybe_fail("set")
        super().remember(identity)


class RecordingMetrics:
    def __init__(self) -> None:
        self.cache_errors: Counter[CacheOp] = Counter()

    def cache_error(self, op: CacheOp) -> None:
        self.cache_errors[op] += 1
