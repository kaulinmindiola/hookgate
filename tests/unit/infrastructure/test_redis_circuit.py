"""Redis adapter cool-down after a failure (ADR-003, FR-IDE-4, NFR-2; finding of F12).

In the compose network a stopped Redis makes every name lookup take seconds, which the
socket timeouts do not bound. After one failed call the adapter must skip Redis for a
cool-down, so an outage costs one slow call per cool-down, not one per delivery.
"""

import json
import threading
import time
from datetime import UTC, datetime
from typing import cast

import pytest
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from tests.fakes import FixedClock, InMemoryEvents, RecordingMetrics, RecordingPublisher

from hookgate.application.ingest_event import IngestCommand, IngestEvent, IngestStatus
from hookgate.domain.events import EventIdentity
from hookgate.domain.signature import build_signature_header
from hookgate.infrastructure.cache import CacheCircuitOpen, RedisDedupeCache

IDENTITY = EventIdentity("sim", "evt_1")
COOLDOWN_S = 5.0


class ManualClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class FakeRedis:
    """Counts the calls that reach "the network"; fails with a Redis error while down."""

    def __init__(self) -> None:
        self.down = False
        self.calls = 0
        self.keys: set[str] = set()
        self.block: threading.Event | None = None  # when set, ping waits on it

    def _reach(self) -> None:
        self.calls += 1
        if self.down:
            raise RedisConnectionError("Error -2 connecting to redis:6379. Name not known.")

    def exists(self, key: str) -> int:
        self._reach()
        return int(key in self.keys)

    def set(self, key: str, value: bytes, ex: int) -> bool:
        self._reach()
        self.keys.add(key)
        return True

    def ping(self) -> bool:
        if self.block is not None:
            self.block.wait(timeout=5)
        self._reach()
        return True

    def close(self) -> None:
        pass


def make_cache() -> tuple[RedisDedupeCache, FakeRedis, ManualClock]:
    fake = FakeRedis()
    clock = ManualClock()
    cache = RedisDedupeCache(cast(Redis, fake), ttl_s=60, cooldown_s=COOLDOWN_S, clock=clock)
    return cache, fake, clock


def test_a_healthy_redis_is_always_used() -> None:
    cache, fake, _ = make_cache()

    cache.remember(IDENTITY)
    assert cache.seen(IDENTITY) is True
    cache.ping()

    assert fake.calls == 3


def test_after_a_failure_redis_is_skipped_without_touching_the_network() -> None:
    cache, fake, clock = make_cache()
    fake.down = True

    with pytest.raises(RedisError):
        cache.seen(IDENTITY)
    for _ in range(50):
        clock.now += COOLDOWN_S / 100
        with pytest.raises(CacheCircuitOpen):
            cache.seen(IDENTITY)
        with pytest.raises(CacheCircuitOpen):
            cache.remember(IDENTITY)
    with pytest.raises(CacheCircuitOpen):
        cache.ping()

    assert fake.calls == 1


def test_after_the_cool_down_one_call_probes_and_success_closes_the_circuit() -> None:
    cache, fake, clock = make_cache()
    fake.down = True
    with pytest.raises(RedisError):
        cache.ping()

    fake.down = False
    clock.now += COOLDOWN_S
    cache.ping()  # the probe
    cache.remember(IDENTITY)
    assert cache.seen(IDENTITY) is True

    assert fake.calls == 4


def test_a_failed_probe_starts_a_new_cool_down() -> None:
    cache, fake, clock = make_cache()
    fake.down = True
    with pytest.raises(RedisError):
        cache.seen(IDENTITY)

    clock.now += COOLDOWN_S
    with pytest.raises(RedisError):
        cache.seen(IDENTITY)  # the probe, still down
    clock.now += COOLDOWN_S - 0.1
    with pytest.raises(CacheCircuitOpen):
        cache.seen(IDENTITY)
    clock.now += 0.1
    with pytest.raises(RedisError):
        cache.seen(IDENTITY)  # next probe

    assert fake.calls == 3


def test_while_the_probe_is_in_flight_the_other_callers_keep_skipping() -> None:
    cache, fake, clock = make_cache()
    fake.down = True
    with pytest.raises(RedisError):
        cache.seen(IDENTITY)
    fake.down = False
    clock.now += COOLDOWN_S
    fake.block = threading.Event()

    probe = threading.Thread(target=cache.ping)
    probe.start()
    time.sleep(0.05)  # the probe is now waiting inside ping
    with pytest.raises(CacheCircuitOpen):
        cache.seen(IDENTITY)
    fake.block.set()
    probe.join(timeout=5)

    assert cache.seen(IDENTITY) is False  # closed again
    assert fake.calls == 3  # failure, probe, last call


def test_errors_that_are_not_from_redis_do_not_open_the_circuit() -> None:
    cache, fake, _ = make_cache()

    def broken(key: str) -> int:
        raise TypeError("a bug, not an outage")

    fake.exists = broken  # type: ignore[method-assign]
    with pytest.raises(TypeError):
        cache.seen(IDENTITY)
    cache.remember(IDENTITY)  # still goes to Redis

    assert fake.calls == 1


def test_cool_down_must_not_be_negative() -> None:
    with pytest.raises(ValueError, match="cooldown_s"):
        RedisDedupeCache(cast(Redis, FakeRedis()), ttl_s=60, cooldown_s=-1)


def test_a_real_unreachable_redis_is_tried_once_then_skipped() -> None:
    cache = RedisDedupeCache.from_url(
        "redis://127.0.0.1:1/0", ttl_s=60, socket_timeout_s=0.05
    )  # nothing listens on port 1
    try:
        with pytest.raises(RedisError):
            cache.seen(IDENTITY)
        started = time.monotonic()
        for _ in range(100):
            with pytest.raises(CacheCircuitOpen):
                cache.seen(IDENTITY)
        assert time.monotonic() - started < 0.5
    finally:
        cache.close()


SECRET = "circuit-test-secret"
NOW = datetime(2026, 11, 10, 12, 0, 0, tzinfo=UTC)


def command(event_id: str) -> IngestCommand:
    raw = json.dumps(
        {
            "id": event_id,
            "type": "payment.created",
            "occurred_at": "2026-11-10T11:59:00Z",
            "resource": {"type": "payment", "id": f"pay_{event_id}"},
            "data": {},
        }
    ).encode()
    return IngestCommand(
        raw_body=raw,
        signature_header=build_signature_header(SECRET, int(NOW.timestamp()), raw),
        headers={},
        trace_id=f"trace-{event_id}",
    )


def test_during_an_outage_ingestion_reaches_redis_once_and_postgres_decides() -> None:
    cache, fake, _ = make_cache()
    fake.down = True
    metrics = RecordingMetrics()
    events = InMemoryEvents()
    ingest = IngestEvent(
        secret=SECRET,
        tolerance_s=300,
        clock=FixedClock(NOW),
        events=events.scope,
        publisher=RecordingPublisher(),
        cache=cache,
        metrics=metrics,
    )

    statuses = [ingest(command(e)).status for e in ["e1", "e2", "e1", "e3", "e2", "e2"]]

    assert statuses == [
        IngestStatus.ACCEPTED,
        IngestStatus.ACCEPTED,
        IngestStatus.DUPLICATE,
        IngestStatus.ACCEPTED,
        IngestStatus.DUPLICATE,
        IngestStatus.DUPLICATE,
    ]
    assert len(events.rows) == 3
    assert fake.calls == 1  # one slow call for the whole sequence, not two per delivery
    assert metrics.cache_errors == {"get": 6, "set": 6}  # every skipped call is still counted
