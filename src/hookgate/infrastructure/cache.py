"""Redis adapter for the ``DedupeCache`` port (ADR-003, FR-IDE-3/4, SDD 11).

Built to fail fast: 50 ms socket timeouts and **no client retries**. redis-py retries
connection and timeout errors by default with exponential backoff, which during an
outage would add seconds to every delivery. Here an outage costs one failed call, and
the use case falls back to PostgreSQL.

The socket timeouts do not bound name resolution: in the compose network, a stopped
``redis`` container makes every ``getaddrinfo("redis")`` take seconds (finding of F12,
measured at about 3.5 s per call). So after a failed call the adapter **skips Redis for
a cool-down** (5 s by default) and raises ``CacheCircuitOpen`` at once, without touching
the network; the use case handles it like any cache error (counted, logged, PostgreSQL
decides). When the cool-down is over, one call probes Redis while the others keep
skipping it: success closes the circuit, failure starts a new cool-down. An outage then
costs one slow call per cool-down instead of one per delivery.
"""

import threading
import time
from collections.abc import Callable

from redis import Redis
from redis.backoff import NoBackoff
from redis.exceptions import RedisError
from redis.retry import Retry

from hookgate.domain.events import EventIdentity

KEY_PREFIX = "hookgate:dedupe"
DEFAULT_COOLDOWN_S = 5.0


class CacheCircuitOpen(Exception):
    """Redis is being skipped after a recent failure; nothing was sent to it."""


def dedupe_key(identity: EventIdentity) -> str:
    return f"{KEY_PREFIX}:{identity.provider}:{identity.event_id}"


class RedisDedupeCache:
    def __init__(
        self,
        client: Redis,
        *,
        ttl_s: int,
        cooldown_s: float = DEFAULT_COOLDOWN_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_s < 1:
            raise ValueError("ttl_s must be >= 1")
        if cooldown_s < 0:
            raise ValueError("cooldown_s must be >= 0")
        self._client = client
        self._ttl_s = ttl_s
        self._cooldown_s = cooldown_s
        self._clock = clock
        self._lock = threading.Lock()
        self._skip_until: float | None = None  # None: circuit closed, Redis is used

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        ttl_s: int,
        socket_timeout_s: float,
        cooldown_s: float = DEFAULT_COOLDOWN_S,
    ) -> "RedisDedupeCache":
        client = Redis.from_url(
            url,
            socket_timeout=socket_timeout_s,
            socket_connect_timeout=socket_timeout_s,
            retry=Retry(NoBackoff(), 0),
            health_check_interval=0,
        )
        return cls(client, ttl_s=ttl_s, cooldown_s=cooldown_s)

    def seen(self, identity: EventIdentity) -> bool:
        return bool(self._call(lambda: self._client.exists(dedupe_key(identity))))

    def remember(self, identity: EventIdentity) -> None:
        self._call(lambda: self._client.set(dedupe_key(identity), b"1", ex=self._ttl_s))

    def ping(self) -> None:
        """Raise unless Redis answers (readiness probe); also skipped during a cool-down."""
        self._call(self._client.ping)

    def close(self) -> None:
        self._client.close()

    def _call[T](self, operation: Callable[[], T]) -> T:
        self._admit()
        try:
            result = operation()
        except RedisError:
            with self._lock:
                self._skip_until = self._clock() + self._cooldown_s
            raise
        with self._lock:
            self._skip_until = None
        return result

    def _admit(self) -> None:
        with self._lock:
            if self._skip_until is None:
                return
            now = self._clock()
            if now < self._skip_until:
                raise CacheCircuitOpen(f"redis skipped for {self._skip_until - now:.1f} s more")
            # Cool-down over: this call probes Redis; the others keep skipping until it ends.
            self._skip_until = now + self._cooldown_s
