"""Redis adapter for the ``DedupeCache`` port (ADR-003, FR-IDE-3/4, SDD 11).

Built to fail fast: 50 ms socket timeouts and **no client retries**. redis-py retries
connection and timeout errors by default with exponential backoff, which during an
outage would add seconds to every delivery. Here an outage costs one failed call, and
the use case falls back to PostgreSQL.
"""

from redis import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from hookgate.domain.events import EventIdentity

KEY_PREFIX = "hookgate:dedupe"


def dedupe_key(identity: EventIdentity) -> str:
    return f"{KEY_PREFIX}:{identity.provider}:{identity.event_id}"


class RedisDedupeCache:
    def __init__(self, client: Redis, *, ttl_s: int) -> None:
        if ttl_s < 1:
            raise ValueError("ttl_s must be >= 1")
        self._client = client
        self._ttl_s = ttl_s

    @classmethod
    def from_url(cls, url: str, *, ttl_s: int, socket_timeout_s: float) -> "RedisDedupeCache":
        client = Redis.from_url(
            url,
            socket_timeout=socket_timeout_s,
            socket_connect_timeout=socket_timeout_s,
            retry=Retry(NoBackoff(), 0),
            health_check_interval=0,
        )
        return cls(client, ttl_s=ttl_s)

    def seen(self, identity: EventIdentity) -> bool:
        return bool(self._client.exists(dedupe_key(identity)))

    def remember(self, identity: EventIdentity) -> None:
        self._client.set(dedupe_key(identity), b"1", ex=self._ttl_s)

    def ping(self) -> None:
        """Raise unless Redis answers (readiness probe)."""
        self._client.ping()

    def close(self) -> None:
        self._client.close()
