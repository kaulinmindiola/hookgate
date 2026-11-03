"""Redis adapter against a real Redis (ADR-003, FR-IDE-3/4)."""

import time

import pytest
from redis import Redis
from redis.exceptions import RedisError

from hookgate.domain.events import EventIdentity
from hookgate.infrastructure.cache import RedisDedupeCache, dedupe_key

pytestmark = pytest.mark.integration

IDENTITY = EventIdentity("sim", "evt_1")
UNREACHABLE_URL = "redis://127.0.0.1:1/0"  # nothing listens on port 1


def test_marker_is_seen_after_remember(dedupe_cache: RedisDedupeCache) -> None:
    assert dedupe_cache.seen(IDENTITY) is False

    dedupe_cache.remember(IDENTITY)

    assert dedupe_cache.seen(IDENTITY) is True
    assert dedupe_cache.seen(EventIdentity("other", "evt_1")) is False


def test_marker_has_the_expected_key_value_and_ttl(
    dedupe_cache: RedisDedupeCache, redis_url: str
) -> None:
    dedupe_cache.remember(IDENTITY)

    client = Redis.from_url(redis_url)
    try:
        assert dedupe_key(IDENTITY) == "hookgate:dedupe:sim:evt_1"
        assert client.get(dedupe_key(IDENTITY)) == b"1"
        assert 0 < int(client.ttl(dedupe_key(IDENTITY))) <= 60
    finally:
        client.close()


def test_ttl_must_be_positive() -> None:
    with pytest.raises(ValueError, match="ttl_s"):
        RedisDedupeCache(Redis(), ttl_s=0)


@pytest.mark.parametrize("operation", ["seen", "remember"])
def test_unreachable_redis_fails_fast_without_retries(operation: str) -> None:
    cache = RedisDedupeCache.from_url(UNREACHABLE_URL, ttl_s=60, socket_timeout_s=0.05)
    started = time.monotonic()

    with pytest.raises(RedisError):
        getattr(cache, operation)(IDENTITY)

    # With redis-py's default retries this would take seconds; one attempt is ~50 ms.
    assert time.monotonic() - started < 1.0
    cache.close()
