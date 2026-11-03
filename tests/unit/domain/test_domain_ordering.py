from datetime import UTC, datetime, timedelta, timezone

import pytest

from hookgate.domain.ordering import OrderingKey, is_stale

T0 = datetime(2026, 11, 2, 10, 0, 0, tzinfo=UTC)


def key(seconds: int, event_id: str) -> OrderingKey:
    return OrderingKey(T0 + timedelta(seconds=seconds), event_id)


def test_no_previous_event_is_never_stale() -> None:
    assert is_stale(key(0, "evt_1"), None) is False


def test_newer_occurred_at_is_not_stale() -> None:
    assert is_stale(key(10, "evt_1"), key(0, "evt_9")) is False


def test_older_occurred_at_is_stale() -> None:
    assert is_stale(key(0, "evt_9"), key(10, "evt_1")) is True


def test_tie_is_decided_by_event_id() -> None:
    assert is_stale(key(5, "evt_b"), key(5, "evt_a")) is False
    assert is_stale(key(5, "evt_a"), key(5, "evt_b")) is True


def test_same_event_redelivered_is_stale() -> None:
    assert is_stale(key(5, "evt_a"), key(5, "evt_a")) is True


def test_equal_instants_in_different_offsets_compare_equal() -> None:
    plus_two = timezone(timedelta(hours=2))
    a = OrderingKey(datetime(2026, 11, 2, 12, 0, 0, tzinfo=plus_two), "evt_a")
    b = OrderingKey(datetime(2026, 11, 2, 10, 0, 0, tzinfo=UTC), "evt_a")
    assert a == b
    assert is_stale(a, b) is True


def test_naive_datetime_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        OrderingKey(datetime(2026, 11, 2, 10, 0, 0), "evt_a")
