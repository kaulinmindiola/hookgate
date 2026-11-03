from datetime import UTC, datetime, timedelta, timezone

import pytest

from hookgate.domain.retry import RetryPolicy, backoff_ceiling, delay, next_attempt_at

DEFAULT = RetryPolicy()


class FixedRng:
    """Picks a fixed fraction of the requested interval (0.0 = lower, 1.0 = upper)."""

    def __init__(self, fraction: float) -> None:
        self.fraction = fraction

    def uniform(self, a: float, b: float) -> float:
        return a + (b - a) * self.fraction


def test_default_policy_matches_the_srs() -> None:
    assert (DEFAULT.max_attempts, DEFAULT.base_s, DEFAULT.cap_s) == (5, 2.0, 300.0)


def test_ceiling_doubles_until_it_reaches_the_cap() -> None:
    ceilings = [backoff_ceiling(n, DEFAULT) for n in range(1, 11)]
    assert ceilings == [2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 300.0, 300.0]


def test_huge_attempt_numbers_do_not_overflow() -> None:
    assert backoff_ceiling(10_000, DEFAULT) == 300.0


def test_delay_is_zero_at_the_lower_edge_and_the_ceiling_at_the_upper_edge() -> None:
    assert delay(3, FixedRng(0.0), DEFAULT) == 0.0
    assert delay(3, FixedRng(1.0), DEFAULT) == 8.0


def test_next_attempt_at_adds_the_delay_to_now() -> None:
    now = datetime(2026, 11, 2, 10, 0, 0, tzinfo=UTC)
    assert next_attempt_at(now, 1, FixedRng(1.0), DEFAULT) == now + timedelta(seconds=2)


def test_next_attempt_at_rejects_naive_now() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        next_attempt_at(datetime(2026, 11, 2), 1, FixedRng(0.5), DEFAULT)


def test_next_attempt_at_keeps_the_timezone() -> None:
    now = datetime(2026, 11, 2, 10, 0, 0, tzinfo=timezone(timedelta(hours=-5)))
    assert next_attempt_at(now, 2, FixedRng(1.0), DEFAULT).utcoffset() == timedelta(hours=-5)


def test_attempt_below_one_is_rejected() -> None:
    with pytest.raises(ValueError, match="attempt"):
        backoff_ceiling(0, DEFAULT)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"max_attempts": 0}, "max_attempts"),
        ({"base_s": 0.0}, "base_s"),
        ({"cap_s": -1.0}, "cap_s"),
    ],
)
def test_policy_validation(kwargs: dict[str, float], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]
