"""Retry policy with exponential backoff and full jitter (FR-PRO-4, SDD section 12).

delay(n) = rng.uniform(0, min(cap, base * 2 ** (n - 1)))
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from hookgate.domain.timeutil import require_aware


class SupportsUniform(Protocol):
    """The slice of ``random.Random`` that the policy needs (injectable for tests)."""

    def uniform(self, a: float, b: float) -> float: ...


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 5
    base_s: float = 2.0
    cap_s: float = 300.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_s <= 0:
            raise ValueError("base_s must be > 0")
        if self.cap_s <= 0:
            raise ValueError("cap_s must be > 0")


def backoff_ceiling(attempt: int, policy: RetryPolicy) -> float:
    """Upper bound of the jitter window for the given attempt (1-based)."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    ceiling = policy.base_s
    for _ in range(attempt - 1):
        if ceiling >= policy.cap_s:
            break
        ceiling *= 2
    return min(ceiling, policy.cap_s)


def delay(attempt: int, rng: SupportsUniform, policy: RetryPolicy) -> float:
    """Seconds to wait before the next attempt, drawn with full jitter."""
    return rng.uniform(0.0, backoff_ceiling(attempt, policy))


def next_attempt_at(
    now: datetime, attempt: int, rng: SupportsUniform, policy: RetryPolicy
) -> datetime:
    """Absolute time of the next attempt (stored in ``next_attempt_at``)."""
    require_aware(now, "now")
    return now + timedelta(seconds=delay(attempt, rng, policy))
