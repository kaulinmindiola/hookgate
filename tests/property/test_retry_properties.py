"""VR-11: the delay is always within [0, min(cap, base * 2 ** (n - 1))]."""

import random

from hypothesis import given
from hypothesis import strategies as st

from hookgate.domain.retry import RetryPolicy, backoff_ceiling, delay

policies = st.builds(
    RetryPolicy,
    max_attempts=st.integers(min_value=1, max_value=10),
    base_s=st.floats(min_value=0.001, max_value=60.0),
    cap_s=st.floats(min_value=0.001, max_value=10_000.0),
)


@given(attempt=st.integers(min_value=1, max_value=60), policy=policies, seed=st.integers())
def test_delay_is_within_the_jitter_window(attempt: int, policy: RetryPolicy, seed: int) -> None:
    value = delay(attempt, random.Random(seed), policy)
    assert 0.0 <= value <= min(policy.cap_s, policy.base_s * 2 ** (attempt - 1))


@given(attempt=st.integers(min_value=1, max_value=60), policy=policies)
def test_ceiling_matches_the_closed_form(attempt: int, policy: RetryPolicy) -> None:
    expected = min(policy.cap_s, policy.base_s * 2 ** (attempt - 1))
    assert backoff_ceiling(attempt, policy) == expected


@given(attempt=st.integers(min_value=1, max_value=59), policy=policies)
def test_ceiling_never_decreases_and_never_exceeds_the_cap(
    attempt: int, policy: RetryPolicy
) -> None:
    assert backoff_ceiling(attempt, policy) <= backoff_ceiling(attempt + 1, policy)
    assert backoff_ceiling(attempt + 1, policy) <= policy.cap_s
