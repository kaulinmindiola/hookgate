"""Pure precursor of VR-2: applying keys in any order ends on the latest one."""

from datetime import UTC, datetime, timedelta

from hypothesis import given
from hypothesis import strategies as st

from hookgate.domain.ordering import OrderingKey, is_stale

T0 = datetime(2026, 11, 2, 10, 0, 0, tzinfo=UTC)

keys = st.builds(
    OrderingKey,
    occurred_at=st.sampled_from([T0 + timedelta(seconds=i) for i in range(4)]),
    event_id=st.sampled_from(["evt_a", "evt_b", "evt_c"]),
)

permuted_keys = st.lists(keys, min_size=1, max_size=8, unique=True).flatmap(st.permutations)


def apply_all(ordered: list[OrderingKey]) -> OrderingKey | None:
    last: OrderingKey | None = None
    for key in ordered:
        if not is_stale(key, last):
            last = key
    return last


@given(a=keys, b=keys)
def test_exactly_one_of_two_distinct_keys_is_stale(a: OrderingKey, b: OrderingKey) -> None:
    if a == b:
        assert is_stale(a, b)
    else:
        assert is_stale(a, b) != is_stale(b, a)


@given(ordered=permuted_keys)
def test_final_key_is_the_maximum_regardless_of_arrival_order(
    ordered: list[OrderingKey],
) -> None:
    assert apply_all(ordered) == max(ordered)


@given(ordered=permuted_keys)
def test_any_permutation_ends_where_the_sorted_order_ends(ordered: list[OrderingKey]) -> None:
    assert apply_all(ordered) == apply_all(sorted(ordered))
