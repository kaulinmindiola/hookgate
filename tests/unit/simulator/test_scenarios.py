"""Seeded, deterministic plans (FR-SIM-1, FR-SIM-2)."""

from itertools import pairwise

import pytest

from simulator.model import POISON_TYPE, Delivery, Pause, Signing
from simulator.scenarios import BASIC, BURST_SIZE, SCENARIOS, build_plan


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_same_seed_and_run_id_give_the_same_plan(scenario: str) -> None:
    assert build_plan(scenario, 7, "r1") == build_plan(scenario, 7, "r1")


@pytest.mark.parametrize("scenario", ["happy", "duplicates", "out_of_order", "chaos"])
def test_another_seed_gives_another_plan(scenario: str) -> None:
    assert build_plan(scenario, 7, "r1").steps != build_plan(scenario, 8, "r1").steps


def test_run_id_only_renames_the_events() -> None:
    first, second = build_plan("chaos", 7, "aaa"), build_plan("chaos", 7, "bbb")

    assert len(first.steps) == len(second.steps)
    for a, b in zip(first.deliveries, second.deliveries, strict=True):
        assert a.event.event_id.replace("aaa", "bbb") == b.event.event_id
        assert (a.copies, a.signing, a.event.occurred_at) == (
            b.copies,
            b.signing,
            b.event.occurred_at,
        )


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_every_event_id_carries_the_run_prefix(scenario: str) -> None:
    plan = build_plan(scenario, 3, "run7")

    assert plan.deliveries
    assert all(d.event.event_id.startswith(plan.id_prefix) for d in plan.deliveries)


def test_burst_is_one_event_delivered_32_times_at_once() -> None:
    (delivery,) = build_plan("burst", 1, "r").deliveries

    assert (delivery.copies, delivery.concurrent) == (BURST_SIZE, True)


def test_duplicates_repeat_events_one_to_five_times() -> None:
    copies = [d.copies for d in build_plan("duplicates", 11, "r").deliveries]

    assert all(1 <= n <= 5 for n in copies)
    assert any(n > 1 for n in copies)


def test_out_of_order_sends_a_resource_out_of_occurrence_order() -> None:
    events = [d.event for d in build_plan("out_of_order", 5, "r").deliveries]
    by_resource: dict[str, list[tuple[object, str]]] = {}
    for event in events:
        by_resource.setdefault(event.resource_id, []).append(event.ordering_key)

    assert any(keys != sorted(keys) for keys in by_resource.values())


def test_delayed_pauses_before_each_delivery() -> None:
    steps = build_plan("delayed", 2, "r").steps

    assert [type(step) for step in steps] == [Pause, Delivery] * 3
    assert all(0.2 <= step.seconds <= 1.0 for step in steps if isinstance(step, Pause))


@pytest.mark.parametrize(
    ("scenario", "signing"),
    [("bad_signature", Signing.BAD_SIGNATURE), ("expired", Signing.EXPIRED)],
)
def test_rejected_scenarios_sign_wrongly(scenario: str, signing: Signing) -> None:
    assert {d.signing for d in build_plan(scenario, 1, "r").deliveries} == {signing}


def test_poison_uses_an_unknown_event_type() -> None:
    assert {d.event.event_type for d in build_plan("poison", 1, "r").deliveries} == {POISON_TYPE}


def test_chaos_contains_every_basic_scenario_in_its_own_order() -> None:
    plan = build_plan("chaos", 42, "r")
    ids = [d.event.event_id for d in plan.deliveries]

    for name in BASIC:
        own = [i for i in ids if i.startswith(f"r-{name}-")]
        assert own, name
    happy = [i for i in ids if i.startswith("r-happy-")]
    assert happy == sorted(happy)  # a scenario keeps its own order inside the mix
    blocks = [i.split("-")[1] for i in ids]
    switches = sum(1 for a, b in pairwise(blocks) if a != b)
    assert switches > len(BASIC)  # interleaved, not one scenario after another


@pytest.mark.parametrize("run_id", ["", "has-dash", "sp ace"])
def test_run_id_must_be_a_simple_name(run_id: str) -> None:
    with pytest.raises(ValueError, match="run_id"):
        build_plan("happy", 1, run_id)


def test_unknown_scenario_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown scenario"):
        build_plan("nope", 1, "r")
