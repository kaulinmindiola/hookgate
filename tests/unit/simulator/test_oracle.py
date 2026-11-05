"""Expected-state oracle (FR-SIM-3): it must pass a correct system and catch a broken one."""

from datetime import UTC, datetime, timedelta

import pytest

from simulator.model import POISON_TYPE, Delivery, EventSpec, Plan, Signing
from simulator.oracle import (
    ObservedEvent,
    PaymentState,
    Response,
    check_responses,
    check_state,
    expectations,
    pending,
)
from simulator.scenarios import build_plan

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def event(n: int, resource: str = "pay_1", event_type: str = "payment.succeeded") -> EventSpec:
    return EventSpec(f"r-e{n}", event_type, resource, T0 + timedelta(minutes=n), 100)


def plan_of(*deliveries: Delivery) -> Plan:
    return Plan("custom", 0, "r", tuple(deliveries))


def correct_state(plan: Plan) -> dict[str, ObservedEvent]:
    """What a correct Hookgate would report: every older event ignored as stale."""
    expected = expectations(plan)
    return {
        e.event_id: ObservedEvent(
            e.event_id,
            e.status,
            None if e.status == "dead" else ("applied" if e.must_be_applied else "ignored_stale"),
        )
        for e in expected.stored.values()
    }


def correct_payments(plan: Plan) -> dict[str, PaymentState | None]:
    return dict(expectations(plan).payments)


def correct_responses(plan: Plan) -> list[Response]:
    expected = expectations(plan)
    responses = []
    seen: set[str] = set()
    for delivery in plan.deliveries:
        event_id = delivery.event.event_id
        for _ in range(delivery.copies):
            if event_id in expected.rejected:
                responses.append(Response(event_id, 401, expected.rejected[event_id]))
            elif event_id in seen:
                responses.append(Response(event_id, 202, "duplicate"))
            else:
                seen.add(event_id)
                responses.append(Response(event_id, 202, "accepted"))
    return responses


# -- expectations --------------------------------------------------------------------


def test_latest_event_per_resource_must_be_applied() -> None:
    plan = plan_of(Delivery(event(3)), Delivery(event(1)), Delivery(event(2, "pay_2")))

    stored = expectations(plan).stored

    assert {k: v.must_be_applied for k, v in stored.items()} == {
        "r-e3": True,
        "r-e1": False,
        "r-e2": True,
    }


def test_ties_on_occurred_at_are_broken_by_event_id() -> None:
    a = EventSpec("r-a", "payment.created", "pay_1", T0, 1)
    b = EventSpec("r-b", "payment.failed", "pay_1", T0, 1)

    stored = expectations(plan_of(Delivery(b), Delivery(a))).stored

    assert (stored["r-a"].must_be_applied, stored["r-b"].must_be_applied) == (False, True)


def test_poison_is_dead_and_never_the_latest() -> None:
    plan = plan_of(Delivery(event(1)), Delivery(event(9, event_type=POISON_TYPE)))

    expected = expectations(plan)

    assert expected.stored["r-e9"].status == "dead"
    assert expected.stored["r-e1"].must_be_applied  # the poison does not count as latest
    assert expected.dead_ids == {"r-e9"}


def test_wrongly_signed_events_are_expected_rejected_not_stored() -> None:
    plan = plan_of(Delivery(event(1), Signing.BAD_SIGNATURE), Delivery(event(2), Signing.EXPIRED))

    expected = expectations(plan)

    assert expected.stored == {}
    assert expected.rejected == {
        "r-e1": "invalid_signature",
        "r-e2": "timestamp_out_of_tolerance",
    }


# -- a correct system passes ------------------------------------------------------------


@pytest.mark.parametrize("seed", [1, 7, 42])
def test_a_correct_system_passes_chaos(seed: int) -> None:
    plan = build_plan("chaos", seed, "r")
    state = correct_state(plan)
    dead = [k for k, v in state.items() if v.status == "dead"]

    assert check_responses(plan, correct_responses(plan)) == []
    assert (
        check_state(expectations(plan), state, listed_dead=dead, payments=correct_payments(plan))
        == []
    )
    assert pending(expectations(plan), state) == []


def test_older_events_may_be_applied_too() -> None:
    plan = plan_of(Delivery(event(1)), Delivery(event(2)))
    state = correct_state(plan)
    state["r-e1"] = ObservedEvent("r-e1", "processed", "applied")  # processed before e2

    assert (
        check_state(expectations(plan), state, listed_dead=[], payments=correct_payments(plan))
        == []
    )


# -- a broken system is caught (deliberate mutations) ---------------------------------


@pytest.fixture
def chaos_plan() -> Plan:
    return build_plan("chaos", 7, "r")


def first_id(plan: Plan, *, status: str, latest: bool | None = None) -> str:
    for e in expectations(plan).stored.values():
        if e.status == status and (latest is None or e.must_be_applied is latest):
            return e.event_id
    raise AssertionError("no such event in the plan")


def test_a_lost_event_is_caught(chaos_plan: Plan) -> None:
    state = correct_state(chaos_plan)
    lost = first_id(chaos_plan, status="processed")
    del state[lost]

    problems = check_state(
        expectations(chaos_plan), state, listed_dead=[], payments=correct_payments(chaos_plan)
    )

    assert any(lost in p and "not stored" in p for p in problems)


def test_a_stale_latest_event_is_caught(chaos_plan: Plan) -> None:
    state = correct_state(chaos_plan)
    latest = first_id(chaos_plan, status="processed", latest=True)
    state[latest] = ObservedEvent(latest, "processed", "ignored_stale")
    dead = [k for k, v in state.items() if v.status == "dead"]

    problems = check_state(
        expectations(chaos_plan), state, listed_dead=dead, payments=correct_payments(chaos_plan)
    )

    assert problems == [f"{latest}: latest event of its resource must be applied, is ignored_stale"]


def test_a_poison_that_was_processed_or_not_listed_is_caught(chaos_plan: Plan) -> None:
    state = correct_state(chaos_plan)
    poison = first_id(chaos_plan, status="dead")
    state[poison] = ObservedEvent(poison, "processed", "applied")

    problems = check_state(
        expectations(chaos_plan), state, listed_dead=[], payments=correct_payments(chaos_plan)
    )

    assert f"{poison}: expected dead, is processed" in problems
    assert f"{poison}: not listed by GET /admin/events?status=dead" in problems


def test_a_stored_rejected_event_is_caught(chaos_plan: Plan) -> None:
    state = correct_state(chaos_plan)
    rejected = next(iter(expectations(chaos_plan).rejected))
    state[rejected] = ObservedEvent(rejected, "processed", "applied")
    dead = [k for k, v in state.items() if v.status == "dead"]

    problems = check_state(
        expectations(chaos_plan), state, listed_dead=dead, payments=correct_payments(chaos_plan)
    )

    assert problems == [f"{rejected}: stored but should have been rejected"]


def test_an_unfinished_event_is_pending(chaos_plan: Plan) -> None:
    state = correct_state(chaos_plan)
    waiting = first_id(chaos_plan, status="processed")
    state[waiting] = ObservedEvent(waiting, "failed", None)

    assert pending(expectations(chaos_plan), state) == [waiting]


def test_two_accepted_answers_for_one_event_are_caught() -> None:
    plan = build_plan("burst", 1, "r")
    responses = correct_responses(plan)
    responses[5] = Response(responses[5].event_id, 202, "accepted")

    (problem,) = check_responses(plan, responses)

    assert "expected 1 accepted and 31 duplicate" in problem


def test_an_accepted_bad_signature_is_caught() -> None:
    plan = build_plan("bad_signature", 1, "r")
    responses = correct_responses(plan)
    responses[0] = Response(responses[0].event_id, 202, "accepted")

    problems = check_responses(plan, responses)

    assert problems == [
        f"{responses[0].event_id}: expected 401 invalid_signature, got 202 accepted"
    ]


def test_missing_responses_are_caught() -> None:
    plan = build_plan("duplicates", 3, "r")

    problems = check_responses(plan, correct_responses(plan)[1:])

    assert len(problems) == 1
    assert "responses" in problems[0]


# -- final payment state (FR-ORD-4) ----------------------------------------------------


def test_expected_payment_is_the_latest_event_snapshot() -> None:
    latest = event(5)
    plan = plan_of(Delivery(latest), Delivery(event(2)), Delivery(event(9, event_type=POISON_TYPE)))

    assert expectations(plan).payments == {
        "pay_1": PaymentState("succeeded", latest.amount, "USD", latest.event_id)
    }


def test_a_payment_overwritten_by_an_older_event_is_caught(chaos_plan: Plan) -> None:
    """The bug outcomes cannot show: an older event applied after the latest one."""
    state = correct_state(chaos_plan)
    dead = [k for k, v in state.items() if v.status == "dead"]
    payments = correct_payments(chaos_plan)
    resource_id, right = next(iter(payments.items()))
    assert right is not None
    payments[resource_id] = PaymentState(right.status, right.amount, "USD", "older-event")

    problems = check_state(expectations(chaos_plan), state, listed_dead=dead, payments=payments)

    assert len(problems) == 1
    assert problems[0].startswith(f"{resource_id}: payment should be")


def test_a_missing_payment_is_caught(chaos_plan: Plan) -> None:
    state = correct_state(chaos_plan)
    dead = [k for k, v in state.items() if v.status == "dead"]
    payments = correct_payments(chaos_plan)
    resource_id = next(iter(payments))
    payments[resource_id] = None

    problems = check_state(expectations(chaos_plan), state, listed_dead=dead, payments=payments)

    wanted = expectations(chaos_plan).payments[resource_id]
    assert problems == [f"{resource_id}: payment should be {wanted}, is None"]
