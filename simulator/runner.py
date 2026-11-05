"""Send a plan and verify the result (FR-SIM-1, FR-SIM-3)."""

import time
from collections.abc import Callable
from dataclasses import dataclass

from simulator.client import AdminClient, ProviderClient
from simulator.model import Delivery, Pause, Plan
from simulator.oracle import (
    ObservedEvent,
    Response,
    check_state,
    expectations,
    pending,
)


def send(
    plan: Plan, provider: ProviderClient, *, sleep: Callable[[float], None] | None = None
) -> list[Response]:
    sleep = sleep or time.sleep
    responses: list[Response] = []
    for step in plan.steps:
        if isinstance(step, Pause):
            sleep(step.seconds)
        elif isinstance(step, Delivery):
            responses.extend(provider.deliver(step))
    return responses


@dataclass(frozen=True, slots=True)
class VerifyReport:
    problems: list[str]
    observed: dict[str, ObservedEvent]
    waited_s: float

    @property
    def ok(self) -> bool:
        return not self.problems


def verify(
    plan: Plan,
    admin: AdminClient,
    *,
    timeout_s: float = 60.0,
    poll_s: float = 0.5,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> VerifyReport:
    """Wait until every expected event is final (or the timeout passes), then compare."""
    clock = clock or time.monotonic
    sleep = sleep or time.sleep
    expected = expectations(plan)
    started = clock()
    while True:
        observed = admin.events(prefix=plan.id_prefix)
        waiting = pending(expected, observed)
        if not waiting or clock() - started >= timeout_s:
            break
        sleep(poll_s)
    listed_dead = admin.events(prefix=plan.id_prefix, status="dead")
    payments = {resource_id: admin.payment(resource_id) for resource_id in expected.payments}
    problems = check_state(expected, observed, listed_dead=listed_dead.keys(), payments=payments)
    if waiting:
        problems.insert(0, f"{len(waiting)} event(s) not final after {timeout_s:.0f} s")
    return VerifyReport(problems=problems, observed=observed, waited_s=clock() - started)
