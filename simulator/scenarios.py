"""Provider behaviours (FR-SIM-2, SDD 15.1). Same scenario, seed and run id: same plan.

Every value comes from ``random.Random(seed)`` and a fixed base time, never from the
clock, so a plan can be rebuilt later for ``verify``. The run id only names the events.
"""

import random
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from simulator.model import (
    KNOWN_TYPES,
    POISON_TYPE,
    Delivery,
    EventSpec,
    Pause,
    Plan,
    Signing,
    Step,
)

BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)
BURST_SIZE = 32  # SDD 15.1: one event, 32 concurrent deliveries

Builder = Callable[[random.Random, str], list[Step]]


class _Ids:
    """Event and resource ids under one namespace (run id plus scenario tag)."""

    def __init__(self, namespace: str) -> None:
        self._namespace = namespace
        self._events = 0

    def event(self) -> str:
        self._events += 1
        return f"{self._namespace}-evt{self._events:03d}"

    def resource(self, n: int) -> str:
        return f"pay_{self._namespace}_{n}"


def _history(
    rng: random.Random, ids: _Ids, resource_id: str, length: int, *, start_minute: int = 0
) -> list[EventSpec]:
    """A resource's events in occurrence order, each one a full snapshot."""
    minute = start_minute
    events = []
    for _ in range(length):
        minute += rng.randint(1, 30)
        events.append(
            EventSpec(
                event_id=ids.event(),
                event_type=rng.choice(KNOWN_TYPES),
                resource_id=resource_id,
                occurred_at=BASE_TIME + timedelta(minutes=minute),
                amount=rng.randint(100, 100_000),
            )
        )
    return events


def happy(rng: random.Random, namespace: str) -> list[Step]:
    ids = _Ids(namespace)
    steps: list[Step] = []
    for n in range(3):
        steps.extend(Delivery(e) for e in _history(rng, ids, ids.resource(n), rng.randint(2, 3)))
    return steps


def duplicates(rng: random.Random, namespace: str) -> list[Step]:
    ids = _Ids(namespace)
    events = _history(rng, ids, ids.resource(0), 5)
    return [Delivery(e, copies=rng.randint(1, 5)) for e in events]


def delayed(rng: random.Random, namespace: str) -> list[Step]:
    """Events held by the provider and sent later; each is signed when it is sent."""
    ids = _Ids(namespace)
    steps: list[Step] = []
    for event in _history(rng, ids, ids.resource(0), 3):
        steps.append(Pause(round(rng.uniform(0.2, 1.0), 2)))
        steps.append(Delivery(event))
    return steps


def out_of_order(rng: random.Random, namespace: str) -> list[Step]:
    ids = _Ids(namespace)
    steps: list[Step] = []
    for n in range(2):
        events = _history(rng, ids, ids.resource(n), 4)
        rng.shuffle(events)
        steps.extend(Delivery(e) for e in events)
    return steps


def burst(rng: random.Random, namespace: str) -> list[Step]:
    ids = _Ids(namespace)
    (event,) = _history(rng, ids, ids.resource(0), 1)
    return [Delivery(event, copies=BURST_SIZE, concurrent=True)]


def bad_signature(rng: random.Random, namespace: str) -> list[Step]:
    ids = _Ids(namespace)
    return [Delivery(e, Signing.BAD_SIGNATURE) for e in _history(rng, ids, ids.resource(0), 2)]


def expired(rng: random.Random, namespace: str) -> list[Step]:
    ids = _Ids(namespace)
    return [Delivery(e, Signing.EXPIRED) for e in _history(rng, ids, ids.resource(0), 2)]


def poison(rng: random.Random, namespace: str) -> list[Step]:
    ids = _Ids(namespace)
    events = _history(rng, ids, ids.resource(0), 2)
    return [
        Delivery(EventSpec(e.event_id, POISON_TYPE, e.resource_id, e.occurred_at, e.amount))
        for e in events
    ]


BASIC: dict[str, Builder] = {
    "happy": happy,
    "duplicates": duplicates,
    "delayed": delayed,
    "out_of_order": out_of_order,
    "burst": burst,
    "bad_signature": bad_signature,
    "expired": expired,
    "poison": poison,
}


def chaos(rng: random.Random, namespace: str) -> list[Step]:
    """Every basic scenario, interleaved by the seed; each keeps its own internal order."""
    queues = [list(build(rng, f"{namespace}-{name}")) for name, build in BASIC.items()]
    steps: list[Step] = []
    while any(queues):
        queue = rng.choice([q for q in queues if q])
        steps.append(queue.pop(0))
    return steps


SCENARIOS: dict[str, Builder] = {**BASIC, "chaos": chaos}


def build_plan(scenario: str, seed: int, run_id: str) -> Plan:
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario {scenario!r}; choose from {sorted(SCENARIOS)}")
    if not run_id or not run_id.replace("_", "").isalnum():
        raise ValueError("run_id must be letters, digits or underscores")
    rng = random.Random(seed)
    steps = SCENARIOS[scenario](rng, run_id)
    return Plan(scenario=scenario, seed=seed, run_id=run_id, steps=tuple(steps))
