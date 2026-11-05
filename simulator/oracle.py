"""Expected-state oracle (FR-SIM-3).

From the plan alone it derives what Hookgate must end up with:

- every validly signed event is stored exactly once, and exactly one of its deliveries
  is answered ``accepted`` (the rest ``duplicate``), however many copies were sent;
- deliveries with a bad signature or an expired timestamp get 401 and are not stored;
- known event types end ``processed``; unknown ones (poison) end ``dead``;
- for each resource, the latest processable event (by ``(occurred_at, event_id)``) ends
  with outcome ``applied``. Older ones may be ``applied`` or ``ignored_stale`` depending
  on processing order;
- the payment of each resource ends exactly as its latest event describes it: status,
  amount, currency and ``last_event_id`` (FR-ORD-4). Outcomes alone cannot show this:
  an older event wrongly applied after the latest one is also reported ``applied``.

The oracle has its own list of event types; it shares no code with Hookgate.
"""

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from simulator.model import CURRENCY, KNOWN_TYPES, Delivery, EventSpec, Plan, Signing

# The provider contract (SDD 13): each event type sets one payment status.
STATUS_BY_TYPE = {
    "payment.created": "created",
    "payment.authorized": "authorized",
    "payment.succeeded": "succeeded",
    "payment.failed": "failed",
    "payment.refunded": "refunded",
}
assert set(STATUS_BY_TYPE) == set(KNOWN_TYPES)

REJECTION_ERRORS = {
    Signing.BAD_SIGNATURE: "invalid_signature",
    Signing.EXPIRED: "timestamp_out_of_tolerance",
}


@dataclass(frozen=True, slots=True)
class ExpectedEvent:
    event_id: str
    status: str  # processed or dead
    must_be_applied: bool  # latest processable event of its resource


@dataclass(frozen=True, slots=True)
class PaymentState:
    """A payment as the projection must hold it (and as the admin API reports it)."""

    status: str | None
    amount: int | None
    currency: str | None
    last_event_id: str | None


@dataclass(frozen=True, slots=True)
class Expectations:
    stored: dict[str, ExpectedEvent]
    rejected: dict[str, str]  # event id -> expected 401 error code
    dead_ids: frozenset[str] = field(default_factory=frozenset)
    payments: dict[str, PaymentState] = field(default_factory=dict)  # resource id -> state


@dataclass(frozen=True, slots=True)
class Response:
    """What one delivery got back: HTTP status and the ``status`` or ``error`` field."""

    event_id: str
    status_code: int
    result: str


@dataclass(frozen=True, slots=True)
class ObservedEvent:
    event_id: str
    status: str
    outcome: str | None


def expectations(plan: Plan) -> Expectations:
    stored: dict[str, ExpectedEvent] = {}
    rejected: dict[str, str] = {}
    valid: dict[str, EventSpec] = {}
    for delivery in plan.deliveries:
        event = delivery.event
        if delivery.signing is Signing.VALID:
            valid[event.event_id] = event
        else:
            rejected[event.event_id] = REJECTION_ERRORS[delivery.signing]

    latest: dict[str, EventSpec] = {}
    for event in valid.values():
        if event.event_type not in KNOWN_TYPES:
            continue
        current = latest.get(event.resource_id)
        if current is None or event.ordering_key > current.ordering_key:
            latest[event.resource_id] = event
    latest_ids = {event.event_id for event in latest.values()}

    for event in valid.values():
        known = event.event_type in KNOWN_TYPES
        stored[event.event_id] = ExpectedEvent(
            event_id=event.event_id,
            status="processed" if known else "dead",
            must_be_applied=event.event_id in latest_ids,
        )
    dead = frozenset(e.event_id for e in stored.values() if e.status == "dead")
    payments = {
        resource_id: PaymentState(
            status=STATUS_BY_TYPE[event.event_type],
            amount=event.amount,
            currency=CURRENCY,
            last_event_id=event.event_id,
        )
        for resource_id, event in latest.items()
    }
    return Expectations(stored=stored, rejected=rejected, dead_ids=dead, payments=payments)


def copies_sent(deliveries: Iterable[Delivery]) -> Counter[str]:
    sent: Counter[str] = Counter()
    for delivery in deliveries:
        sent[delivery.event.event_id] += delivery.copies
    return sent


def check_responses(plan: Plan, responses: Iterable[Response]) -> list[str]:
    """HTTP-level checks, done right after sending (idempotency and rejections)."""
    expected = expectations(plan)
    sent = copies_sent(plan.deliveries)
    by_event: dict[str, list[Response]] = {}
    for response in responses:
        by_event.setdefault(response.event_id, []).append(response)

    problems: list[str] = []
    for event_id, count in sorted(sent.items()):
        got = by_event.get(event_id, [])
        if len(got) != count:
            problems.append(f"{event_id}: sent {count} deliveries, got {len(got)} responses")
            continue
        if event_id in expected.rejected:
            wanted = expected.rejected[event_id]
            for response in got:
                if (response.status_code, response.result) != (401, wanted):
                    problems.append(
                        f"{event_id}: expected 401 {wanted}, got "
                        f"{response.status_code} {response.result}"
                    )
            continue
        codes = Counter((r.status_code, r.result) for r in got)
        if codes[(202, "accepted")] != 1 or codes[(202, "duplicate")] != count - 1:
            problems.append(
                f"{event_id}: expected 1 accepted and {count - 1} duplicate, got {dict(codes)}"
            )
    return problems


def pending(expected: Expectations, observed: Mapping[str, ObservedEvent]) -> list[str]:
    """Events that are expected but not yet in a final state (still worth waiting for)."""
    return sorted(
        event_id
        for event_id in expected.stored
        if event_id not in observed or observed[event_id].status not in ("processed", "dead")
    )


def check_state(
    expected: Expectations,
    observed: Mapping[str, ObservedEvent],
    *,
    listed_dead: Iterable[str],
    payments: Mapping[str, PaymentState | None],
) -> list[str]:
    """Final-state checks against what the admin API reports for this run's events."""
    problems: list[str] = []
    for event_id, want in sorted(expected.stored.items()):
        got = observed.get(event_id)
        if got is None:
            problems.append(f"{event_id}: expected {want.status}, not stored")
            continue
        if got.status != want.status:
            problems.append(f"{event_id}: expected {want.status}, is {got.status}")
        elif want.must_be_applied and got.outcome != "applied":
            problems.append(
                f"{event_id}: latest event of its resource must be applied, is {got.outcome}"
            )
    for event_id in sorted(observed.keys() - expected.stored.keys()):
        problems.append(f"{event_id}: stored but should have been rejected")
    missing_from_dlq = expected.dead_ids - set(listed_dead)
    for event_id in sorted(missing_from_dlq):
        problems.append(f"{event_id}: not listed by GET /admin/events?status=dead")
    for resource_id, want_payment in sorted(expected.payments.items()):
        got_payment = payments.get(resource_id)
        if got_payment != want_payment:
            problems.append(f"{resource_id}: payment should be {want_payment}, is {got_payment}")
    return problems
