from collections.abc import Mapping
from datetime import UTC, datetime

import pytest

from hookgate.domain.errors import PermanentError
from hookgate.domain.events import Event, EventIdentity
from hookgate.domain.ordering import OrderingKey
from hookgate.domain.payments import (
    Payment,
    PaymentSnapshot,
    PaymentStatus,
    parse_snapshot,
    project_event,
    status_for_event_type,
)

OCCURRED = datetime(2026, 11, 2, 10, 0, 0, tzinfo=UTC)


def make_event(
    event_type: str = "payment.succeeded",
    resource_type: str = "payment",
    data: Mapping[str, object] | None = None,
) -> Event:
    return Event(
        identity=EventIdentity("sim", "evt_1"),
        event_type=event_type,
        resource_type=resource_type,
        resource_id="pay_1",
        occurred_at=OCCURRED,
        data={"amount": 1500, "currency": "USD"} if data is None else data,
    )


@pytest.mark.parametrize(
    ("event_type", "expected"),
    [
        ("payment.created", PaymentStatus.CREATED),
        ("payment.authorized", PaymentStatus.AUTHORIZED),
        ("payment.succeeded", PaymentStatus.SUCCEEDED),
        ("payment.failed", PaymentStatus.FAILED),
        ("payment.refunded", PaymentStatus.REFUNDED),
    ],
)
def test_event_type_maps_to_status(event_type: str, expected: PaymentStatus) -> None:
    assert status_for_event_type(event_type) == expected


def test_unknown_event_type_is_a_permanent_error() -> None:
    with pytest.raises(PermanentError, match="unknown event type"):
        status_for_event_type("payment.exploded")


def test_valid_snapshot_is_parsed() -> None:
    assert parse_snapshot({"amount": 0, "currency": "COP"}) == PaymentSnapshot(0, "COP")


@pytest.mark.parametrize(
    "data",
    [
        {},
        {"currency": "USD"},
        {"amount": 10},
        {"amount": -1, "currency": "USD"},
        {"amount": True, "currency": "USD"},
        {"amount": 1.5, "currency": "USD"},
        {"amount": "10", "currency": "USD"},
        {"amount": 10, "currency": "usd"},
        {"amount": 10, "currency": "US"},
        {"amount": 10, "currency": 5},
    ],
)
def test_invalid_snapshot_is_a_permanent_error(data: Mapping[str, object]) -> None:
    with pytest.raises(PermanentError):
        parse_snapshot(data)


def test_project_event_builds_the_payment_state() -> None:
    payment = project_event(make_event())
    assert payment == Payment(
        resource_id="pay_1",
        status=PaymentStatus.SUCCEEDED,
        amount=1500,
        currency="USD",
        last_key=OrderingKey(OCCURRED, "evt_1"),
    )


def test_project_event_rejects_other_resource_types() -> None:
    with pytest.raises(PermanentError, match="unsupported resource type"):
        project_event(make_event(resource_type="invoice"))


def test_project_event_rejects_unknown_type_and_bad_snapshot() -> None:
    with pytest.raises(PermanentError):
        project_event(make_event(event_type="payment.exploded"))
    with pytest.raises(PermanentError):
        project_event(make_event(data={"amount": -5, "currency": "USD"}))
