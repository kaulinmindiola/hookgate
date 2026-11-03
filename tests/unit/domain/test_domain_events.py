from datetime import UTC, datetime

import pytest

from hookgate.domain.events import Event, EventIdentity
from hookgate.domain.ordering import OrderingKey

OCCURRED = datetime(2026, 11, 2, 10, 15, 30, 123456, tzinfo=UTC)


def make_event(**overrides: object) -> Event:
    values: dict[str, object] = {
        "identity": EventIdentity("sim", "evt_1"),
        "event_type": "payment.created",
        "resource_type": "payment",
        "resource_id": "pay_1",
        "occurred_at": OCCURRED,
    }
    values.update(overrides)
    return Event(**values)  # type: ignore[arg-type]


def test_identity_is_provider_and_event_id() -> None:
    assert EventIdentity("sim", "evt_1") == EventIdentity("sim", "evt_1")
    assert EventIdentity("sim", "evt_1") != EventIdentity("other", "evt_1")


@pytest.mark.parametrize(("provider", "event_id"), [("", "evt_1"), ("sim", "")])
def test_identity_rejects_empty_parts(provider: str, event_id: str) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        EventIdentity(provider, event_id)


def test_event_exposes_id_and_ordering_key() -> None:
    event = make_event()
    assert event.event_id == "evt_1"
    assert event.ordering_key() == OrderingKey(OCCURRED, "evt_1")


def test_event_rejects_naive_occurred_at() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        make_event(occurred_at=datetime(2026, 11, 2, 10, 0, 0))
