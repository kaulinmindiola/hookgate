"""Pure helpers of process_event (no database)."""

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.orm import sessionmaker
from tests.fakes import FixedRng

from hookgate.application.process_event import (
    MAX_ERROR_LENGTH,
    ProcessEvent,
    describe_error,
    to_domain_event,
)
from hookgate.application.records import StoredEvent
from hookgate.domain.errors import PermanentError
from hookgate.domain.events import EventIdentity
from hookgate.domain.retry import RetryPolicy
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.db.transactions import processing_scope

OCCURRED = datetime(2026, 11, 3, 11, 59, tzinfo=UTC)


def stored(raw_body: bytes) -> StoredEvent:
    return StoredEvent(
        id=uuid4(),
        provider="sim",
        event_id="evt_1",
        event_type="payment.created",
        resource_type="payment",
        resource_id="pay_1",
        occurred_at=OCCURRED,
        received_at=OCCURRED,
        raw_body=raw_body,
        headers={},
        status=EventStatus.PROCESSING,
        outcome=None,
        attempts=1,
        next_attempt_at=None,
        locked_until=None,
        last_error=None,
        trace_id="t-1",
        processed_at=None,
    )


def test_domain_event_is_rebuilt_from_columns_and_body_data() -> None:
    raw = json.dumps({"id": "evt_1", "data": {"amount": 5, "currency": "USD"}}).encode()

    event = to_domain_event(stored(raw))

    assert event.identity == EventIdentity("sim", "evt_1")
    assert (event.event_type, event.resource_type, event.resource_id) == (
        "payment.created",
        "payment",
        "pay_1",
    )
    assert event.occurred_at == OCCURRED
    assert dict(event.data) == {"amount": 5, "currency": "USD"}


def test_missing_data_becomes_an_empty_snapshot() -> None:
    assert dict(to_domain_event(stored(b'{"id": "evt_1"}')).data) == {}


@pytest.mark.parametrize("raw", [b"{broken", b'{"data": [1, 2]}', b'"just a string"'])
def test_unusable_body_is_a_permanent_error(raw: bytes) -> None:
    with pytest.raises(PermanentError):
        to_domain_event(stored(raw))


def test_error_description_has_type_and_message_and_is_bounded() -> None:
    assert describe_error(ValueError("boom")) == "ValueError: boom"
    assert len(describe_error(RuntimeError("x" * 5_000))) == MAX_ERROR_LENGTH


def test_lease_must_be_positive() -> None:
    with pytest.raises(ValueError, match="lease_s"):
        ProcessEvent(
            units=processing_scope(sessionmaker()),
            lease_s=0,
            policy=RetryPolicy(),
            rng=FixedRng(0.5),
        )
