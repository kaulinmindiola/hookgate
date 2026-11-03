"""Payment projection model and the event-type to status mapping (SDD section 13).

Business validation lives here, not at ingestion: a semantically bad event is stored
and ends in the dead-letter queue instead of being rejected and lost (SDD section 10).
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

from hookgate.domain.errors import PermanentError
from hookgate.domain.events import Event
from hookgate.domain.ordering import OrderingKey

PAYMENT_RESOURCE_TYPE = "payment"


class PaymentStatus(StrEnum):
    CREATED = "created"
    AUTHORIZED = "authorized"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    REFUNDED = "refunded"


_STATUS_BY_EVENT_TYPE: Mapping[str, PaymentStatus] = MappingProxyType(
    {
        "payment.created": PaymentStatus.CREATED,
        "payment.authorized": PaymentStatus.AUTHORIZED,
        "payment.succeeded": PaymentStatus.SUCCEEDED,
        "payment.failed": PaymentStatus.FAILED,
        "payment.refunded": PaymentStatus.REFUNDED,
    }
)

_CURRENCY = re.compile(r"[A-Z]{3}")


@dataclass(frozen=True, slots=True)
class PaymentSnapshot:
    amount: int
    currency: str


@dataclass(frozen=True, slots=True)
class Payment:
    """The projection of a payment after applying an event."""

    resource_id: str
    status: PaymentStatus
    amount: int
    currency: str
    last_key: OrderingKey


def status_for_event_type(event_type: str) -> PaymentStatus:
    try:
        return _STATUS_BY_EVENT_TYPE[event_type]
    except KeyError:
        raise PermanentError(f"unknown event type: {event_type!r}") from None


def parse_snapshot(data: Mapping[str, object]) -> PaymentSnapshot:
    """Validate the full snapshot carried by an event."""
    amount = data.get("amount")
    if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
        raise PermanentError("snapshot.amount must be a non-negative integer (minor units)")
    currency = data.get("currency")
    if not isinstance(currency, str) or _CURRENCY.fullmatch(currency) is None:
        raise PermanentError("snapshot.currency must be a 3-letter uppercase code")
    return PaymentSnapshot(amount=amount, currency=currency)


def project_event(event: Event) -> Payment:
    """Build the payment state that results from applying ``event``."""
    if event.resource_type != PAYMENT_RESOURCE_TYPE:
        raise PermanentError(f"unsupported resource type: {event.resource_type!r}")
    status = status_for_event_type(event.event_type)
    snapshot = parse_snapshot(event.data)
    return Payment(
        resource_id=event.resource_id,
        status=status,
        amount=snapshot.amount,
        currency=snapshot.currency,
        last_key=event.ordering_key(),
    )
