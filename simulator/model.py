"""The delivery plan: what a scenario sends, built only from a seeded RNG (FR-SIM-1)."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

PROVIDER = "sim"
CURRENCY = "USD"
# Types the provider can emit. Anything else is a poison payload.
KNOWN_TYPES = (
    "payment.created",
    "payment.authorized",
    "payment.succeeded",
    "payment.failed",
    "payment.refunded",
)
POISON_TYPE = "payment.exploded"


class Signing(StrEnum):
    VALID = "valid"
    BAD_SIGNATURE = "bad_signature"  # signed with the wrong secret
    EXPIRED = "expired"  # signed correctly, but with a timestamp far in the past


@dataclass(frozen=True, slots=True)
class EventSpec:
    event_id: str
    event_type: str
    resource_id: str
    occurred_at: datetime
    amount: int

    @property
    def ordering_key(self) -> tuple[datetime, str]:
        return (self.occurred_at, self.event_id)


@dataclass(frozen=True, slots=True)
class Delivery:
    event: EventSpec
    signing: Signing = Signing.VALID
    copies: int = 1
    concurrent: bool = False  # True: all copies at the same instant (burst)


@dataclass(frozen=True, slots=True)
class Pause:
    seconds: float


Step = Delivery | Pause


@dataclass(frozen=True, slots=True)
class Plan:
    scenario: str
    seed: int
    run_id: str
    steps: tuple[Step, ...]

    @property
    def deliveries(self) -> list[Delivery]:
        return [step for step in self.steps if isinstance(step, Delivery)]

    @property
    def id_prefix(self) -> str:
        """Every event id of the plan starts with this, so runs never collide."""
        return f"{self.run_id}-"
