"""Event model and identity (FR-IDE-1)."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from hookgate.domain.ordering import OrderingKey
from hookgate.domain.timeutil import require_aware


@dataclass(frozen=True, slots=True)
class EventIdentity:
    """An event is identified by ``(provider, event_id)``."""

    provider: str
    event_id: str

    def __post_init__(self) -> None:
        if not self.provider:
            raise ValueError("provider must not be empty")
        if not self.event_id:
            raise ValueError("event_id must not be empty")


@dataclass(frozen=True, slots=True)
class Event:
    identity: EventIdentity
    event_type: str
    resource_type: str
    resource_id: str
    occurred_at: datetime
    data: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_aware(self.occurred_at, "occurred_at")

    @property
    def event_id(self) -> str:
        return self.identity.event_id

    def ordering_key(self) -> OrderingKey:
        return OrderingKey(self.occurred_at, self.identity.event_id)
