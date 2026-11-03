"""Per-resource ordering rule (FR-ORD-1, FR-ORD-2, ADR-006).

The ordering key is ``(occurred_at, event_id)``: a total, deterministic order even
when two events share a timestamp. The comparison is done here, in Python, so it never
depends on the database collation for ``event_id``.
"""

from dataclasses import dataclass
from datetime import datetime

from hookgate.domain.timeutil import require_aware


@dataclass(frozen=True, slots=True, order=True)
class OrderingKey:
    occurred_at: datetime
    event_id: str

    def __post_init__(self) -> None:
        require_aware(self.occurred_at, "occurred_at")


def is_stale(incoming: OrderingKey, last_applied: OrderingKey | None) -> bool:
    """Return True when ``incoming`` must be ignored (outcome ``ignored_stale``).

    ``last_applied`` is None for a resource that has no applied event yet (a freshly
    created projection row has NULL ``last_*`` columns), so nothing is stale.
    An equal key is the same event redelivered: it is stale too.
    """
    if last_applied is None:
        return False
    return incoming <= last_applied
