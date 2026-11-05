"""Read one payment of the projection (admin API).

Exists so that an external checker, the simulator's oracle (FR-SIM-3), can verify the
final state of a resource. Event outcomes alone cannot show it: an older event that
wrongly overwrote a newer one is reported ``applied`` either way (found in phase F10).
"""

from hookgate.application.ports import PaymentReadScope
from hookgate.application.records import PaymentRecord


class InspectPayment:
    def __init__(self, *, units: PaymentReadScope) -> None:
        self._units = units

    def __call__(self, resource_id: str) -> PaymentRecord | None:
        with self._units() as payments:
            return payments.get(resource_id)
