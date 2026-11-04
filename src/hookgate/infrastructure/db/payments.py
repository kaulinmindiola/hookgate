"""Payment projection and applied effects (FR-PRO-3, FR-ORD-1..4, SDD 8, 11 and 13).

All methods run in the caller's transaction. ``lock_resource()`` serializes events of
the same resource with a row lock (events of other resources are not blocked) and
returns the ordering key of the last applied event. ``apply()`` writes the snapshot and
the ``applied_effects`` row; ``applied_effects.event_id`` is a primary key, so a second
effect for the same event fails structurally instead of silently applying twice.
"""

import uuid
from typing import cast

from sqlalchemy import Table, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from hookgate.application.records import PaymentRecord
from hookgate.domain.ordering import OrderingKey
from hookgate.domain.payments import Payment
from hookgate.infrastructure.db.models import AppliedEffectRow, PaymentRow

_PAYMENTS = cast(Table, PaymentRow.__table__)
_EFFECTS = cast(Table, AppliedEffectRow.__table__)


class PaymentProjection:
    def __init__(self, session: Session) -> None:
        self._session = session

    def lock_resource(self, resource_id: str) -> OrderingKey | None:
        """Ensure the row exists, lock it until commit and return its last applied key.

        A row created here has NULL ``last_*`` columns: None means "no event applied
        yet", so nothing is stale for it (decision P5).
        """
        self._session.execute(
            pg_insert(_PAYMENTS)
            .values(resource_id=resource_id)
            .on_conflict_do_nothing(index_elements=[_PAYMENTS.c.resource_id])
        )
        row = self._session.execute(
            select(_PAYMENTS.c.last_occurred_at, _PAYMENTS.c.last_event_id)
            .where(_PAYMENTS.c.resource_id == resource_id)
            .with_for_update()
        ).one()
        if row.last_occurred_at is None or row.last_event_id is None:
            return None
        return OrderingKey(row.last_occurred_at, row.last_event_id)

    def apply(self, payment: Payment, event_id: uuid.UUID) -> None:
        """Write the full snapshot and record the effect. Call ``lock_resource()`` first."""
        updated = self._session.execute(
            update(_PAYMENTS)
            .where(_PAYMENTS.c.resource_id == payment.resource_id)
            .values(
                status=payment.status.value,
                amount=payment.amount,
                currency=payment.currency,
                last_occurred_at=payment.last_key.occurred_at,
                last_event_id=payment.last_key.event_id,
                updated_at=func.now(),
            )
            .returning(_PAYMENTS.c.resource_id)
        ).first()
        if updated is None:
            raise RuntimeError(f"payment row {payment.resource_id!r} is missing: lock it first")
        self._session.execute(insert(_EFFECTS).values(event_id=event_id))

    def get(self, resource_id: str) -> PaymentRecord | None:
        row = self._session.execute(
            select(_PAYMENTS).where(_PAYMENTS.c.resource_id == resource_id)
        ).first()
        if row is None:
            return None
        data = row._mapping
        return PaymentRecord(
            resource_id=data["resource_id"],
            status=data["status"],
            amount=data["amount"],
            currency=data["currency"],
            last_occurred_at=data["last_occurred_at"],
            last_event_id=data["last_event_id"],
            updated_at=data["updated_at"],
        )

    def effect_count(self, event_id: uuid.UUID) -> int:
        statement = (
            select(func.count()).select_from(_EFFECTS).where(_EFFECTS.c.event_id == event_id)
        )
        count: int = self._session.execute(statement).scalar_one()
        return count
