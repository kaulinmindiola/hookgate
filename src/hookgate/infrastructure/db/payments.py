"""Payment projection and applied effects (FR-PRO-3, SDD 8 and 11).

``apply()`` writes the projection and the ``applied_effects`` row in the caller's
transaction. ``applied_effects.event_id`` is a primary key, so a second effect for the
same event fails structurally instead of silently applying twice.
"""

import uuid
from typing import cast

from sqlalchemy import Table, func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from hookgate.application.records import PaymentRecord
from hookgate.domain.payments import Payment
from hookgate.infrastructure.db.models import AppliedEffectRow, PaymentRow

_PAYMENTS = cast(Table, PaymentRow.__table__)
_EFFECTS = cast(Table, AppliedEffectRow.__table__)


class PaymentProjection:
    def __init__(self, session: Session) -> None:
        self._session = session

    def apply(self, payment: Payment, event_id: uuid.UUID) -> None:
        """Upsert the projection with the full snapshot and record the effect.

        Last write wins for now; the per-resource ordering rule arrives in phase F6.
        """
        values = {
            "status": payment.status.value,
            "amount": payment.amount,
            "currency": payment.currency,
            "last_occurred_at": payment.last_key.occurred_at,
            "last_event_id": payment.last_key.event_id,
        }
        upsert = pg_insert(_PAYMENTS).values(resource_id=payment.resource_id, **values)
        upsert = upsert.on_conflict_do_update(
            index_elements=[_PAYMENTS.c.resource_id],
            set_={**values, "updated_at": func.now()},
        )
        self._session.execute(upsert)
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
