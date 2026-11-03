"""Signed webhook ingestion endpoint (FR-ING-1..7)."""

from typing import Annotated

from fastapi import APIRouter, Depends, Request, status

from hookgate.api.deps import get_ingest_event, get_trace_id, read_raw_body
from hookgate.api.schemas import ErrorBody, WebhookAck
from hookgate.application.ingest_event import IngestCommand, IngestEvent

SIGNATURE_HEADER = "Hookgate-Signature"

router = APIRouter(tags=["webhooks"])


@router.post(
    "/webhooks/payments",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        400: {"model": ErrorBody, "description": "Not JSON or a required field is missing"},
        401: {"model": ErrorBody, "description": "Signature or timestamp rejected"},
        413: {"model": ErrorBody, "description": "Body larger than 1 MiB"},
    },
)
def receive_payment_webhook(
    request: Request,
    raw_body: Annotated[bytes, Depends(read_raw_body)],
    trace_id: Annotated[str, Depends(get_trace_id)],
    ingest: Annotated[IngestEvent, Depends(get_ingest_event)],
) -> WebhookAck:
    # A plain `def` endpoint: FastAPI runs it in a threadpool, so the synchronous
    # database work does not block the event loop (ADR-005).
    result = ingest(
        IngestCommand(
            raw_body=raw_body,
            signature_header=request.headers.get(SIGNATURE_HEADER),
            headers=dict(request.headers),
            trace_id=trace_id,
        )
    )
    return WebhookAck(status=result.status)
