"""Map domain and application errors to HTTP responses (FR-ING-2/3/4, FR-SEC-1)."""

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from hookgate.api.cursor import InvalidCursor
from hookgate.api.schemas import ErrorBody
from hookgate.application.ingest_event import InvalidPayload
from hookgate.domain.errors import SignatureError

_log = structlog.get_logger(__name__)


class PayloadTooLarge(Exception):
    def __init__(self, limit_bytes: int) -> None:
        super().__init__(f"body exceeds {limit_bytes} bytes")
        self.limit_bytes = limit_bytes


class AdminAuthError(Exception):
    """Missing or wrong ``X-API-Key`` on an admin endpoint."""


def _error(status_code: int, body: ErrorBody) -> JSONResponse:
    return JSONResponse(status_code=status_code, content=body.model_dump(exclude_none=True))


# Starlette types every handler as (Request, Exception); each one narrows its own type.


async def _signature_error(_: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, SignatureError)
    return _error(401, ErrorBody(error=exc.reason.value))


async def _invalid_payload(_: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, InvalidPayload)
    return _error(400, ErrorBody(error="invalid_payload", detail=list(exc.problems)))


async def _payload_too_large(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, PayloadTooLarge)
    _log.warning(
        "webhook_rejected",
        reason="payload_too_large",
        limit_bytes=exc.limit_bytes,
        trace_id=getattr(request.state, "trace_id", None),
    )
    return _error(413, ErrorBody(error="payload_too_large"))


async def _admin_auth_error(_: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, AdminAuthError)
    return _error(401, ErrorBody(error="invalid_api_key"))


async def _invalid_cursor(_: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, InvalidCursor)
    return _error(400, ErrorBody(error="invalid_cursor"))


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(SignatureError, _signature_error)
    app.add_exception_handler(InvalidPayload, _invalid_payload)
    app.add_exception_handler(PayloadTooLarge, _payload_too_large)
    app.add_exception_handler(AdminAuthError, _admin_auth_error)
    app.add_exception_handler(InvalidCursor, _invalid_cursor)
