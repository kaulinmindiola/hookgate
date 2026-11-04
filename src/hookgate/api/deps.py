"""Request-level dependencies: raw body with a size limit, trace id, admin key, use cases."""

import hmac
import re
from uuid import uuid4

import structlog
from fastapi import Request

from hookgate.api.errors import AdminAuthError, PayloadTooLarge
from hookgate.application.ingest_event import IngestEvent
from hookgate.application.inspect_events import InspectEvents
from hookgate.application.replay_event import ReplayEvent

MAX_BODY_BYTES = 1024 * 1024  # 1 MiB (FR-ING-4)
TRACE_HEADER = "X-Trace-Id"
ADMIN_KEY_HEADER = "X-API-Key"
_VALID_TRACE_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")

_log = structlog.get_logger(__name__)


def resolve_trace_id(candidate: str | None) -> str:
    """Use the caller's trace id when it is safe to log and store; otherwise generate one."""
    if candidate is not None and _VALID_TRACE_ID.fullmatch(candidate):
        return candidate
    return uuid4().hex


def get_trace_id(request: Request) -> str:
    return str(request.state.trace_id)


async def read_raw_body(request: Request) -> bytes:
    """Read the exact bytes before any JSON parsing (SDD 10), stopping past the limit."""
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > MAX_BODY_BYTES:
        raise PayloadTooLarge(MAX_BODY_BYTES)
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > MAX_BODY_BYTES:
            raise PayloadTooLarge(MAX_BODY_BYTES)
    return bytes(body)


def require_admin_key(request: Request) -> None:
    """FR-SEC-1: ``X-API-Key`` must match, compared in constant time.

    Runs before the endpoint's own parameters are validated, so a caller without the
    key never learns anything about the admin API (not even its validation errors).
    The key is never logged.
    """
    expected: bytes = request.app.state.admin_api_key
    provided = request.headers.get(ADMIN_KEY_HEADER)
    if provided is not None and hmac.compare_digest(provided.encode("utf-8"), expected):
        return
    _log.warning(
        "admin_auth_failed",
        trace_id=getattr(request.state, "trace_id", None),
        reason="missing" if provided is None else "mismatch",
        path=request.url.path,
    )
    raise AdminAuthError()


def get_ingest_event(request: Request) -> IngestEvent:
    ingest: IngestEvent = request.app.state.ingest_event
    return ingest


def get_inspect_events(request: Request) -> InspectEvents:
    inspect: InspectEvents = request.app.state.inspect_events
    return inspect


def get_replay_event(request: Request) -> ReplayEvent:
    replay: ReplayEvent = request.app.state.replay_event
    return replay
