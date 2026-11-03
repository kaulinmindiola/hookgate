"""Request-level dependencies: raw body with a size limit, trace id, use cases."""

import re
from uuid import uuid4

from fastapi import Request

from hookgate.api.errors import PayloadTooLarge
from hookgate.application.ingest_event import IngestEvent

MAX_BODY_BYTES = 1024 * 1024  # 1 MiB (FR-ING-4)
TRACE_HEADER = "X-Trace-Id"
_VALID_TRACE_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")


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


def get_ingest_event(request: Request) -> IngestEvent:
    ingest: IngestEvent = request.app.state.ingest_event
    return ingest
