"""Admin API: list, inspect and replay events (FR-DLQ-1..4, FR-SEC-1, SDD 14).

Every route requires ``X-API-Key`` (router-level dependency). Plain ``def`` endpoints:
FastAPI runs them in a threadpool, so the synchronous database work does not block
the event loop (ADR-005).
"""

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import JSONResponse

from hookgate.api.cursor import decode_cursor, encode_cursor
from hookgate.api.deps import (
    get_inspect_events,
    get_replay_event,
    get_trace_id,
    require_admin_key,
)
from hookgate.api.schemas import (
    ErrorBody,
    EventDetailBody,
    EventPageBody,
    EventSummary,
    ReplayAck,
)
from hookgate.application.inspect_events import MAX_PAGE_SIZE, InspectEvents
from hookgate.application.records import EventQuery
from hookgate.application.replay_event import ReplayEvent, ReplayOutcome
from hookgate.domain.states import EventStatus

DEFAULT_PAGE_SIZE = 50
_UNAUTHORIZED: dict[int | str, dict[str, Any]] = {
    401: {"model": ErrorBody, "description": "Missing or wrong X-API-Key"}
}
_NOT_FOUND: dict[int | str, dict[str, Any]] = {
    404: {"model": ErrorBody, "description": "No event with this id"}
}

router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(require_admin_key)],
    responses=_UNAUTHORIZED,
)


def _error(status_code: int, error: str, detail: list[str] | None = None) -> JSONResponse:
    body = ErrorBody(error=error, detail=detail)
    return JSONResponse(status_code=status_code, content=body.model_dump(exclude_none=True))


@router.get(
    "/events",
    responses={400: {"model": ErrorBody, "description": "Cursor not produced by this API"}},
)
def list_events(
    inspect: Annotated[InspectEvents, Depends(get_inspect_events)],
    status_filter: Annotated[
        list[EventStatus] | None,
        Query(alias="status", description="Repeat to match several statuses."),
    ] = None,
    resource_id: Annotated[str | None, Query(max_length=200)] = None,
    event_type: Annotated[str | None, Query(max_length=200)] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    cursor: Annotated[str | None, Query(max_length=500)] = None,
) -> EventPageBody:
    """Events ordered by ``(received_at, id)``; pass ``next_cursor`` to get the next page."""
    query = EventQuery(
        statuses=tuple(dict.fromkeys(status_filter or ())),
        resource_id=resource_id,
        event_type=event_type,
    )
    after = None if cursor is None else decode_cursor(cursor)
    page = inspect.page(query, limit=limit, after=after)
    return EventPageBody(
        items=[EventSummary.from_stored(event) for event in page.items],
        next_cursor=None if page.next_key is None else encode_cursor(page.next_key),
    )


@router.get("/events/{event_uuid}", response_model=EventDetailBody, responses=_NOT_FOUND)
def get_event(
    event_uuid: UUID,
    inspect: Annotated[InspectEvents, Depends(get_inspect_events)],
) -> EventDetailBody | JSONResponse:
    """Stored raw payload, state, last error, transitions and attempts."""
    detail = inspect.detail(event_uuid)
    if detail is None:
        return _error(404, "event_not_found")
    return EventDetailBody.build(detail.event, detail.transitions, detail.attempts)


@router.post(
    "/events/{event_uuid}/replay",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ReplayAck,
    responses={
        **_NOT_FOUND,
        409: {"model": ErrorBody, "description": "The event is not dead"},
    },
)
def replay_event(
    event_uuid: UUID,
    replay: Annotated[ReplayEvent, Depends(get_replay_event)],
    trace_id: Annotated[str, Depends(get_trace_id)],
) -> ReplayAck | JSONResponse:
    """Move a dead event back to ``received`` and enqueue it (history is kept)."""
    result = replay(event_uuid, request_trace_id=trace_id)
    if result.outcome is ReplayOutcome.NOT_FOUND:
        return _error(404, "event_not_found")
    if result.outcome is ReplayOutcome.NOT_DEAD:
        assert result.status is not None
        return _error(409, "event_not_dead", [f"status: {result.status.value}"])
    assert result.status is not None
    return ReplayAck(status=result.status, enqueued=result.enqueued)
