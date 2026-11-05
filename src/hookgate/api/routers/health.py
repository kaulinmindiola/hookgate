"""Liveness and readiness probes (FR-OBS-2, SDD 15.4).

``/health/live`` answers 200 whenever the process can serve a request (restart probe).
``/health`` is the readiness report: 200 when ``ok`` or ``degraded`` (Redis or the
broker down: ingestion still works), 503 when ``down`` (the database is down).
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from hookgate.api.schemas import HealthBody, ReadinessBody
from hookgate.application.health import HealthStatus, Readiness

router = APIRouter(tags=["health"])


def get_readiness(request: Request) -> Readiness:
    readiness: Readiness = request.app.state.readiness
    return readiness


@router.get("/health/live")
def liveness() -> HealthBody:
    return HealthBody(status="ok")


@router.get(
    "/health",
    response_model=ReadinessBody,
    responses={503: {"model": ReadinessBody, "description": "The database is down"}},
)
def readiness(readiness: Annotated[Readiness, Depends(get_readiness)]) -> JSONResponse:
    report = readiness()
    body = ReadinessBody(status=report.status, checks=report.checks)
    status_code = 503 if report.status is HealthStatus.DOWN else 200
    return JSONResponse(status_code=status_code, content=body.model_dump(mode="json"))
