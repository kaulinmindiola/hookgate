"""Liveness probe. Readiness checks (db, broker, redis) arrive in phase F9."""

from fastapi import APIRouter

from hookgate.api.schemas import HealthBody

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> HealthBody:
    return HealthBody(status="ok")
