"""Prometheus exposition endpoint (FR-OBS-3). Not authenticated, like most scrapers expect;
it exposes counts only, never payloads, identifiers or secrets."""

from fastapi import APIRouter, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest

router = APIRouter(tags=["metrics"])


@router.get("/metrics", response_class=Response)
def metrics(request: Request) -> Response:
    registry: CollectorRegistry = request.app.state.metrics_registry
    return Response(content=generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
