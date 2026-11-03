"""Response bodies of the HTTP API."""

from pydantic import BaseModel

from hookgate.application.ingest_event import IngestStatus


class WebhookAck(BaseModel):
    status: IngestStatus


class ErrorBody(BaseModel):
    error: str
    detail: list[str] | None = None


class HealthBody(BaseModel):
    status: str
