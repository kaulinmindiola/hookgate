"""Ingest one signed webhook delivery (FR-ING-1..7, FR-IDE-1..6, SDD 6.2, 10 and 11).

Order: signature and timestamp, minimal envelope, Redis marker (fast path), then the
PostgreSQL insert, which decides. The raw bytes are stored exactly as received and the
transaction commits before the marker is written or anything is published, so an
acknowledged event is always durable (ADR-001) and a marker never exists for an event
that was not stored (ADR-003). Any cache failure degrades to PostgreSQL (FR-IDE-4).
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated
from uuid import UUID

import structlog
from pydantic import AwareDatetime, BaseModel, ConfigDict, StringConstraints, ValidationError
from structlog.typing import FilteringBoundLogger

from hookgate.application.metrics import NULL_METRICS
from hookgate.application.ports import (
    CacheOp,
    Clock,
    DedupeCache,
    DuplicateLayer,
    EventPublisher,
    EventStoreScope,
    IngestionMetrics,
)
from hookgate.application.records import NewEvent
from hookgate.domain.errors import SignatureError
from hookgate.domain.events import EventIdentity
from hookgate.domain.signature import verify_signature

DEFAULT_PROVIDER = "sim"

# Headers kept with the event for audit (lowercase). Anything else is dropped.
STORED_HEADERS: frozenset[str] = frozenset(
    {"content-type", "user-agent", "hookgate-signature", "x-trace-id"}
)

_log = structlog.get_logger(__name__)

NonEmptyStr = Annotated[str, StringConstraints(min_length=1)]


class InvalidPayload(Exception):
    """The body is not JSON or lacks a field needed for identity or ordering (400)."""

    def __init__(self, problems: Sequence[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = tuple(problems)


class ResourceRef(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="ignore")

    type: NonEmptyStr
    id: NonEmptyStr


class WebhookEnvelope(BaseModel):
    """Only identity and ordering fields; ``data`` is validated later by the handler."""

    model_config = ConfigDict(strict=True, frozen=True, extra="ignore")

    id: NonEmptyStr
    type: NonEmptyStr
    occurred_at: AwareDatetime
    resource: ResourceRef


def parse_envelope(raw_body: bytes) -> WebhookEnvelope:
    try:
        return WebhookEnvelope.model_validate_json(raw_body)
    except ValidationError as exc:
        raise InvalidPayload(_describe(exc)) from None


def _describe(exc: ValidationError) -> list[str]:
    """Field path and message only: the offending input is never echoed back."""
    problems = []
    for error in exc.errors(include_url=False, include_input=False, include_context=False):
        location = ".".join(str(part) for part in error["loc"]) or "body"
        problems.append(f"{location}: {error['msg']}")
    return problems


class IngestStatus(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"


@dataclass(frozen=True, slots=True)
class IngestCommand:
    raw_body: bytes
    signature_header: str | None
    headers: Mapping[str, str]
    trace_id: str


@dataclass(frozen=True, slots=True)
class IngestResult:
    status: IngestStatus
    event_id: str
    stored_id: UUID | None  # None for duplicates


class IngestEvent:
    def __init__(
        self,
        *,
        secret: str,
        tolerance_s: int,
        clock: Clock,
        events: EventStoreScope,
        publisher: EventPublisher,
        cache: DedupeCache | None = None,
        metrics: IngestionMetrics | None = None,
        provider: str = DEFAULT_PROVIDER,
    ) -> None:
        self._secret = secret
        self._tolerance_s = tolerance_s
        self._clock = clock
        self._events = events
        self._publisher = publisher
        self._cache = cache
        self._metrics = metrics or NULL_METRICS
        self._provider = provider

    def __call__(self, command: IngestCommand) -> IngestResult:
        log = _log.bind(trace_id=command.trace_id, provider=self._provider)
        now = self._clock.now()

        try:
            verify_signature(
                command.signature_header,
                command.raw_body,
                self._secret,
                now=now,
                tolerance_s=self._tolerance_s,
            )
        except SignatureError as exc:
            self._metrics.received("rejected")
            log.warning("webhook_rejected", reason=exc.reason.value)
            raise

        try:
            envelope = parse_envelope(command.raw_body)
        except InvalidPayload as exc:
            self._metrics.received("rejected")
            log.warning("webhook_rejected", reason="invalid_payload", problems=list(exc.problems))
            raise

        log = log.bind(event_id=envelope.id, resource_id=envelope.resource.id)
        new = NewEvent(
            identity=EventIdentity(self._provider, envelope.id),
            event_type=envelope.type,
            resource_type=envelope.resource.type,
            resource_id=envelope.resource.id,
            occurred_at=envelope.occurred_at,
            received_at=now,
            raw_body=command.raw_body,
            headers={k: v for k, v in command.headers.items() if k.lower() in STORED_HEADERS},
            trace_id=command.trace_id,
        )

        if self._marker_exists(new.identity, log):
            # Known event: skip the database. A changed body is not detected on this
            # path (FR-IDE-6 is checked only when PostgreSQL detects the duplicate).
            log.info("webhook_duplicate", layer="redis")
            self._count_duplicate("redis")
            return IngestResult(IngestStatus.DUPLICATE, envelope.id, None)

        original_body: bytes | None = None
        with self._events() as store:
            stored_id = store.insert_if_new(new)
            if stored_id is None:
                original_body = store.find_raw_body(new.identity)
        # Committed: from here on the event is durable.

        if stored_id is None:
            if original_body is not None and original_body != command.raw_body:
                log.warning("payload_mismatch", layer="postgres")
            log.info("webhook_duplicate", layer="postgres")
            self._count_duplicate("postgres")
            self._write_marker(new.identity, log)  # the row exists: safe to re-mark
            return IngestResult(IngestStatus.DUPLICATE, envelope.id, None)

        log.info("webhook_accepted", status="received")
        self._metrics.received("accepted")
        self._write_marker(new.identity, log)
        try:
            self._publisher.publish(stored_id, trace_id=command.trace_id)
        except Exception as exc:
            # FR-ING-7: the provider still gets 202; the sweeper recovers the event.
            log.warning("publish_failed", error=type(exc).__name__)
        return IngestResult(IngestStatus.ACCEPTED, envelope.id, stored_id)

    def _marker_exists(self, identity: EventIdentity, log: FilteringBoundLogger) -> bool:
        if self._cache is None:
            return False
        try:
            return self._cache.seen(identity)
        except Exception as exc:
            self._cache_failed("get", exc, log)
            return False

    def _write_marker(self, identity: EventIdentity, log: FilteringBoundLogger) -> None:
        if self._cache is None:
            return
        try:
            self._cache.remember(identity)
        except Exception as exc:
            # Best effort: a missing marker only costs one database round trip later.
            self._cache_failed("set", exc, log)

    def _count_duplicate(self, layer: DuplicateLayer) -> None:
        self._metrics.received("duplicate")
        self._metrics.duplicate(layer)

    def _cache_failed(self, op: CacheOp, exc: Exception, log: FilteringBoundLogger) -> None:
        self._metrics.cache_error(op)
        log.warning("cache_error", op=op, error=type(exc).__name__)
