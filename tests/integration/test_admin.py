"""Admin API against a real PostgreSQL (FR-DLQ-1..4; VR-6 complete, VR-10).

Time is moved by back-dating columns (decision P3). Processing runs the real
``process_event`` use case synchronously; the broker is replaced by a recording fake.
"""

import base64
import json
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker
from tests.fakes import FixedClock, FixedRng, InMemoryCache, RecordingPublisher

from hookgate.api.main import create_app
from hookgate.application.process_event import ProcessEvent, ProcessOutcome
from hookgate.application.records import NewEvent, StoredEvent
from hookgate.application.replay_event import ReplayEvent, ReplayOutcome
from hookgate.application.sweep import SweepEvents
from hookgate.config import Settings
from hookgate.domain.events import EventIdentity
from hookgate.domain.retry import RetryPolicy
from hookgate.domain.signature import build_signature_header
from hookgate.domain.states import EventStatus
from hookgate.infrastructure.db.engine import session_scope
from hookgate.infrastructure.db.repositories import EventRepository
from hookgate.infrastructure.db.transactions import admin_scope, processing_scope, sweep_scope

pytestmark = pytest.mark.integration

Factory = sessionmaker[Session]
S = EventStatus
ADMIN_KEY = "integration-admin-key"
SECRET = "integration-test-secret"
AUTH = {"X-API-Key": ADMIN_KEY}
POLICY = RetryPolicy(max_attempts=3, base_s=2.0, cap_s=300.0)


def body(event_id: str, event_type: str = "payment.succeeded", resource_id: str = "") -> bytes:
    return json.dumps(
        {
            "id": event_id,
            "type": event_type,
            "occurred_at": "2026-11-03T11:59:00Z",
            "resource": {"type": "payment", "id": resource_id or f"pay_{event_id}"},
            "data": {"amount": 1500, "currency": "USD"},
        }
    ).encode()


def insert_event(
    factory: Factory,
    event_id: str,
    *,
    event_type: str = "payment.succeeded",
    resource_id: str = "",
) -> UUID:
    resource = resource_id or f"pay_{event_id}"
    new = NewEvent(
        identity=EventIdentity("sim", event_id),
        event_type=event_type,
        resource_type="payment",
        resource_id=resource,
        occurred_at=datetime(2026, 11, 3, 11, 59, tzinfo=UTC),
        received_at=datetime.now(UTC),
        raw_body=body(event_id, event_type, resource),
        headers={"content-type": "application/json"},
        trace_id=f"trace-{event_id}",
    )
    with session_scope(factory) as session:
        stored_id = EventRepository(session).insert_if_new(new)
    assert stored_id is not None
    return stored_id


def run_sql(factory: Factory, sql: str, event_id: UUID) -> None:
    with session_scope(factory) as session:
        session.execute(text(sql), {"id": event_id})


def set_status(factory: Factory, event_id: UUID, status: str) -> None:
    run_sql(factory, f"UPDATE webhook_events SET status = '{status}' WHERE id = :id", event_id)


def load(factory: Factory, event_id: UUID) -> StoredEvent:
    with session_scope(factory) as session:
        stored = EventRepository(session).get(event_id)
    assert stored is not None
    return stored


@pytest.fixture
def publisher() -> RecordingPublisher:
    return RecordingPublisher()


@pytest.fixture
def client(
    database_url: str, engine: Engine, publisher: RecordingPublisher
) -> Iterator[TestClient]:
    settings = Settings(
        database_url=database_url,
        redis_url="redis://unused",
        broker_url="amqp://unused:unused@127.0.0.1:5672//",
        webhook_secret=SecretStr(SECRET),
        admin_api_key=SecretStr(ADMIN_KEY),
    )
    app = create_app(
        settings,
        engine=engine,
        clock=FixedClock(datetime.now(UTC)),
        cache=InMemoryCache(),
        publisher=publisher,
    )
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def process(session_factory: Factory) -> ProcessEvent:
    return ProcessEvent(
        units=processing_scope(session_factory), lease_s=60, policy=POLICY, rng=FixedRng(1.0)
    )


def ids(response_json: dict[str, object]) -> list[str]:
    items = response_json["items"]
    assert isinstance(items, list)
    return [item["id"] for item in items]


# -- list (FR-DLQ-1) ---------------------------------------------------------------


def test_list_filters_by_status_resource_and_event_type(
    session_factory: Factory, client: TestClient
) -> None:
    dead = insert_event(session_factory, "evt_dead", resource_id="pay_A")
    failed = insert_event(session_factory, "evt_failed", event_type="payment.failed")
    insert_event(session_factory, "evt_received", resource_id="pay_A")
    set_status(session_factory, dead, "dead")
    set_status(session_factory, failed, "failed")

    def get(query: str) -> list[str]:
        response = client.get(f"/admin/events?{query}", headers=AUTH)
        assert response.status_code == 200
        return ids(response.json())

    assert get("status=dead") == [str(dead)]
    assert get("status=dead&status=failed") == [str(dead), str(failed)]
    assert len(get("resource_id=pay_A")) == 2
    assert get("resource_id=pay_A&status=dead") == [str(dead)]
    assert get("event_type=payment.failed") == [str(failed)]
    assert len(get("")) == 3
    assert get("status=processed") == []


def test_pages_cover_every_event_once_with_ties_and_new_arrivals(
    session_factory: Factory, client: TestClient
) -> None:
    originals = [insert_event(session_factory, f"evt_{n}") for n in range(7)]
    # Three events share one received_at: the id breaks the tie (keyset on both columns).
    with session_scope(session_factory) as session:
        session.execute(
            text(
                "UPDATE webhook_events SET received_at = '2026-11-03T10:00:00Z' "
                "WHERE id = ANY(:ids)"
            ),
            {"ids": originals[2:5]},
        )

    seen: list[str] = []
    cursor = None
    pages = 0
    arrived_meanwhile: UUID | None = None
    while True:
        url = "/admin/events?limit=2" + (f"&cursor={cursor}" if cursor else "")
        response = client.get(url, headers=AUTH)
        assert response.status_code == 200
        page = response.json()
        seen.extend(ids(page))
        pages += 1
        if arrived_meanwhile is None:
            arrived_meanwhile = insert_event(session_factory, "evt_late")  # newest received_at
        cursor = page["next_cursor"]
        if cursor is None:
            break

    expected = {str(event_id) for event_id in [*originals, arrived_meanwhile]}
    assert len(seen) == len(set(seen)) == len(expected)  # no duplicates
    assert set(seen) == expected  # no gaps
    assert pages == 4

    with session_scope(session_factory) as session:
        rows = session.execute(text("SELECT id::text FROM webhook_events ORDER BY received_at, id"))
        ordered = [str(row[0]) for row in rows]
    assert seen == ordered


def test_last_page_of_an_exact_multiple_has_no_cursor(
    session_factory: Factory, client: TestClient
) -> None:
    for n in range(4):
        insert_event(session_factory, f"evt_{n}")

    first = client.get("/admin/events?limit=2", headers=AUTH).json()
    second = client.get(f"/admin/events?limit=2&cursor={first['next_cursor']}", headers=AUTH)

    assert first["next_cursor"] is not None
    assert len(ids(second.json())) == 2
    assert second.json()["next_cursor"] is None


# -- detail (FR-DLQ-2) -------------------------------------------------------------


def test_detail_returns_raw_payload_error_transitions_and_attempts(
    session_factory: Factory, client: TestClient, process: ProcessEvent
) -> None:
    event_id = insert_event(session_factory, "evt_poison", event_type="payment.exploded")
    assert process(event_id).outcome is ProcessOutcome.DEAD

    response = client.get(f"/admin/events/{event_id}", headers=AUTH)

    assert response.status_code == 200
    detail = response.json()
    assert (detail["status"], detail["attempts"], detail["event_id"]) == ("dead", 1, "evt_poison")
    assert "payment.exploded" in detail["last_error"]
    raw = body("evt_poison", "payment.exploded", "pay_evt_poison")
    assert base64.b64decode(detail["raw_body_base64"]) == raw
    assert detail["raw_body"] == raw.decode()
    assert detail["headers"] == {"content-type": "application/json"}
    assert [t["reason"] for t in detail["transitions"]] == [
        "ingested",
        "claim",
        "permanent_error",
    ]
    assert [(a["attempt_no"], a["result"]) for a in detail["attempts_history"]] == [(1, "dead")]


# -- replay (FR-DLQ-3, VR-6 complete, VR-10) ---------------------------------------


def test_poison_payload_ends_dead_is_listed_and_replays_back_to_dead_keeping_history(
    session_factory: Factory,
    client: TestClient,
    publisher: RecordingPublisher,
    process: ProcessEvent,
) -> None:
    """VR-6 end to end: signed ingestion, dead, listed, replayed, dead again."""
    raw = body("evt_vr6", "payment.exploded")
    accepted = client.post(
        "/webhooks/payments",
        content=raw,
        headers={
            "Content-Type": "application/json",
            "Hookgate-Signature": build_signature_header(SECRET, int(time.time()), raw),
            "X-Trace-Id": "trace-vr6",
        },
    )
    assert accepted.status_code == 202
    (event_id, _) = publisher.published[0]
    assert process(event_id).outcome is ProcessOutcome.DEAD

    listed = client.get("/admin/events?status=dead", headers=AUTH).json()
    assert ids(listed) == [str(event_id)]

    replayed = client.post(f"/admin/events/{event_id}/replay", headers=AUTH)
    assert replayed.status_code == 202
    assert replayed.json() == {"status": "received", "enqueued": True}
    assert publisher.published[-1] == (event_id, "trace-vr6")  # the event's own trace id
    stored = load(session_factory, event_id)
    assert (stored.status, stored.attempts, stored.next_attempt_at, stored.locked_until) == (
        S.RECEIVED,
        0,
        None,
        None,
    )
    assert stored.last_error is not None  # history is kept

    assert process(event_id).outcome is ProcessOutcome.DEAD  # still invalid: dead again
    detail = client.get(f"/admin/events/{event_id}", headers=AUTH).json()
    assert (detail["status"], detail["attempts"]) == ("dead", 1)
    assert [(t["from_status"], t["to_status"], t["reason"]) for t in detail["transitions"]] == [
        (None, "received", "ingested"),
        ("received", "processing", "claim"),
        ("processing", "dead", "permanent_error"),
        ("dead", "received", "operator_replay"),
        ("received", "processing", "claim"),
        ("processing", "dead", "permanent_error"),
    ]
    assert all(t["trace_id"] == "trace-vr6" for t in detail["transitions"])
    assert [(a["attempt_no"], a["result"]) for a in detail["attempts_history"]] == [
        (1, "dead"),
        (1, "dead"),
    ]


@pytest.mark.parametrize("status", ["received", "processing", "processed", "failed"])
def test_replay_of_an_event_that_is_not_dead_is_409_and_changes_nothing(
    session_factory: Factory, client: TestClient, publisher: RecordingPublisher, status: str
) -> None:
    event_id = insert_event(session_factory, "evt_alive")
    set_status(session_factory, event_id, status)

    response = client.post(f"/admin/events/{event_id}/replay", headers=AUTH)

    assert response.status_code == 409
    assert response.json() == {"error": "event_not_dead", "detail": [f"status: {status}"]}
    assert load(session_factory, event_id).status.value == status
    assert publisher.published == []
    with session_scope(session_factory) as session:
        reasons = [t.reason for t in EventRepository(session).list_transitions(event_id)]
    assert reasons == ["ingested"]


def test_second_replay_is_409(
    session_factory: Factory, client: TestClient, publisher: RecordingPublisher
) -> None:
    event_id = insert_event(session_factory, "evt_twice")
    set_status(session_factory, event_id, "dead")

    first = client.post(f"/admin/events/{event_id}/replay", headers=AUTH)
    second = client.post(f"/admin/events/{event_id}/replay", headers=AUTH)

    assert (first.status_code, second.status_code) == (202, 409)
    assert len(publisher.published) == 1


def test_concurrent_replays_enqueue_the_event_once(session_factory: Factory) -> None:
    """FR-DLQ-4: N simultaneous replays, one winner, one publish."""
    event_id = insert_event(session_factory, "evt_race")
    set_status(session_factory, event_id, "dead")
    publisher = RecordingPublisher()
    replay = ReplayEvent(units=admin_scope(session_factory), publisher=publisher)
    callers = 8
    barrier = Barrier(callers)

    def call(_: int) -> ReplayOutcome:
        barrier.wait()
        return replay(event_id, request_trace_id="t").outcome

    with ThreadPoolExecutor(max_workers=callers) as pool:
        outcomes = list(pool.map(call, range(callers)))

    assert outcomes.count(ReplayOutcome.REPLAYED) == 1
    assert outcomes.count(ReplayOutcome.NOT_DEAD) == callers - 1
    assert publisher.published == [(event_id, "trace-evt_race")]
    with session_scope(session_factory) as session:
        reasons = [t.reason for t in EventRepository(session).list_transitions(event_id)]
    assert reasons.count("operator_replay") == 1


def test_replayed_event_is_claimable_even_if_it_had_a_future_retry_time(
    session_factory: Factory, client: TestClient, process: ProcessEvent
) -> None:
    """Regression (plan finding 1): replay clears next_attempt_at, so the claim succeeds."""
    event_id = insert_event(session_factory, "evt_future")
    run_sql(
        session_factory,
        "UPDATE webhook_events SET status = 'dead', attempts = 3, "
        "next_attempt_at = now() + interval '1 day' WHERE id = :id",
        event_id,
    )

    assert client.post(f"/admin/events/{event_id}/replay", headers=AUTH).status_code == 202
    result = process(event_id)

    assert result.outcome is ProcessOutcome.PROCESSED
    assert result.attempt == 1


def test_replay_with_the_broker_down_still_succeeds_and_the_sweeper_enqueues_it(
    session_factory: Factory, client: TestClient, publisher: RecordingPublisher
) -> None:
    event_id = insert_event(session_factory, "evt_broker_down")
    set_status(session_factory, event_id, "dead")
    publisher.fail = True

    response = client.post(f"/admin/events/{event_id}/replay", headers=AUTH)

    assert response.status_code == 202
    assert response.json() == {"status": "received", "enqueued": False}
    run_sql(
        session_factory,
        "UPDATE webhook_events SET received_at = now() - interval '5 minutes' WHERE id = :id",
        event_id,
    )
    sweeper_publisher = RecordingPublisher()
    sweep = SweepEvents(
        units=sweep_scope(session_factory),
        publisher=sweeper_publisher,
        policy=POLICY,
        rng=FixedRng(1.0),
    )
    assert sweep().published == 1
    assert sweeper_publisher.published == [(event_id, "trace-evt_broker_down")]
