"""Admin authentication and request validation (FR-SEC-1). No database needed."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from tests.fakes import FixedClock, InMemoryCache, InMemoryEvents, RecordingPublisher

from hookgate.api.main import create_app
from hookgate.application.ports import AdminEvents
from hookgate.application.records import (
    AttemptRecord,
    EventQuery,
    PageKey,
    StoredEvent,
    TransitionRecord,
)
from hookgate.config import Settings
from hookgate.domain.outcomes import EventOutcome
from hookgate.domain.states import TransitionResult

ADMIN_KEY = "unit-admin-key-0123456789"
NOW = datetime(2026, 11, 3, 12, 0, 0, tzinfo=UTC)


class EmptyAdminEvents:
    """An admin store with no events; records the queries it receives."""

    def __init__(self) -> None:
        self.queries: list[tuple[EventQuery, int, PageKey | None]] = []

    def list_events(
        self, query: EventQuery, *, limit: int, after: PageKey | None = None
    ) -> list[StoredEvent]:
        self.queries.append((query, limit, after))
        return []

    def get(self, event_id: UUID) -> StoredEvent | None:
        return None

    def list_transitions(self, event_id: UUID) -> list[TransitionRecord]:
        return []

    def list_attempts(self, event_id: UUID) -> list[AttemptRecord]:
        return []

    def apply_transition(
        self,
        event_id: UUID,
        result: TransitionResult,
        *,
        retry_after: timedelta | None = None,
        last_error: str | None = None,
        outcome: EventOutcome | None = None,
        expected_attempts: int | None = None,
    ) -> bool:
        return False

    @contextmanager
    def scope(self) -> Iterator[AdminEvents]:
        yield self


@pytest.fixture
def admin_store() -> EmptyAdminEvents:
    return EmptyAdminEvents()


@pytest.fixture
def client(admin_store: EmptyAdminEvents) -> Iterator[TestClient]:
    settings = Settings(
        database_url="postgresql+psycopg://unused:unused@127.0.0.1:5432/unused",
        redis_url="redis://127.0.0.1:6379/0",
        broker_url="amqp://unused:unused@127.0.0.1:5672//",
        webhook_secret=SecretStr("unit-webhook-secret"),
        admin_api_key=SecretStr(ADMIN_KEY),
    )
    app = create_app(
        settings,
        events=InMemoryEvents().scope,
        admin_events=admin_store.scope,
        clock=FixedClock(NOW),
        publisher=RecordingPublisher(),
        cache=InMemoryCache(),
    )
    with TestClient(app) as test_client:
        yield test_client


ROUTES = [
    ("GET", "/admin/events"),
    ("GET", f"/admin/events/{uuid4()}"),
    ("POST", f"/admin/events/{uuid4()}/replay"),
]


@pytest.mark.parametrize(("method", "url"), ROUTES)
@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}, {"X-API-Key": ""}])
def test_every_admin_route_requires_the_key(
    client: TestClient, method: str, url: str, headers: dict[str, str]
) -> None:
    response = client.request(method, url, headers=headers)

    assert response.status_code == 401
    assert response.json() == {"error": "invalid_api_key"}


def test_auth_is_checked_before_parameters_are_validated(client: TestClient) -> None:
    response = client.get("/admin/events?limit=0&status=bogus", headers={"X-API-Key": "wrong"})

    assert response.status_code == 401


def test_a_key_that_only_shares_a_prefix_is_rejected(client: TestClient) -> None:
    for candidate in (ADMIN_KEY[:-1], ADMIN_KEY + "x", ADMIN_KEY.upper()):
        response = client.get("/admin/events", headers={"X-API-Key": candidate})
        assert response.status_code == 401


def test_the_key_is_never_logged(client: TestClient, capsys: pytest.CaptureFixture[str]) -> None:
    client.get("/admin/events", headers={"X-API-Key": "leaked-wrong-key"})
    client.get("/admin/events", headers={"X-API-Key": ADMIN_KEY})

    output = capsys.readouterr()
    assert "leaked-wrong-key" not in output.out + output.err
    assert ADMIN_KEY not in output.out + output.err
    assert "admin_auth_failed" in output.out + output.err


def test_valid_key_lists_an_empty_page(client: TestClient) -> None:
    response = client.get("/admin/events", headers={"X-API-Key": ADMIN_KEY})

    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}


def test_filters_are_passed_to_the_store_and_repeated_statuses_are_merged(
    client: TestClient, admin_store: EmptyAdminEvents
) -> None:
    response = client.get(
        "/admin/events?status=dead&status=failed&status=dead"
        "&resource_id=pay_1&event_type=payment.failed&limit=10",
        headers={"X-API-Key": ADMIN_KEY},
    )

    assert response.status_code == 200
    query, limit, after = admin_store.queries[-1]
    assert [s.value for s in query.statuses] == ["dead", "failed"]
    assert (query.resource_id, query.event_type) == ("pay_1", "payment.failed")
    assert (limit, after) == (11, None)  # one extra row to detect the next page


@pytest.mark.parametrize(
    "query",
    ["limit=0", "limit=201", "status=bogus", "limit=abc"],
)
def test_invalid_parameters_are_422_once_authenticated(client: TestClient, query: str) -> None:
    response = client.get(f"/admin/events?{query}", headers={"X-API-Key": ADMIN_KEY})

    assert response.status_code == 422


def test_invalid_cursor_is_400(client: TestClient) -> None:
    response = client.get("/admin/events?cursor=garbage", headers={"X-API-Key": ADMIN_KEY})

    assert response.status_code == 400
    assert response.json() == {"error": "invalid_cursor"}


def test_unknown_event_is_404_for_detail_and_replay(client: TestClient) -> None:
    headers = {"X-API-Key": ADMIN_KEY}
    event_uuid = uuid4()

    detail = client.get(f"/admin/events/{event_uuid}", headers=headers)
    replay = client.post(f"/admin/events/{event_uuid}/replay", headers=headers)

    assert (detail.status_code, detail.json()) == (404, {"error": "event_not_found"})
    assert (replay.status_code, replay.json()) == (404, {"error": "event_not_found"})


def test_malformed_event_id_is_422(client: TestClient) -> None:
    response = client.get("/admin/events/not-a-uuid", headers={"X-API-Key": ADMIN_KEY})

    assert response.status_code == 422
