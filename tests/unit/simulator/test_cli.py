"""Command line and HTTP clients against an in-process fake Hookgate (no server needed)."""

import json
from datetime import UTC, datetime

import httpx
import pytest

from hookgate.domain.errors import SignatureError
from hookgate.domain.signature import verify_signature
from simulator.__main__ import main
from simulator.client import AdminClient, ProviderClient, envelope, signature_header
from simulator.model import Signing
from simulator.scenarios import build_plan

SECRET = "cli-secret"
NOW = 1_800_000_000


class FakeHookgate:
    """Just enough of Hookgate's HTTP contract for the simulator: idempotent ingestion,
    signature check, and a paginated admin list where every event is already final."""

    def __init__(self) -> None:
        self.events: dict[str, dict[str, object]] = {}
        self.payments: dict[str, dict[str, object]] = {}
        self.latest_keys: dict[str, tuple[str, str]] = {}
        self.calls = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        if request.url.path == "/webhooks/payments":
            return self._ingest(request)
        if request.url.path.startswith("/admin/payments/"):
            payment = self.payments.get(request.url.path.rsplit("/", 1)[1])
            if payment is None:
                return httpx.Response(404, json={"error": "payment_not_found"})
            return httpx.Response(200, json=payment)
        return self._list(request)

    def _ingest(self, request: httpx.Request) -> httpx.Response:
        body = request.content
        t_part, v_part = request.headers["Hookgate-Signature"].split(",")
        timestamp = int(t_part.removeprefix("t="))
        if signature_header(SECRET, timestamp, body) != f"{t_part},{v_part}":
            return httpx.Response(401, json={"error": "invalid_signature"})
        if abs(NOW - timestamp) > 300:
            return httpx.Response(401, json={"error": "timestamp_out_of_tolerance"})
        doc = json.loads(body)
        if doc["id"] in self.events:
            return httpx.Response(202, json={"status": "duplicate"})
        dead = doc["type"] == "payment.exploded"
        key = (str(doc["occurred_at"]), str(doc["id"]))
        current = self.latest_keys.get(doc["resource"]["id"])
        newer = current is None or key > current
        self.events[doc["id"]] = {
            "event_id": doc["id"],
            "status": "dead" if dead else "processed",
            "outcome": None if dead else ("applied" if newer else "ignored_stale"),
        }
        if not dead and newer:  # latest-wins, like Hookgate
            self.latest_keys[doc["resource"]["id"]] = key
            self.payments[doc["resource"]["id"]] = {
                "status": doc["type"].removeprefix("payment."),
                "amount": doc["data"]["amount"],
                "currency": doc["data"]["currency"],
                "last_event_id": doc["id"],
            }
        return httpx.Response(202, json={"status": "accepted"})

    def _list(self, request: httpx.Request) -> httpx.Response:
        assert request.headers["X-API-Key"] == "key"
        items = list(self.events.values())
        status = request.url.params.get("status")
        if status:
            items = [i for i in items if i["status"] == status]
        start = int(request.url.params.get("cursor", "0"))
        page = items[start : start + 2]  # tiny pages to exercise the cursor walk
        nxt = str(start + 2) if start + 2 < len(items) else None
        return httpx.Response(200, json={"items": page, "next_cursor": nxt})


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeHookgate:
    hookgate = FakeHookgate()
    real_client = httpx.Client

    def client(**kwargs: object) -> httpx.Client:
        return real_client(transport=httpx.MockTransport(hookgate.handler), **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr("simulator.__main__.httpx.Client", client)
    monkeypatch.setattr("simulator.client.time.time", lambda: NOW)
    monkeypatch.setattr("simulator.runner.time.sleep", lambda _: None)
    return hookgate


def test_envelope_is_the_ingestion_contract() -> None:
    (delivery,) = build_plan("burst", 1, "r").deliveries
    doc = json.loads(envelope(delivery.event))

    assert set(doc) == {"id", "type", "occurred_at", "resource", "data"}
    assert doc["occurred_at"].endswith("Z")
    assert doc["resource"]["type"] == "payment"
    assert doc["data"]["currency"] == "USD"


def test_independent_signer_is_accepted_by_hookgate_verifier() -> None:
    """The simulator signs on its own; Hookgate's verifier must accept it (and only it)."""
    body = envelope(build_plan("happy", 1, "r").deliveries[0].event)
    now = datetime.fromtimestamp(NOW, tz=UTC)

    verify_signature(signature_header(SECRET, NOW, body), body, SECRET, now=now, tolerance_s=300)
    with pytest.raises(SignatureError):
        verify_signature(signature_header("x", NOW, body), body, SECRET, now=now, tolerance_s=300)


def test_run_chaos_passes_against_a_correct_fake(
    fake: FakeHookgate, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(
        [
            "run",
            "--scenario",
            "chaos",
            "--seed",
            "7",
            "--run-id",
            "cli",
            "--url",
            "http://hookgate",
            "--secret",
            SECRET,
            "--api-key",
            "key",
        ]
    )

    out = capsys.readouterr().out
    assert code == 0, out
    assert "OK (python -m simulator verify --scenario chaos --seed 7 --run-id cli)" in out


def test_verify_fails_when_the_system_lost_an_event(
    fake: FakeHookgate, capsys: pytest.CaptureFixture[str]
) -> None:
    args = [
        "--scenario",
        "happy",
        "--seed",
        "3",
        "--run-id",
        "lost",
        "--url",
        "http://hookgate",
        "--secret",
        SECRET,
        "--api-key",
        "key",
    ]
    assert main(["run", "--no-verify", *args]) == 0
    fake.events.pop(next(iter(fake.events)))

    code = main(["verify", "--timeout", "0", *args])

    out = capsys.readouterr().out
    assert code == 1
    assert "not stored" in out


def test_a_wrong_secret_makes_every_delivery_fail(
    fake: FakeHookgate, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(
        [
            "run",
            "--scenario",
            "happy",
            "--seed",
            "1",
            "--run-id",
            "bad",
            "--timeout",
            "0",
            "--url",
            "http://hookgate",
            "--secret",
            "wrong",
            "--api-key",
            "key",
        ]
    )

    assert code == 1
    assert "expected 1 accepted" in capsys.readouterr().out


def test_concurrent_copies_all_get_an_answer() -> None:
    hookgate = FakeHookgate()
    (delivery,) = build_plan("burst", 1, "r").deliveries
    with httpx.Client(transport=httpx.MockTransport(hookgate.handler), base_url="http://h") as http:
        responses = ProviderClient(http, secret=SECRET, now=lambda: NOW).deliver(delivery)

    assert sorted(r.result for r in responses) == ["accepted"] + ["duplicate"] * 31


def test_expired_signing_is_far_outside_the_window() -> None:
    hookgate = FakeHookgate()
    (first, _) = build_plan("expired", 1, "r").deliveries
    with httpx.Client(transport=httpx.MockTransport(hookgate.handler), base_url="http://h") as http:
        (response,) = ProviderClient(http, secret=SECRET, now=lambda: NOW).deliver(first)

    assert (first.signing, response.status_code, response.result) == (
        Signing.EXPIRED,
        401,
        "timestamp_out_of_tolerance",
    )


def test_admin_client_walks_every_page_and_filters_by_prefix() -> None:
    hookgate = FakeHookgate()
    for n in range(5):
        hookgate.events[f"mine-{n}"] = {
            "event_id": f"mine-{n}",
            "status": "processed",
            "outcome": "applied",
        }
    hookgate.events["other-1"] = {"event_id": "other-1", "status": "dead", "outcome": None}
    with httpx.Client(transport=httpx.MockTransport(hookgate.handler), base_url="http://h") as http:
        found = AdminClient(http, api_key="key").events(prefix="mine-")

    assert sorted(found) == [f"mine-{n}" for n in range(5)]


def test_list_prints_every_scenario(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["list"]) == 0
    assert capsys.readouterr().out.split() == [
        "happy",
        "duplicates",
        "delayed",
        "out_of_order",
        "burst",
        "bad_signature",
        "expired",
        "poison",
        "chaos",
    ]


def test_invalid_run_id_is_a_usage_error(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", "--scenario", "happy", "--seed", "1", "--run-id", "a-b"]) == 2
