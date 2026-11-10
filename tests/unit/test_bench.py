"""The benchmark script (NFR-2, NFR-3): statistics, signed deliveries, waits and the report.

A fake Hookgate (``httpx.MockTransport``) checks the signature like the real one, pages
the admin listing and reports health, so the whole measurement runs without services.
"""

import hashlib
import hmac
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from scripts.bench import (
    Admin,
    BenchError,
    Latency,
    Provider,
    Report,
    Throughput,
    _measure,
    _parser,
    measure_latency,
    measure_throughput,
    percentile,
)

SECRET = "bench-test-secret"
API_KEY = "bench-test-key"


class FakeHookgate:
    """Just enough of Hookgate: HMAC check, stored events, paged admin list, health."""

    def __init__(self, *, final_status: str = "processed", page_size: int = 3) -> None:
        self.statuses: dict[str, str] = {}
        self.final_status = final_status
        self.page_size = page_size
        self.health = "ok"
        self.health_flag: Path | None = None  # when set: degraded while the file exists
        self.listing_calls = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/webhooks/payments":
            return self._ingest(request)
        if request.url.path == "/health":
            degraded = self.health_flag is not None and self.health_flag.exists()
            return httpx.Response(200, json={"status": "degraded" if degraded else self.health})
        if request.url.path == "/admin/events":
            return self._list(request)
        return httpx.Response(404)

    def _ingest(self, request: httpx.Request) -> httpx.Response:
        body = request.content
        fields = dict(
            part.split("=", 1) for part in request.headers["Hookgate-Signature"].split(",")
        )
        mac = hmac.new(SECRET.encode(), f"{fields['t']}.".encode() + body, hashlib.sha256)
        if not hmac.compare_digest(mac.hexdigest(), fields["v1"]):
            return httpx.Response(401, json={"error": "invalid_signature"})
        event_id = json.loads(body)["id"]
        if event_id in self.statuses:
            return httpx.Response(202, json={"status": "duplicate"})
        self.statuses[event_id] = "received"
        return httpx.Response(202, json={"status": "accepted"})

    def _list(self, request: httpx.Request) -> httpx.Response:
        assert request.headers["X-API-Key"] == API_KEY
        self.listing_calls += 1
        if self.listing_calls > 1:  # the worker catches up after the first look
            self.statuses = dict.fromkeys(self.statuses, self.final_status)
        wanted = request.url.params.get_list("status")
        matching = sorted(k for k, v in self.statuses.items() if v in wanted)
        start = int(request.url.params.get("cursor", "0"))
        page = matching[start : start + self.page_size]
        end = start + self.page_size
        return httpx.Response(
            200,
            json={
                "items": [{"event_id": k, "status": self.statuses[k]} for k in page],
                "next_cursor": str(end) if end < len(matching) else None,
            },
        )


def client(fake: FakeHookgate) -> httpx.Client:
    return httpx.Client(base_url="http://hookgate.test", transport=httpx.MockTransport(fake))


def test_percentile_is_nearest_rank() -> None:
    values = [float(n) for n in range(100, 0, -1)]  # 1..100, unsorted
    assert [percentile(values, q) for q in (50, 95, 99, 100)] == [50.0, 95.0, 99.0, 100.0]
    assert percentile([7.0], 95) == 7.0
    assert percentile([1.0, 2.0, 3.0], 95) == 3.0


@pytest.mark.parametrize("q", [0, -1, 101])
def test_percentile_rejects_out_of_range(q: float) -> None:
    with pytest.raises(ValueError, match="percentile"):
        percentile([1.0], q)


def test_percentile_rejects_no_samples() -> None:
    with pytest.raises(ValueError, match="no samples"):
        percentile([], 50)


def test_latency_is_reported_in_milliseconds_against_its_target() -> None:
    seconds = [0.001 * n for n in range(1, 101)]
    latency = Latency.from_seconds("Redis up", seconds, target_p95_ms=100)
    assert (latency.samples, latency.p50_ms, latency.p95_ms) == (100, 50.0, 95.0)
    assert latency.max_ms == pytest.approx(100.0)
    assert latency.within_target
    assert not Latency.from_seconds("slow", [0.2], target_p95_ms=100).within_target


def test_deliveries_are_signed_new_events() -> None:
    fake = FakeHookgate()
    with client(fake) as http:
        provider = Provider(http, secret=SECRET, run_id="r1")
        samples = measure_latency(provider, "up", requests=5, warmup=2)
    assert len(samples) == 5
    assert all(s >= 0 for s in samples)
    assert len(fake.statuses) == 7
    assert all(k.startswith("bench-r1-up-") for k in fake.statuses)


def test_any_answer_but_accepted_fails_the_measurement() -> None:
    with client(FakeHookgate()) as http:
        provider = Provider(http, secret="not-the-secret", run_id="r1")
        with pytest.raises(BenchError, match="401 invalid_signature"):
            provider.deliver(provider.event("x"))
        provider = Provider(http, secret=SECRET, run_id="r1")
        event = provider.event("twice")
        provider.deliver(event)
        with pytest.raises(BenchError, match="202 duplicate"):
            provider.deliver(event)


def test_admin_walks_every_page_and_keeps_only_the_run() -> None:
    fake = FakeHookgate(page_size=2)
    fake.statuses = {f"bench-r1-load-{n}": "received" for n in range(5)}
    fake.statuses |= {"other-run-1": "received", "bench-r1-load-dead": "dead"}
    with client(fake) as http:
        admin = Admin(http, api_key=API_KEY)
        fake.listing_calls = -10  # keep the statuses as they are
        pending = admin.pending_ids("bench-r1-load-")
    assert pending == {f"bench-r1-load-{n}" for n in range(5)}


def test_throughput_waits_until_nothing_of_the_run_is_pending() -> None:
    fake = FakeHookgate()
    with client(fake) as http:
        provider = Provider(http, secret=SECRET, run_id="r2")
        admin = Admin(http, api_key=API_KEY)
        result = measure_throughput(
            provider, admin, events=20, concurrency=4, timeout_s=5, poll_s=0.01
        )
    assert (result.events, result.concurrency) == (20, 4)
    assert result.end_to_end_s >= result.send_s > 0
    assert set(fake.statuses.values()) == {"processed"}


def test_throughput_fails_when_events_end_dead() -> None:
    fake = FakeHookgate(final_status="dead")
    with client(fake) as http:
        provider = Provider(http, secret=SECRET, run_id="r3")
        admin = Admin(http, api_key=API_KEY)
        with pytest.raises(BenchError, match="3 event"):
            measure_throughput(provider, admin, events=3, concurrency=2, timeout_s=5, poll_s=0.01)


def test_report_shows_targets_and_what_was_not_measured() -> None:
    up = Latency.from_seconds("Redis up", [0.004] * 10, target_p95_ms=100)
    slow = Latency.from_seconds("Redis down", [0.2] * 10, target_p95_ms=150)
    throughput = Throughput(events=1000, concurrency=8, send_s=2.0, end_to_end_s=4.0)
    measured_at = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)

    text = Report("http://h", "abcd", measured_at, up, slow, throughput).markdown()
    assert "indicative" in text
    assert "| Redis up | 10 | 4.0 ms | 4.0 ms | 4.0 ms | 4.0 ms | < 100 ms: met |" in text
    assert "| Redis down | 10 | 200.0 ms" in text
    assert "< 150 ms: **not met**" in text
    assert "| New events | 1000 | 8 | 500 | 250 |" in text

    skipped = Report("http://h", "abcd", measured_at, up, None, throughput).markdown()
    assert "| Redis down | not measured |" in skipped


def test_full_run_stops_redis_and_always_starts_it_again(tmp_path: Path) -> None:
    flag = tmp_path / "redis-down"
    python = f'"{sys.executable}" -c'
    args = _parser().parse_args(
        [
            f"--secret={SECRET}",
            f"--api-key={API_KEY}",
            "--requests=4",
            "--warmup=1",
            "--events=6",
            "--concurrency=2",
            "--timeout=5",
            f"--redis-stop={python} \"import pathlib; pathlib.Path(r'{flag}').touch()\"",
            f"--redis-start={python} \"import pathlib; pathlib.Path(r'{flag}').unlink()\"",
        ]
    )
    fake = FakeHookgate()
    fake.health_flag = flag
    with client(fake) as http:
        report = _measure(args, "r4", http)
    assert report.redis_down is not None
    assert (report.redis_up.samples, report.redis_down.samples) == (4, 4)
    assert report.throughput.events == 6
    assert not flag.exists()  # Redis was started again
    assert len(fake.statuses) == 4 + 1 + 4 + 1 + 6


def test_run_refuses_to_start_when_the_system_is_not_healthy() -> None:
    fake = FakeHookgate()
    fake.health = "degraded"
    with client(fake) as http, pytest.raises(BenchError, match="make stack"):
        _measure(_parser().parse_args([f"--secret={SECRET}"]), "r5", http)


def test_a_delivery_without_answer_in_time_fails_clearly() -> None:
    def hang(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    with httpx.Client(base_url="http://h", transport=httpx.MockTransport(hang)) as http:
        provider = Provider(http, secret=SECRET, run_id="r6")
        with pytest.raises(BenchError, match="no answer within the request timeout"):
            provider.deliver(provider.event("slow"))


def test_request_timeout_defaults_to_five_seconds() -> None:
    assert _parser().parse_args([]).request_timeout == 5.0
