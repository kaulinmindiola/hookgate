"""Indicative ingestion latency (NFR-2) and throughput (NFR-3) of a running Hookgate.

Start the whole system first (``make stack``), then, from the repository root:

    uv run python -m scripts.bench --output docs/benchmark.md

Everything is measured from the client, over HTTP on one machine, so the numbers are
indicative only: they describe that machine, not a capacity promise.

1. Latency, Redis up: sequential deliveries of new events (one in flight at a time),
   after a short warm-up; p50, p95, p99 and max.
2. Latency, Redis down: the same after ``docker compose stop redis``, once ``/health``
   reports ``degraded``. Redis is always started again, even if the run fails.
3. Throughput: new events sent by concurrent clients. "Accepted/s" is the ingestion
   rate (every answer must be ``202 accepted``); "end to end/s" counts until no event
   of the run is pending any more (``received``, ``processing`` or ``failed``) in the
   admin API, so it includes queueing and processing by the worker.

Connection settings come from flags or from HOOKGATE_URL, WEBHOOK_SECRET and
ADMIN_API_KEY (defaults: the development values of ``.env.example``). Exit code 0 when
the measurement completed (a missed target is reported, not failed), 1 otherwise.
"""

import argparse
import math
import os
import platform
import secrets
import subprocess
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from simulator.client import SIGNATURE_HEADER, WEBHOOK_PATH, envelope, signature_header
from simulator.model import EventSpec

DEFAULT_URL = "http://127.0.0.1:8000"
DEFAULT_SECRET = "dev-only-webhook-secret-change-me"
DEFAULT_API_KEY = "dev-only-admin-key-change-me"
TARGET_P95_REDIS_UP_MS = 100.0
TARGET_P95_REDIS_DOWN_MS = 150.0
PENDING_STATUSES = ("received", "processing", "failed")
PAGE_SIZE = 200


class BenchError(Exception):
    """The measurement could not be completed (wrong answer, timeout, failed command)."""


def percentile(values: Sequence[float], q: float) -> float:
    """Nearest-rank percentile: the smallest sample with at least ``q`` % of them at or below."""
    if not values:
        raise ValueError("no samples")
    if not 0 < q <= 100:
        raise ValueError(f"percentile must be in (0, 100], got {q}")
    ordered = sorted(values)
    return ordered[math.ceil(q / 100 * len(ordered)) - 1]


@dataclass(frozen=True, slots=True)
class Latency:
    label: str
    samples: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    target_p95_ms: float

    @classmethod
    def from_seconds(
        cls, label: str, seconds: Sequence[float], *, target_p95_ms: float
    ) -> "Latency":
        ms = [s * 1000 for s in seconds]
        return cls(
            label=label,
            samples=len(ms),
            p50_ms=percentile(ms, 50),
            p95_ms=percentile(ms, 95),
            p99_ms=percentile(ms, 99),
            max_ms=max(ms),
            target_p95_ms=target_p95_ms,
        )

    @property
    def within_target(self) -> bool:
        return self.p95_ms < self.target_p95_ms


@dataclass(frozen=True, slots=True)
class Throughput:
    events: int
    concurrency: int
    send_s: float
    end_to_end_s: float

    @property
    def accepted_per_s(self) -> float:
        return self.events / self.send_s

    @property
    def processed_per_s(self) -> float:
        return self.events / self.end_to_end_s


class Provider:
    """Signs and sends new events, as the provider would; ids are unique per run."""

    def __init__(self, http: httpx.Client, *, secret: str, run_id: str) -> None:
        self._http = http
        self._secret = secret
        self.prefix = f"bench-{run_id}-"

    def event(self, name: str) -> EventSpec:
        event_id = f"{self.prefix}{name}"
        # One payment per event: no row-lock contention between events of the run.
        return EventSpec(event_id, "payment.succeeded", f"pay_{event_id}", datetime.now(UTC), 1500)

    def deliver(self, event: EventSpec) -> float:
        """Send one delivery; returns the seconds taken. Anything but 202 accepted fails."""
        body = envelope(event)
        headers = {
            "Content-Type": "application/json",
            SIGNATURE_HEADER: signature_header(self._secret, int(time.time()), body),
        }
        started = time.perf_counter()
        try:
            response = self._http.post(WEBHOOK_PATH, content=body, headers=headers)
        except httpx.TimeoutException as exc:
            raise BenchError(
                f"{event.event_id}: no answer within the request timeout "
                f"({time.perf_counter() - started:.1f} s); a provider would give up and retry"
            ) from exc
        elapsed = time.perf_counter() - started
        result = _result(response)
        if (response.status_code, result) != (202, "accepted"):
            raise BenchError(f"{event.event_id}: {response.status_code} {result}")
        return elapsed


def _result(response: httpx.Response) -> str:
    try:
        payload: Any = response.json()
    except ValueError:
        return response.text[:100]
    if isinstance(payload, dict):
        return str(payload.get("status") or payload.get("error") or payload)
    return str(payload)


def measure_latency(provider: Provider, phase: str, *, requests: int, warmup: int) -> list[float]:
    for n in range(warmup):
        provider.deliver(provider.event(f"{phase}-warmup-{n}"))
    return [provider.deliver(provider.event(f"{phase}-{n}")) for n in range(requests)]


def measure_throughput(
    provider: Provider,
    admin: "Admin",
    *,
    events: int,
    concurrency: int,
    timeout_s: float,
    poll_s: float = 0.5,
) -> Throughput:
    batch = [provider.event(f"load-{n}") for n in range(events)]
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(provider.deliver, batch))
    sent = time.perf_counter()
    prefix = f"{provider.prefix}load-"
    wait_until(
        lambda: not admin.pending_ids(prefix),
        timeout_s=timeout_s,
        poll_s=poll_s,
        what=f"the {events} events of the load phase to be processed",
    )
    finished = time.perf_counter()
    dead = admin.ids(prefix, ("dead",))
    if dead:
        raise BenchError(f"{len(dead)} event(s) of the load phase ended dead, e.g. {min(dead)}")
    return Throughput(events, concurrency, sent - started, finished - started)


class Admin:
    """The admin API, only what the benchmark needs."""

    def __init__(self, http: httpx.Client, *, api_key: str) -> None:
        self._http = http
        self._headers = {"X-API-Key": api_key}

    def pending_ids(self, prefix: str) -> set[str]:
        return self.ids(prefix, PENDING_STATUSES)

    def ids(self, prefix: str, statuses: Sequence[str]) -> set[str]:
        """Provider ids starting with ``prefix`` among the events in ``statuses``."""
        return {
            str(item["event_id"])
            for item in self._pages(statuses)
            if str(item["event_id"]).startswith(prefix)
        }

    def health(self) -> str:
        return str(self._http.get("/health").json()["status"])

    def _pages(self, statuses: Sequence[str]) -> Iterator[dict[str, Any]]:
        params: dict[str, str | int | list[str]] = {"limit": PAGE_SIZE, "status": list(statuses)}
        while True:
            response = self._http.get("/admin/events", params=params, headers=self._headers)
            response.raise_for_status()
            page: dict[str, Any] = response.json()
            yield from page["items"]
            if page["next_cursor"] is None:
                return
            params["cursor"] = str(page["next_cursor"])


def wait_until(
    condition: Callable[[], bool], *, timeout_s: float, poll_s: float, what: str
) -> None:
    deadline = time.monotonic() + timeout_s
    while not condition():
        if time.monotonic() >= deadline:
            raise BenchError(f"timed out after {timeout_s:.0f} s waiting for {what}")
        time.sleep(poll_s)


def run_command(command: str) -> None:
    result = subprocess.run(command, shell=True, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise BenchError(
            f"{command!r} failed ({result.returncode}): {result.stdout}{result.stderr}"
        )


@dataclass(frozen=True, slots=True)
class Report:
    url: str
    run_id: str
    measured_at: datetime
    redis_up: Latency
    redis_down: Latency | None
    throughput: Throughput

    def markdown(self) -> str:
        lines = [
            "## Benchmark (indicative: one machine, client-side, HTTP over loopback)",
            "",
            f"- Measured {self.measured_at:%Y-%m-%d %H:%M} UTC against {self.url} "
            f"(run `{self.run_id}`, `python -m scripts.bench`).",
            f"- Machine: {platform.platform()}, {os.cpu_count()} logical CPUs, "
            f"Python {platform.python_version()} for the client.",
            "- Latency: sequential deliveries of new events, after a warm-up. Throughput: "
            "concurrent clients; end to end = until no event of the run is pending.",
            "",
            "| Ingestion latency | Samples | p50 | p95 | p99 | max | NFR-2 target (p95) |",
            "|---|---|---|---|---|---|---|",
            _latency_row(self.redis_up),
        ]
        if self.redis_down is None:
            lines.append("| Redis down | not measured | | | | | < 150 ms |")
        else:
            lines.append(_latency_row(self.redis_down))
        t = self.throughput
        lines += [
            "",
            "| Throughput (NFR-3) | Events | Clients | Accepted/s | End to end/s |",
            "|---|---|---|---|---|",
            f"| New events | {t.events} | {t.concurrency} | {t.accepted_per_s:.0f} "
            f"| {t.processed_per_s:.0f} |",
            "",
        ]
        return "\n".join(lines)


def _latency_row(latency: Latency) -> str:
    verdict = "met" if latency.within_target else "**not met**"
    return (
        f"| {latency.label} | {latency.samples} | {latency.p50_ms:.1f} ms "
        f"| {latency.p95_ms:.1f} ms | {latency.p99_ms:.1f} ms | {latency.max_ms:.1f} ms "
        f"| < {latency.target_p95_ms:.0f} ms: {verdict} |"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.bench",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--url", default=os.environ.get("HOOKGATE_URL", DEFAULT_URL))
    parser.add_argument("--secret", default=os.environ.get("WEBHOOK_SECRET", DEFAULT_SECRET))
    parser.add_argument("--api-key", default=os.environ.get("ADMIN_API_KEY", DEFAULT_API_KEY))
    parser.add_argument("--requests", type=int, default=500, help="latency samples per phase")
    parser.add_argument("--warmup", type=int, default=20, help="discarded deliveries per phase")
    parser.add_argument("--events", type=int, default=2000, help="events in the throughput phase")
    parser.add_argument("--concurrency", type=int, default=8, help="concurrent clients")
    parser.add_argument("--timeout", type=float, default=300.0, help="end-to-end wait, seconds")
    parser.add_argument(
        "--request-timeout", type=float, default=5.0, help="per-delivery limit, seconds"
    )
    parser.add_argument("--skip-redis-down", action="store_true", help="do not stop Redis")
    parser.add_argument("--redis-stop", default="docker compose stop redis")
    parser.add_argument("--redis-start", default="docker compose start redis")
    parser.add_argument("--output", type=Path, help="also write the report to this file")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    run_id = secrets.token_hex(4)
    limits = httpx.Limits(
        max_connections=args.concurrency, max_keepalive_connections=args.concurrency
    )
    try:
        with httpx.Client(base_url=args.url, timeout=args.request_timeout, limits=limits) as http:
            report = _measure(args, run_id, http)
    except (BenchError, httpx.HTTPError) as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    text = report.markdown()
    print(text)
    if args.output is not None:
        args.output.write_text(text, encoding="utf-8")
        print(f"report written to {args.output}")
    return 0


def _measure(args: argparse.Namespace, run_id: str, http: httpx.Client) -> Report:
    provider = Provider(http, secret=args.secret, run_id=run_id)
    admin = Admin(http, api_key=args.api_key)
    health = admin.health()
    if health != "ok":
        raise BenchError(f"/health is {health!r}, expected 'ok': is `make stack` up?")
    measured_at = datetime.now(UTC)

    print(f"run {run_id}: latency with Redis up ({args.requests} deliveries)", flush=True)
    up = measure_latency(provider, "up", requests=args.requests, warmup=args.warmup)
    redis_up = Latency.from_seconds("Redis up", up, target_p95_ms=TARGET_P95_REDIS_UP_MS)

    redis_down: Latency | None = None
    if not args.skip_redis_down:
        print("latency with Redis down (stopping it)", flush=True)
        run_command(args.redis_stop)
        try:
            wait_until(
                lambda: admin.health() == "degraded", timeout_s=60, poll_s=0.5, what="degraded"
            )
            down = measure_latency(provider, "down", requests=args.requests, warmup=args.warmup)
        finally:
            run_command(args.redis_start)
        wait_until(lambda: admin.health() == "ok", timeout_s=90, poll_s=0.5, what="/health ok")
        redis_down = Latency.from_seconds(
            "Redis down", down, target_p95_ms=TARGET_P95_REDIS_DOWN_MS
        )

    print(f"throughput: {args.events} events, {args.concurrency} clients", flush=True)
    throughput = measure_throughput(
        provider, admin, events=args.events, concurrency=args.concurrency, timeout_s=args.timeout
    )
    return Report(args.url, run_id, measured_at, redis_up, redis_down, throughput)


if __name__ == "__main__":
    sys.exit(main())
