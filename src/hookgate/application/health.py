"""Readiness aggregation (FR-OBS-2, SDD 15.4).

Each check is a callable that returns when the dependency answers and raises when it
does not. Checks run in parallel, so the probe takes as long as the slowest timeout,
not their sum. A failed *critical* check (the database: without it nothing can be
acknowledged) makes the service ``down``; any other failure makes it ``degraded``,
because ingestion still works: Redis is only a fast path (ADR-003) and a broker outage
is recovered by the sweeper (FR-ING-7).
"""

from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import StrEnum

import structlog

HealthCheck = Callable[[], None]

_log = structlog.get_logger(__name__)


class HealthStatus(StrEnum):
    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"


class CheckStatus(StrEnum):
    OK = "ok"
    DOWN = "down"


@dataclass(frozen=True, slots=True)
class HealthReport:
    status: HealthStatus
    checks: dict[str, CheckStatus]


class Readiness:
    def __init__(self, checks: Mapping[str, HealthCheck], *, critical: frozenset[str]) -> None:
        unknown = critical - checks.keys()
        if unknown:
            raise ValueError(f"critical checks without a probe: {sorted(unknown)}")
        self._checks = dict(checks)
        self._critical = critical

    def __call__(self) -> HealthReport:
        with ThreadPoolExecutor(max_workers=len(self._checks) or 1) as pool:
            futures = {name: pool.submit(check) for name, check in self._checks.items()}
        results: dict[str, CheckStatus] = {}
        for name, future in futures.items():
            error = future.exception()
            if error is None:
                results[name] = CheckStatus.OK
            else:
                results[name] = CheckStatus.DOWN
                _log.warning("health_check_failed", check=name, error=type(error).__name__)
        failed = {name for name, result in results.items() if result is CheckStatus.DOWN}
        if failed & self._critical:
            status = HealthStatus.DOWN
        elif failed:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.OK
        return HealthReport(status=status, checks=results)
