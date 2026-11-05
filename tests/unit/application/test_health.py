"""Readiness aggregation (FR-OBS-2, SDD 15.4)."""

import time

import pytest

from hookgate.application.health import CheckStatus, HealthReport, HealthStatus, Readiness


def ok() -> None:
    return None


def down() -> None:
    raise ConnectionError("unreachable")


def readiness(db: bool = True, redis: bool = True, broker: bool = True) -> Readiness:
    return Readiness(
        {
            "db": ok if db else down,
            "redis": ok if redis else down,
            "broker": ok if broker else down,
        },
        critical=frozenset({"db"}),
    )


def test_all_up_is_ok() -> None:
    assert readiness()() == HealthReport(
        HealthStatus.OK,
        {"db": CheckStatus.OK, "redis": CheckStatus.OK, "broker": CheckStatus.OK},
    )


@pytest.mark.parametrize(
    ("db", "redis", "broker", "expected"),
    [
        (True, False, True, HealthStatus.DEGRADED),
        (True, True, False, HealthStatus.DEGRADED),
        (True, False, False, HealthStatus.DEGRADED),
        (False, True, True, HealthStatus.DOWN),
        (False, False, False, HealthStatus.DOWN),
    ],
)
def test_only_the_database_makes_the_service_down(
    db: bool, redis: bool, broker: bool, expected: HealthStatus
) -> None:
    report = readiness(db, redis, broker)()

    assert report.status is expected
    assert report.checks["db"] is (CheckStatus.OK if db else CheckStatus.DOWN)
    assert report.checks["redis"] is (CheckStatus.OK if redis else CheckStatus.DOWN)
    assert report.checks["broker"] is (CheckStatus.OK if broker else CheckStatus.DOWN)


def test_any_exception_counts_as_down() -> None:
    def broken() -> None:
        raise RuntimeError("unexpected")

    report = Readiness({"db": ok, "redis": broken}, critical=frozenset({"db"}))()

    assert report.checks["redis"] is CheckStatus.DOWN


def test_checks_run_in_parallel() -> None:
    def slow() -> None:
        time.sleep(0.3)

    started = time.monotonic()
    Readiness({"db": slow, "redis": slow, "broker": slow}, critical=frozenset({"db"}))()

    assert time.monotonic() - started < 0.6


def test_critical_check_must_exist() -> None:
    with pytest.raises(ValueError, match="db"):
        Readiness({"redis": ok}, critical=frozenset({"db"}))
