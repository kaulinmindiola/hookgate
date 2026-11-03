import pytest
from pydantic import ValidationError

from hookgate.config import Settings, get_settings

REQUIRED = {
    "DATABASE_URL": "postgresql+psycopg://u:p@localhost:5432/db",
    "REDIS_URL": "redis://localhost:6379/0",
    "BROKER_URL": "amqp://u:p@localhost:5672//",
    "WEBHOOK_SECRET": "s3cret-webhook",
    "ADMIN_API_KEY": "s3cret-admin",
}
OPTIONAL = [
    "TIMESTAMP_TOLERANCE_S",
    "DEDUPE_TTL_S",
    "LEASE_S",
    "MAX_ATTEMPTS",
    "RETRY_BASE_S",
    "RETRY_CAP_S",
    "REDIS_SOCKET_TIMEOUT_S",
    "SWEEP_INTERVAL_S",
]


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: pytest.TempPathFactory) -> None:
    """No .env file and no inherited variables: each test sets what it needs."""
    monkeypatch.chdir(str(tmp_path))
    for name in [*REQUIRED, *OPTIONAL]:
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()


def set_required(monkeypatch: pytest.MonkeyPatch, skip: str | None = None) -> None:
    for name, value in REQUIRED.items():
        if name != skip:
            monkeypatch.setenv(name, value)


def test_defaults_match_the_srs(monkeypatch: pytest.MonkeyPatch) -> None:
    set_required(monkeypatch)

    settings = Settings()

    assert settings.database_url == REQUIRED["DATABASE_URL"]
    assert settings.timestamp_tolerance_s == 300
    assert settings.dedupe_ttl_s == 86_400
    assert settings.lease_s == 60
    assert settings.max_attempts == 5
    assert (settings.retry_base_s, settings.retry_cap_s) == (2.0, 300.0)
    assert settings.redis_socket_timeout_s == 0.05
    assert settings.sweep_interval_s == 15


def test_optional_values_can_be_overridden(monkeypatch: pytest.MonkeyPatch) -> None:
    set_required(monkeypatch)
    monkeypatch.setenv("LEASE_S", "2")
    monkeypatch.setenv("MAX_ATTEMPTS", "3")

    settings = Settings()

    assert (settings.lease_s, settings.max_attempts) == (2, 3)


@pytest.mark.parametrize("missing", list(REQUIRED))
def test_each_required_value_must_be_set(monkeypatch: pytest.MonkeyPatch, missing: str) -> None:
    set_required(monkeypatch, skip=missing)

    with pytest.raises(ValidationError, match=missing.lower()):
        Settings()


def test_blank_required_value_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    set_required(monkeypatch)
    monkeypatch.setenv("WEBHOOK_SECRET", "   ")

    with pytest.raises(ValidationError, match="webhook_secret"):
        Settings()


def test_secrets_never_appear_in_the_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    set_required(monkeypatch)

    text = repr(Settings())

    assert "s3cret-webhook" not in text
    assert "s3cret-admin" not in text


def test_invalid_optional_value_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    set_required(monkeypatch)
    monkeypatch.setenv("LEASE_S", "0")

    with pytest.raises(ValidationError, match="lease_s"):
        Settings()


def test_get_settings_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    set_required(monkeypatch)

    assert get_settings() is get_settings()
