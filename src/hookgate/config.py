"""Application settings, read from environment variables (FR-SEC-2).

The five required values default to an empty string and are rejected by a validator,
so a missing value fails at startup with a clear message. Optional tuning values have
the defaults of the SRS.
"""

from functools import lru_cache

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Required
    database_url: str = Field(default="", validate_default=True)
    redis_url: str = Field(default="", validate_default=True)
    broker_url: str = Field(default="", validate_default=True)
    webhook_secret: SecretStr = Field(default=SecretStr(""), validate_default=True)
    admin_api_key: SecretStr = Field(default=SecretStr(""), validate_default=True)

    # Optional tuning (SRS defaults)
    timestamp_tolerance_s: int = Field(default=300, ge=0)
    dedupe_ttl_s: int = Field(default=86_400, gt=0)
    lease_s: int = Field(default=60, gt=0)
    max_attempts: int = Field(default=5, ge=1)
    retry_base_s: float = Field(default=2.0, gt=0)
    retry_cap_s: float = Field(default=300.0, gt=0)
    redis_socket_timeout_s: float = Field(default=0.05, gt=0)
    sweep_interval_s: int = Field(default=15, gt=0)
    worker_metrics_port: int = Field(default=9100, ge=1, le=65535)

    @field_validator("database_url", "redis_url", "broker_url", mode="after")
    @classmethod
    def _require_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must be set")
        return value

    @field_validator("webhook_secret", "admin_api_key", mode="after")
    @classmethod
    def _require_secret(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("must be set")
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
