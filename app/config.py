"""Application settings, loaded from environment variables (prefix ``TF_``)."""

from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TF_", env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    key_prefix: str = "tf"
    redis_max_connections: int = 100  # pool size; callers wait (up to 5s) when exhausted

    # Auth: comma-separated API keys. Empty means every request is rejected.
    api_keys: str = ""

    # Rate limiting (token bucket, per API key)
    rate_limit_capacity: int = 20
    rate_limit_refill_per_sec: float = 10.0

    # Queue behaviour
    visibility_timeout_s: int = 30  # how long a claimed job may run before it is reclaimed
    job_timeout_s: int = 25  # handler hard timeout; must be < visibility_timeout_s
    max_attempts_default: int = 3
    backoff_base_s: float = 1.0
    backoff_max_s: float = 60.0
    idempotency_ttl_s: int = 86_400
    job_ttl_s: int = 7 * 86_400
    max_payload_bytes: int = 64 * 1024

    # Worker
    embedded_worker: bool = False  # run a worker inside the API process (free-tier friendly)
    worker_concurrency: int = 4
    reaper_interval_s: float = 5.0
    promoter_interval_s: float = 0.5
    shutdown_grace_s: float = 20.0

    log_level: str = "INFO"

    # If set, GET /metrics requires "Authorization: Bearer <token>".
    metrics_token: str = ""

    @model_validator(mode="after")
    def _check(self) -> "Settings":
        if self.job_timeout_s >= self.visibility_timeout_s:
            raise ValueError("job_timeout_s must be smaller than visibility_timeout_s")
        return self

    @property
    def api_key_set(self) -> frozenset[str]:
        return frozenset(k.strip() for k in self.api_keys.split(",") if k.strip())


@lru_cache
def get_settings() -> Settings:
    return Settings()
