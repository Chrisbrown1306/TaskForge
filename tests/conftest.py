import os

import pytest
import pytest_asyncio

from app.config import Settings
from app.queue import JobQueue
from app.worker import make_redis

# Tests run against a real Redis. They use database 15 and flush it, so we refuse to run
# against any other database index to avoid wiping real data.
TEST_REDIS_URL = os.environ.get("TF_TEST_REDIS_URL", "redis://localhost:6379/15")
assert TEST_REDIS_URL.rstrip("/").endswith("/15"), "TF_TEST_REDIS_URL must use database 15"


@pytest.fixture
def settings() -> Settings:
    return Settings(
        redis_url=TEST_REDIS_URL,
        key_prefix="tftest",
        api_keys="key-alpha,key-beta",
        rate_limit_capacity=1000,
        rate_limit_refill_per_sec=1000,
        visibility_timeout_s=3,
        job_timeout_s=2,
        backoff_base_s=0.05,
        backoff_max_s=0.2,
        worker_concurrency=2,
        promoter_interval_s=0.05,
        reaper_interval_s=0.1,
        shutdown_grace_s=3,
        log_level="WARNING",
    )


@pytest_asyncio.fixture
async def redis(settings):
    client = make_redis(settings)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


@pytest_asyncio.fixture
async def queue(redis, settings) -> JobQueue:
    return JobQueue(redis, settings)
