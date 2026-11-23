"""Job worker. Run standalone with ``python -m app.worker`` or embed in the API process."""

from __future__ import annotations

import asyncio
import json
import signal
import time

from redis.asyncio import BlockingConnectionPool, Redis
from redis.exceptions import RedisError

from .config import Settings, get_settings
from .handlers import HANDLERS, JobContext
from .logging import configure_logging, get_logger
from .queue import Job, JobQueue

log = get_logger("taskforge.worker")

MAX_RESULT_BYTES = 64 * 1024


class Worker:
    def __init__(self, queue: JobQueue, settings: Settings):
        self.queue = queue
        self.s = settings
        self._stop = asyncio.Event()

    def stop(self) -> None:
        """Ask the worker to stop claiming new jobs; in-flight jobs are allowed to finish."""
        self._stop.set()

    async def run(self) -> None:
        log.info("worker.start", concurrency=self.s.worker_concurrency)
        tasks = [asyncio.create_task(self._consumer(i)) for i in range(self.s.worker_concurrency)]
        tasks.append(asyncio.create_task(self._promoter()))
        tasks.append(asyncio.create_task(self._reaper()))
        await self._stop.wait()
        log.info("worker.draining", grace_s=self.s.shutdown_grace_s)
        done, pending = await asyncio.wait(tasks, timeout=self.s.shutdown_grace_s)
        for t in pending:  # grace period over: abandon; the reaper elsewhere will retry the job
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        log.info("worker.stopped", abandoned=len(pending))

    async def _consumer(self, n: int) -> None:
        while not self._stop.is_set():
            try:
                job = await self.queue.claim(timeout=1)
            except RedisError as exc:
                log.error("worker.redis_error", error=str(exc))
                await asyncio.sleep(1)
                continue
            if job is not None:
                await self._execute(job)

    async def _execute(self, job: Job) -> None:
        handler = HANDLERS.get(job.type)
        started = time.monotonic()
        try:
            if handler is None:
                await self.queue.fail(job, f"unknown job type {job.type!r}", retryable=False)
                log.warning("job.unknown_type", job_id=job.id, type=job.type)
                return
            result = await asyncio.wait_for(
                handler(job.payload, JobContext(job.id, job.attempts)),
                timeout=self.s.job_timeout_s,
            )
            encoded = json.dumps(result, default=str)
            if len(encoded) > MAX_RESULT_BYTES:
                encoded = json.dumps({"truncated": True})
            await self.queue.complete(job, encoded)
            log.info(
                "job.succeeded",
                job_id=job.id,
                type=job.type,
                attempt=job.attempts,
                duration_ms=round((time.monotonic() - started) * 1000, 1),
            )
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            state = await self.queue.fail(job, f"handler exceeded {self.s.job_timeout_s}s timeout")
            log.warning("job.timeout", job_id=job.id, type=job.type, state=state)
        except Exception as exc:  # noqa: BLE001 - a handler may raise anything
            state = await self.queue.fail(job, f"{type(exc).__name__}: {exc}")
            log.warning("job.failed", job_id=job.id, type=job.type, attempt=job.attempts, state=state, error=str(exc))

    async def _promoter(self) -> None:
        while not self._stop.is_set():
            try:
                await self.queue.promote_due()
            except RedisError as exc:
                log.error("promoter.redis_error", error=str(exc))
            await self._sleep(self.s.promoter_interval_s)

    async def _reaper(self) -> None:
        while not self._stop.is_set():
            try:
                n = await self.queue.reap()
                if n:
                    log.warning("reaper.reclaimed", count=n)
            except RedisError as exc:
                log.error("reaper.redis_error", error=str(exc))
            await self._sleep(self.s.reaper_interval_s)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            pass


def make_redis(settings: Settings) -> Redis:
    """Redis client backed by a blocking pool: when every connection is busy, callers wait up
    to 5s for one instead of failing immediately. Each worker consumer holds one connection
    while blocked in BLMOVE, so the pool must exceed worker_concurrency."""
    pool = BlockingConnectionPool.from_url(
        settings.redis_url,
        decode_responses=True,
        max_connections=settings.redis_max_connections + settings.worker_concurrency,
        timeout=5,
        health_check_interval=30,
    )
    return Redis(connection_pool=pool)


async def _main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    redis = make_redis(settings)
    worker = Worker(JobQueue(redis, settings), settings)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop)
    try:
        await worker.run()
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
