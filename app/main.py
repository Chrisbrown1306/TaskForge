"""HTTP API for TaskForge."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
import time
import uuid
from contextlib import asynccontextmanager

import structlog
from fastapi import Depends, FastAPI, Header, HTTPException, Path, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.exceptions import RedisError

from .config import Settings, get_settings
from .handlers import HANDLERS
from .logging import configure_logging, get_logger
from .metrics import Metrics
from .models import JobAccepted, JobCreate, JobOut
from .queue import JobQueue
from .ratelimit import RateLimiter
from .worker import Worker, make_redis

log = get_logger("taskforge.api")

IDEMPOTENCY_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
JOB_ID_PATTERN = r"^[0-9a-f]{32}$"


def owner_id(api_key: str) -> str:
    """Stable, non-reversible identifier for an API key. Raw keys are never stored in Redis."""
    return hashlib.sha256(api_key.encode()).hexdigest()[:16]


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        redis = make_redis(settings)
        queue = JobQueue(redis, settings)
        app.state.settings = settings
        app.state.redis = redis
        app.state.queue = queue
        app.state.limiter = RateLimiter(
            redis,
            capacity=settings.rate_limit_capacity,
            refill_per_sec=settings.rate_limit_refill_per_sec,
            prefix=settings.key_prefix,
        )
        worker_task: asyncio.Task | None = None
        worker: Worker | None = None
        if settings.embedded_worker:
            worker = Worker(queue, settings)
            worker_task = asyncio.create_task(worker.run())
        if not settings.api_key_set:
            log.warning("config.no_api_keys", hint="set TF_API_KEYS; all /v1 requests will be rejected")
        log.info("api.start", embedded_worker=settings.embedded_worker)
        try:
            yield
        finally:
            if worker and worker_task:
                worker.stop()
                await worker_task
            await redis.aclose()
            log.info("api.stopped")

    app = FastAPI(
        title="TaskForge",
        version="1.0.0",
        description="Reliable job queue with a distributed rate limiter.",
        lifespan=lifespan,
    )
    app.state.metrics = Metrics()

    # ------------------------------------------------------------------ middleware

    @app.middleware("http")
    async def observe(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            elapsed = time.perf_counter() - start
            route = getattr(request.scope.get("route"), "path", "unmatched")
            m: Metrics = app.state.metrics
            m.http_requests.labels(request.method, route, str(status)).inc()
            m.http_latency.labels(request.method, route).observe(elapsed)
            if route not in ("/healthz", "/readyz", "/metrics"):
                log.info(
                    "http.request",
                    method=request.method,
                    path=request.url.path,
                    status=status,
                    duration_ms=round(elapsed * 1000, 2),
                )

    @app.exception_handler(RedisError)
    async def redis_down(request: Request, exc: RedisError):
        log.error("redis.error", error=str(exc))
        return JSONResponse({"detail": "backing store unavailable"}, status_code=503)

    # ------------------------------------------------------------------ dependencies

    async def authenticate(request: Request, x_api_key: str | None = Header(default=None)) -> str:
        if not x_api_key:
            raise HTTPException(401, "missing X-API-Key header", headers={"WWW-Authenticate": "ApiKey"})
        supplied = x_api_key.encode()
        matched: str | None = None
        for key in app.state.settings.api_key_set:  # no early exit: constant-ish time
            if hmac.compare_digest(key.encode(), supplied):
                matched = key
        if matched is None:
            raise HTTPException(401, "invalid API key", headers={"WWW-Authenticate": "ApiKey"})
        return owner_id(matched)

    async def rate_limit(response: Response, owner: str = Depends(authenticate)) -> str:
        limiter: RateLimiter = app.state.limiter
        res = await limiter.check(owner)
        response.headers["RateLimit-Limit"] = str(limiter.capacity)
        response.headers["RateLimit-Remaining"] = str(res.remaining)
        if not res.allowed:
            app.state.metrics.rate_limited.inc()
            raise HTTPException(
                429,
                "rate limit exceeded",
                headers={
                    "Retry-After": str(max(1, int(res.retry_after_s + 0.999))),
                    "RateLimit-Limit": str(limiter.capacity),
                    "RateLimit-Remaining": "0",
                },
            )
        return owner

    # ------------------------------------------------------------------ routes

    @app.post("/v1/jobs", response_model=JobAccepted, status_code=202, tags=["jobs"])
    async def create_job(
        body: JobCreate,
        response: Response,
        owner: str = Depends(rate_limit),
        idempotency_key: str | None = Header(default=None),
    ):
        if body.type not in HANDLERS:
            raise HTTPException(422, f"unknown job type {body.type!r}; available: {sorted(HANDLERS)}")
        if len(json.dumps(body.payload)) > settings.max_payload_bytes:
            raise HTTPException(413, f"payload exceeds {settings.max_payload_bytes} bytes")
        if idempotency_key is not None and not IDEMPOTENCY_RE.match(idempotency_key):
            raise HTTPException(422, "Idempotency-Key must match [A-Za-z0-9_.:-]{1,128}")

        queue: JobQueue = app.state.queue
        job_id, created = await queue.enqueue(
            job_type=body.type,
            payload=body.payload,
            owner=owner,
            max_attempts=body.max_attempts,
            delay_s=body.delay_seconds,
            idempotency_key=idempotency_key,
        )
        response.headers["Location"] = f"/v1/jobs/{job_id}"
        if not created:
            response.status_code = 200  # replay of an earlier request
        return JobAccepted(id=job_id, status="queued" if created else "duplicate", duplicate=not created)

    async def _owned_job(job_id: str, owner: str) -> dict:
        job = await app.state.queue.get(job_id)
        # 404 (not 403) for other tenants' jobs, so ids cannot be probed.
        if job is None or job.get("owner") != owner:
            raise HTTPException(404, "job not found")
        return job

    @app.get("/v1/jobs/{job_id}", response_model=JobOut, tags=["jobs"])
    async def get_job(job_id: str = Path(pattern=JOB_ID_PATTERN), owner: str = Depends(rate_limit)):
        return JobOut(**{k: v for k, v in (await _owned_job(job_id, owner)).items() if k in JobOut.model_fields})

    @app.post("/v1/jobs/{job_id}/retry", response_model=JobAccepted, status_code=202, tags=["jobs"])
    async def retry_dead_job(job_id: str = Path(pattern=JOB_ID_PATTERN), owner: str = Depends(rate_limit)):
        await _owned_job(job_id, owner)
        if not await app.state.queue.requeue_dead(job_id):
            raise HTTPException(409, "only jobs in the 'dead' state can be retried")
        return JobAccepted(id=job_id, status="queued")

    @app.get("/v1/queue/stats", tags=["ops"])
    async def queue_stats(owner: str = Depends(rate_limit)):
        return await app.state.queue.stats()

    @app.get("/healthz", tags=["ops"])
    async def healthz():
        return {"status": "ok"}

    @app.get("/readyz", tags=["ops"])
    async def readyz():
        try:
            await app.state.redis.ping()
        except RedisError:
            return JSONResponse({"status": "unavailable"}, status_code=503)
        return {"status": "ready"}

    @app.get("/metrics", include_in_schema=False)
    async def metrics(authorization: str | None = Header(default=None)):
        token = settings.metrics_token
        if token and not hmac.compare_digest(authorization or "", f"Bearer {token}"):
            raise HTTPException(401, "metrics token required")
        m: Metrics = app.state.metrics
        try:
            m.queue.snapshot = await app.state.queue.stats()
        except RedisError:
            m.queue.snapshot = {}
        return Response(generate_latest(m.registry), media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()
