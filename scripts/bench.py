"""Benchmarks. Usage:

    python scripts/bench.py worker --jobs 20000 --concurrency 32
    python scripts/bench.py http   --url http://localhost:8000 --key bench --requests 20000 --concurrency 64

`worker` measures end-to-end queue throughput (enqueue -> claim -> run -> complete) with no
HTTP in the path. `http` measures POST /v1/jobs latency and throughput against a running API
(start it with a very high rate limit, otherwise you are benchmarking the limiter).
"""

import argparse
import asyncio
import os
import statistics
import sys
import time

import httpx

from app.config import Settings
from app.logging import configure_logging
from app.queue import JobQueue
from app.worker import Worker, make_redis


async def bench_worker(args: argparse.Namespace) -> None:
    configure_logging("ERROR")  # per-job INFO logs would dominate the measurement
    settings = Settings(
        redis_url=args.redis_url,
        key_prefix="bench",
        worker_concurrency=args.concurrency,
        promoter_interval_s=0.5,
        log_level="ERROR",
    )
    redis = make_redis(settings)
    await redis.flushdb()
    queue = JobQueue(redis, settings)

    t0 = time.perf_counter()
    for start in range(0, args.jobs, 500):
        await asyncio.gather(
            *(
                queue.enqueue(job_type="echo", payload={"i": i}, owner="bench")
                for i in range(start, min(start + 500, args.jobs))
            )
        )
    enqueue_s = time.perf_counter() - t0

    procs: list[asyncio.subprocess.Process] = []
    worker = task = None
    if args.processes > 1:
        env = {
            **os.environ,
            "TF_REDIS_URL": args.redis_url,
            "TF_KEY_PREFIX": "bench",
            "TF_WORKER_CONCURRENCY": str(args.concurrency),
            "TF_LOG_LEVEL": "ERROR",
            "PYTHONPATH": ".",
        }
        procs = [
            await asyncio.create_subprocess_exec(sys.executable, "-m", "app.worker", env=env)
            for _ in range(args.processes)
        ]
    else:
        worker = Worker(queue, settings)
        task = asyncio.create_task(worker.run())
    t1 = time.perf_counter()
    while (await queue.stats())["counters"].get("succeeded", 0) < args.jobs:  # noqa: ASYNC110
        await asyncio.sleep(0.05)
    drain_s = time.perf_counter() - t1
    if worker and task:
        worker.stop()
        await task
    for proc in procs:
        proc.terminate()
        await proc.wait()

    print(f"enqueue : {args.jobs / enqueue_s:>9,.0f} jobs/s  ({args.jobs} jobs in {enqueue_s:.2f}s)")
    shape = f"processes={args.processes} x concurrency={args.concurrency}"
    print(f"process : {args.jobs / drain_s:>9,.0f} jobs/s  ({shape}, {drain_s:.2f}s)")
    await redis.flushdb()
    await redis.aclose()


async def bench_http(args: argparse.Namespace) -> None:
    latencies: list[float] = []
    statuses: dict[int, int] = {}
    sem = asyncio.Semaphore(args.concurrency)
    headers = {"X-API-Key": args.key}

    async with httpx.AsyncClient(
        base_url=args.url,
        headers=headers,
        timeout=30,
        limits=httpx.Limits(max_connections=args.concurrency),
    ) as client:
        # warm up the connection pool
        await asyncio.gather(*(client.get("/healthz") for _ in range(args.concurrency)))

        async def one(i: int) -> None:
            async with sem:
                t = time.perf_counter()
                r = await client.post("/v1/jobs", json={"type": "echo", "payload": {"i": i}})
                latencies.append((time.perf_counter() - t) * 1000)
                statuses[r.status_code] = statuses.get(r.status_code, 0) + 1

        t0 = time.perf_counter()
        await asyncio.gather(*(one(i) for i in range(args.requests)))
        total = time.perf_counter() - t0

    latencies.sort()
    pct = lambda p: latencies[min(len(latencies) - 1, int(len(latencies) * p))]  # noqa: E731
    print(f"requests: {args.requests} in {total:.2f}s -> {args.requests / total:,.0f} req/s  status={statuses}")
    print(
        f"latency ms: mean={statistics.mean(latencies):.1f} p50={pct(0.50):.1f} "
        f"p95={pct(0.95):.1f} p99={pct(0.99):.1f} max={latencies[-1]:.1f}"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker")
    w.add_argument("--jobs", type=int, default=20000)
    w.add_argument("--concurrency", type=int, default=32)
    w.add_argument("--processes", type=int, default=1, help="worker processes (1 = in-process)")
    w.add_argument("--redis-url", default="redis://localhost:6379/14")
    h = sub.add_parser("http")
    h.add_argument("--url", default="http://localhost:8000")
    h.add_argument("--key", required=True)
    h.add_argument("--requests", type=int, default=20000)
    h.add_argument("--concurrency", type=int, default=64)
    args = ap.parse_args()
    asyncio.run(bench_worker(args) if args.cmd == "worker" else bench_http(args))


if __name__ == "__main__":
    main()
