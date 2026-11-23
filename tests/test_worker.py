import asyncio

import pytest

from app.handlers import register
from app.worker import Worker


@register("test.slow")
async def _slow(payload, ctx):
    await asyncio.sleep(float(payload.get("s", 5)))
    return "done"


async def _wait_for(queue, job_id, statuses, timeout=5.0):  # noqa: ASYNC109
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        job = await queue.get(job_id)
        if job and job["status"] in statuses:
            return job
        await asyncio.sleep(0.03)
    raise AssertionError(f"job {job_id} never reached {statuses}; last={await queue.get(job_id)}")


@pytest.fixture
async def running_worker(queue, settings):
    worker = Worker(queue, settings)
    task = asyncio.create_task(worker.run())
    yield worker
    worker.stop()
    await asyncio.wait_for(task, timeout=10)


async def test_processes_job_successfully(queue, running_worker):
    jid, _ = await queue.enqueue(job_type="sum", payload={"numbers": [1, 2, 3.5]}, owner="o")
    job = await _wait_for(queue, jid, {"succeeded"})
    assert job["result"] == {"sum": 6.5, "count": 3}


async def test_flaky_job_succeeds_after_retries(queue, running_worker):
    jid, _ = await queue.enqueue(job_type="flaky", payload={"succeed_on_attempt": 3}, owner="o", max_attempts=5)
    job = await _wait_for(queue, jid, {"succeeded"})
    assert job["attempts"] == 3 and job["result"] == {"attempts_needed": 3}


async def test_always_failing_job_ends_in_dlq(queue, running_worker):
    jid, _ = await queue.enqueue(job_type="fail", payload={"message": "nope"}, owner="o", max_attempts=2)
    job = await _wait_for(queue, jid, {"dead"})
    assert job["attempts"] == 2 and "nope" in job["error"]
    assert (await queue.stats())["depth"]["dead"] == 1


async def test_unknown_type_is_dead_without_retry(queue, running_worker):
    jid, _ = await queue.enqueue(job_type="does-not-exist", payload={}, owner="o", max_attempts=5)
    job = await _wait_for(queue, jid, {"dead"})
    assert job["attempts"] == 1 and "unknown job type" in job["error"]


async def test_handler_timeout_is_a_failure(queue, running_worker):
    # job_timeout_s = 2 in test settings
    jid, _ = await queue.enqueue(job_type="test.slow", payload={"s": 10}, owner="o", max_attempts=1)
    job = await _wait_for(queue, jid, {"dead"}, timeout=6)
    assert "timeout" in job["error"]


async def test_many_jobs_processed_exactly_once(queue, running_worker):
    ids = [(await queue.enqueue(job_type="echo", payload={"i": i}, owner="o"))[0] for i in range(100)]
    for jid in ids:
        job = await _wait_for(queue, jid, {"succeeded"}, timeout=10)
        assert job["attempts"] == 1


async def test_graceful_shutdown_finishes_in_flight_job(queue, settings):
    worker = Worker(queue, settings)
    task = asyncio.create_task(worker.run())
    jid, _ = await queue.enqueue(job_type="test.slow", payload={"s": 0.5}, owner="o")
    await _wait_for(queue, jid, {"running"})
    worker.stop()  # SIGTERM equivalent: stop claiming, let the running job finish
    await asyncio.wait_for(task, timeout=10)
    assert (await queue.get(jid))["status"] == "succeeded"
