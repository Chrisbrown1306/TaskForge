import asyncio


async def _enqueue(queue, **kw):
    kw.setdefault("job_type", "echo")
    kw.setdefault("payload", {"x": 1})
    kw.setdefault("owner", "o1")
    return await queue.enqueue(**kw)


async def test_enqueue_claim_complete(queue):
    job_id, created = await _enqueue(queue)
    assert created
    assert (await queue.get(job_id))["status"] == "queued"

    job = await queue.claim(timeout=1)
    assert job.id == job_id and job.attempts == 1 and job.payload == {"x": 1}
    assert (await queue.get(job_id))["status"] == "running"

    await queue.complete(job, '{"ok": true}')
    done = await queue.get(job_id)
    assert done["status"] == "succeeded" and done["result"] == {"ok": True}
    assert (await queue.stats())["depth"] == {"ready": 0, "delayed": 0, "processing": 0, "dead": 0}


async def test_fifo_order(queue):
    ids = [(await _enqueue(queue, payload={"n": i}))[0] for i in range(5)]
    claimed = [(await queue.claim(timeout=1)).id for _ in range(5)]
    assert claimed == ids


async def test_claim_times_out_when_empty(queue):
    assert await queue.claim(timeout=0.2) is None


async def test_idempotency_key_dedupes_per_owner(queue):
    a, created_a = await _enqueue(queue, idempotency_key="k1")
    b, created_b = await _enqueue(queue, idempotency_key="k1")
    assert created_a and not created_b and a == b
    # Same key from a different owner is a different job.
    c, created_c = await _enqueue(queue, idempotency_key="k1", owner="o2")
    assert created_c and c != a
    assert (await queue.stats())["depth"]["ready"] == 2


async def test_delayed_job_runs_only_after_due(queue):
    job_id, _ = await _enqueue(queue, delay_s=0.3)
    assert (await queue.get(job_id))["status"] == "scheduled"
    assert await queue.promote_due() == 0
    assert await queue.claim(timeout=0.1) is None
    await asyncio.sleep(0.4)
    assert await queue.promote_due() == 1
    assert (await queue.claim(timeout=1)).id == job_id


async def test_retry_then_dead_letter(queue):
    job_id, _ = await _enqueue(queue, max_attempts=2)

    job = await queue.claim(timeout=1)
    assert await queue.fail(job, "boom 1") == "retrying"
    snap = await queue.get(job_id)
    assert snap["status"] == "retrying" and snap["error"] == "boom 1"

    await asyncio.sleep(0.3)  # backoff (<= 0.05s here) elapses
    await queue.promote_due()
    job = await queue.claim(timeout=1)
    assert job.attempts == 2
    assert await queue.fail(job, "boom 2") == "dead"

    assert (await queue.get(job_id))["status"] == "dead"
    stats = await queue.stats()
    assert stats["depth"]["dead"] == 1 and stats["depth"]["processing"] == 0
    assert stats["counters"]["retried"] == 1 and stats["counters"]["dead"] == 1


async def test_non_retryable_failure_goes_straight_to_dead(queue):
    await _enqueue(queue, max_attempts=5)
    job = await queue.claim(timeout=1)
    assert await queue.fail(job, "bad input", retryable=False) == "dead"


async def test_requeue_dead_job(queue):
    job_id, _ = await _enqueue(queue, max_attempts=1)
    await queue.fail(await queue.claim(timeout=1), "x")
    assert await queue.requeue_dead(job_id)
    snap = await queue.get(job_id)
    assert snap["status"] == "queued" and snap["attempts"] == 0
    assert not await queue.requeue_dead(job_id)  # no longer dead
    assert (await queue.claim(timeout=1)).id == job_id


async def test_backoff_grows_and_is_capped(queue):
    delays = [queue.backoff_delay(n) for n in (1, 2, 3, 10)]
    base, cap = queue.s.backoff_base_s, queue.s.backoff_max_s
    assert base / 2 <= delays[0] <= base
    assert delays[3] <= cap


async def test_reaper_reclaims_crashed_worker_job(queue, redis):
    """A worker claims a job and 'crashes' (never completes). After the visibility deadline the
    reaper retries it."""
    job_id, _ = await _enqueue(queue, max_attempts=3)
    job = await queue.claim(timeout=1)
    assert job.attempts == 1

    assert await queue.reap() == 0  # deadline (3s) not reached yet
    await redis.zadd(queue.k_inflight, {job_id: 1})  # force the deadline into the past
    assert await queue.reap() == 1

    assert (await queue.get(job_id))["status"] == "retrying"
    assert (await queue.stats())["depth"]["processing"] == 0
    await asyncio.sleep(0.3)
    await queue.promote_due()
    again = await queue.claim(timeout=1)
    assert again.id == job_id and again.attempts == 2


async def test_reaper_adopts_orphan_in_processing(queue, redis):
    """Crash between BLMOVE and the claim script leaves an id in `processing` with no deadline."""
    job_id, _ = await _enqueue(queue)
    await redis.blmove(queue.k_ready, queue.k_processing, 1, "RIGHT", "LEFT")
    assert await redis.zscore(queue.k_inflight, job_id) is None

    await queue.reap()
    assert await redis.zscore(queue.k_inflight, job_id) is not None  # adopted, will expire later

    await redis.zadd(queue.k_inflight, {job_id: 1})
    assert await queue.reap() == 1
    assert (await queue.get(job_id))["status"] == "retrying"


async def test_duplicate_delivery_of_finished_job_is_skipped(queue, redis):
    job_id, _ = await _enqueue(queue)
    job = await queue.claim(timeout=1)
    await queue.complete(job, "1")
    await redis.lpush(queue.k_ready, job_id)  # simulate a stray duplicate delivery
    assert await queue.claim(timeout=1) is None  # dropped, not re-run
    assert (await queue.get(job_id))["attempts"] == 1


async def test_expired_job_hash_is_dropped_safely(queue, redis):
    job_id, _ = await _enqueue(queue)
    await redis.delete(queue.job_key(job_id))
    assert await queue.claim(timeout=1) is None
    assert (await queue.stats())["depth"]["processing"] == 0


async def test_concurrent_claims_never_double_deliver(queue):
    ids = {(await _enqueue(queue, payload={"n": i}))[0] for i in range(50)}
    results = await asyncio.gather(*(queue.claim(timeout=1) for _ in range(50)))
    claimed = [j.id for j in results if j]
    assert len(claimed) == 50 and set(claimed) == ids
