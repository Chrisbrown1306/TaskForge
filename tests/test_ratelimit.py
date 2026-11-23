import asyncio

import pytest

from app.ratelimit import RateLimiter


async def test_allows_up_to_capacity_then_blocks(redis):
    rl = RateLimiter(redis, capacity=5, refill_per_sec=0.001, prefix="t")
    results = [await rl.check("u1") for _ in range(7)]
    assert [r.allowed for r in results] == [True] * 5 + [False] * 2
    assert results[0].remaining == 4
    assert results[-1].retry_after_s > 0


async def test_refills_over_time(redis):
    rl = RateLimiter(redis, capacity=2, refill_per_sec=20, prefix="t")
    assert (await rl.check("u1")).allowed
    assert (await rl.check("u1")).allowed
    assert not (await rl.check("u1")).allowed
    await asyncio.sleep(0.15)  # ~3 tokens refilled, capped at capacity 2
    assert (await rl.check("u1")).allowed


async def test_identities_are_isolated(redis):
    rl = RateLimiter(redis, capacity=1, refill_per_sec=0.001, prefix="t")
    assert (await rl.check("a")).allowed
    assert not (await rl.check("a")).allowed
    assert (await rl.check("b")).allowed


async def test_atomic_under_concurrency(redis):
    """200 simultaneous callers must never get more than `capacity` tokens in total."""
    rl = RateLimiter(redis, capacity=50, refill_per_sec=0.001, prefix="t")
    results = await asyncio.gather(*(rl.check("shared") for _ in range(200)))
    assert sum(r.allowed for r in results) == 50


def test_rejects_bad_config(redis):
    with pytest.raises(ValueError):
        RateLimiter(redis, capacity=0, refill_per_sec=1)
    with pytest.raises(ValueError):
        RateLimiter(redis, capacity=1, refill_per_sec=0)
