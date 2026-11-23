"""Reliable job queue on Redis.

Data model (``p`` = key prefix):

    p:job:<id>        hash   job state (type, payload, status, attempts, ...)
    p:queue:ready     list   job ids waiting to run          (LPUSH in, claimed from the tail)
    p:queue:delayed   zset   job ids scheduled for later     (score = run-at, Redis clock)
    p:queue:processing list  job ids claimed by workers
    p:queue:inflight  zset   claimed job ids                 (score = visibility deadline)
    p:queue:dead      list   job ids that exhausted retries  (dead-letter queue)
    p:idem:<owner>:<key>     idempotency key -> job id
    p:stats           hash   cumulative counters

Delivery semantics: **at-least-once**. A worker that dies mid-job leaves the id in
``processing``; once its visibility deadline passes, the reaper retries the job elsewhere.
Handlers must therefore be idempotent. The claim script skips jobs that already finished,
which suppresses most duplicates caused by a slow worker finishing after a reclaim.

All multi-key state transitions are Lua scripts, so they are atomic. All timestamps come
from the Redis server clock, so skew between hosts cannot reorder retries.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis

from .config import Settings

# --------------------------------------------------------------------------------------
# Lua scripts
# --------------------------------------------------------------------------------------

_NOW = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
"""

# KEYS: idem, job, ready, delayed, stats
# ARGV: job_id, idem_ttl, job_ttl, delay_s, use_idem('1'/'0'), field, value, field, value ...
ENQUEUE_LUA = (
    _NOW
    + """
if ARGV[5] == '1' then
  local existing = redis.call('GET', KEYS[1])
  if existing then
    return {existing, 0}
  end
  redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
end

local hset = {'HSET', KEYS[2]}
for i = 6, #ARGV do hset[#hset + 1] = ARGV[i] end
redis.call(unpack(hset))
redis.call('HSET', KEYS[2], 'created_at', tostring(now), 'updated_at', tostring(now))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))

local delay = tonumber(ARGV[4])
if delay > 0 then
  redis.call('HSET', KEYS[2], 'status', 'scheduled')
  redis.call('ZADD', KEYS[4], now + delay, ARGV[1])
else
  redis.call('LPUSH', KEYS[3], ARGV[1])
end
redis.call('HINCRBY', KEYS[5], 'enqueued', 1)
return {ARGV[1], 1}
"""
)

# KEYS: delayed, ready      ARGV: limit
PROMOTE_LUA = (
    _NOW
    + """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('LPUSH', KEYS[2], id)
end
return #ids
"""
)

# KEYS: job, inflight, processing      ARGV: job_id, visibility_s
CLAIM_LUA = (
    _NOW
    + """
local f = redis.call('HMGET', KEYS[1], 'type', 'payload', 'attempts', 'max_attempts', 'status')
if (not f[1]) or f[5] == 'succeeded' or f[5] == 'dead' then
  -- job expired or already finished (duplicate delivery): drop it
  redis.call('LREM', KEYS[3], 1, ARGV[1])
  redis.call('ZREM', KEYS[2], ARGV[1])
  return false
end
local attempts = redis.call('HINCRBY', KEYS[1], 'attempts', 1)
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), ARGV[1])
redis.call('HSET', KEYS[1], 'status', 'running', 'started_at', tostring(now), 'updated_at', tostring(now))
return {f[1], f[2], tostring(attempts), f[4]}
"""
)

# KEYS: job, processing, inflight, stats      ARGV: job_id, result_json
COMPLETE_LUA = (
    _NOW
    + """
redis.call('LREM', KEYS[2], 1, ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('HSET', KEYS[1], 'status', 'succeeded', 'result', ARGV[2],
           'finished_at', tostring(now), 'updated_at', tostring(now))
redis.call('HDEL', KEYS[1], 'error')
redis.call('HINCRBY', KEYS[4], 'succeeded', 1)
return 1
"""
)

# KEYS: job, processing, inflight, delayed, stats      ARGV: job_id, delay_s, error
RETRY_LUA = (
    _NOW
    + """
redis.call('LREM', KEYS[2], 1, ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('ZADD', KEYS[4], now + tonumber(ARGV[2]), ARGV[1])
redis.call('HSET', KEYS[1], 'status', 'retrying', 'error', ARGV[3], 'updated_at', tostring(now))
redis.call('HINCRBY', KEYS[5], 'retried', 1)
return 1
"""
)

# KEYS: job, processing, inflight, dead, stats      ARGV: job_id, error
DEAD_LUA = (
    _NOW
    + """
redis.call('LREM', KEYS[2], 1, ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('LPUSH', KEYS[4], ARGV[1])
redis.call('LTRIM', KEYS[4], 0, 9999)
redis.call('HSET', KEYS[1], 'status', 'dead', 'error', ARGV[2],
           'finished_at', tostring(now), 'updated_at', tostring(now))
redis.call('HINCRBY', KEYS[5], 'dead', 1)
return 1
"""
)

# KEYS: job, dead, ready      ARGV: job_id
REQUEUE_DEAD_LUA = (
    _NOW
    + """
if redis.call('HGET', KEYS[1], 'status') ~= 'dead' then return 0 end
redis.call('LREM', KEYS[2], 1, ARGV[1])
redis.call('HSET', KEYS[1], 'status', 'queued', 'attempts', 0, 'updated_at', tostring(now))
redis.call('HDEL', KEYS[1], 'error', 'finished_at')
redis.call('LPUSH', KEYS[3], ARGV[1])
return 1
"""
)

# KEYS: processing, inflight      ARGV: job_id, visibility_s
# Adopts a job that sits in `processing` without an inflight deadline (worker crashed between
# the blocking move and the claim script), so the reaper can eventually retry it.
ADOPT_LUA = (
    _NOW
    + """
if redis.call('LPOS', KEYS[1], ARGV[1]) and not redis.call('ZSCORE', KEYS[2], ARGV[1]) then
  redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), ARGV[1])
  return 1
end
return 0
"""
)


@dataclass(slots=True)
class Job:
    id: str
    type: str
    payload: dict[str, Any]
    attempts: int
    max_attempts: int


class JobQueue:
    def __init__(self, redis: Redis, settings: Settings):
        self.r = redis
        self.s = settings
        p = settings.key_prefix
        self.k_ready = f"{p}:queue:ready"
        self.k_delayed = f"{p}:queue:delayed"
        self.k_processing = f"{p}:queue:processing"
        self.k_inflight = f"{p}:queue:inflight"
        self.k_dead = f"{p}:queue:dead"
        self.k_stats = f"{p}:stats"
        self._p = p

        reg = redis.register_script
        self._enqueue = reg(ENQUEUE_LUA)
        self._promote = reg(PROMOTE_LUA)
        self._claim = reg(CLAIM_LUA)
        self._complete = reg(COMPLETE_LUA)
        self._retry = reg(RETRY_LUA)
        self._dead = reg(DEAD_LUA)
        self._requeue_dead = reg(REQUEUE_DEAD_LUA)
        self._adopt = reg(ADOPT_LUA)

    def job_key(self, job_id: str) -> str:
        return f"{self._p}:job:{job_id}"

    # ------------------------------------------------------------------ producers

    async def enqueue(
        self,
        *,
        job_type: str,
        payload: dict[str, Any],
        owner: str,
        max_attempts: int | None = None,
        delay_s: float = 0.0,
        idempotency_key: str | None = None,
    ) -> tuple[str, bool]:
        """Create a job. Returns ``(job_id, created)``; ``created`` is False when the
        idempotency key was already used by this owner (the original job id is returned)."""
        job_id = uuid.uuid4().hex
        fields = {
            "id": job_id,
            "type": job_type,
            "payload": json.dumps(payload, separators=(",", ":")),
            "status": "queued",
            "attempts": 0,
            "max_attempts": max_attempts or self.s.max_attempts_default,
            "owner": owner,
        }
        flat: list[Any] = []
        for k, v in fields.items():
            flat += [k, v]
        idem_key = f"{self._p}:idem:{owner}:{idempotency_key}" if idempotency_key else f"{self._p}:idem:unused"
        jid, created = await self._enqueue(
            keys=[idem_key, self.job_key(job_id), self.k_ready, self.k_delayed, self.k_stats],
            args=[
                job_id,
                self.s.idempotency_ttl_s,
                self.s.job_ttl_s,
                delay_s,
                "1" if idempotency_key else "0",
                *flat,
            ],
        )
        return jid, bool(int(created))

    async def get(self, job_id: str) -> dict[str, Any] | None:
        data = await self.r.hgetall(self.job_key(job_id))
        if not data:
            return None
        for field in ("payload", "result"):
            if field in data:
                try:
                    data[field] = json.loads(data[field])
                except json.JSONDecodeError:
                    pass
        for field in ("attempts", "max_attempts"):
            data[field] = int(data.get(field, 0))
        for field in ("created_at", "updated_at", "started_at", "finished_at"):
            if field in data:
                data[field] = float(data[field])
        return data

    async def requeue_dead(self, job_id: str) -> bool:
        res = await self._requeue_dead(keys=[self.job_key(job_id), self.k_dead, self.k_ready], args=[job_id])
        return bool(int(res))

    # ------------------------------------------------------------------ consumers

    async def promote_due(self, limit: int = 500) -> int:
        return int(await self._promote(keys=[self.k_delayed, self.k_ready], args=[limit]))

    async def claim(self, timeout: float = 1.0) -> Job | None:  # noqa: ASYNC109 - Redis BLMOVE wait, not an asyncio timeout
        """Block up to ``timeout`` seconds for a job and claim it."""
        job_id = await self.r.blmove(self.k_ready, self.k_processing, timeout, "RIGHT", "LEFT")
        if job_id is None:
            return None
        res = await self._claim(
            keys=[self.job_key(job_id), self.k_inflight, self.k_processing],
            args=[job_id, self.s.visibility_timeout_s],
        )
        if not res:
            return None
        jtype, payload, attempts, max_attempts = res
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            parsed = {}
        return Job(job_id, jtype, parsed, int(attempts), int(max_attempts))

    async def complete(self, job: Job, result_json: str) -> None:
        await self._complete(
            keys=[self.job_key(job.id), self.k_processing, self.k_inflight, self.k_stats],
            args=[job.id, result_json],
        )

    def backoff_delay(self, attempts: int) -> float:
        """Exponential backoff with equal jitter: half fixed, half random."""
        exp = min(self.s.backoff_max_s, self.s.backoff_base_s * (2 ** max(0, attempts - 1)))
        return exp / 2 + random.uniform(0, exp / 2)

    async def fail(self, job: Job, error: str, *, retryable: bool = True) -> str:
        """Record a failure. Returns the resulting state: ``retrying`` or ``dead``."""
        error = error[:1000]
        if retryable and job.attempts < job.max_attempts:
            await self._retry(
                keys=[
                    self.job_key(job.id),
                    self.k_processing,
                    self.k_inflight,
                    self.k_delayed,
                    self.k_stats,
                ],
                args=[job.id, self.backoff_delay(job.attempts), error],
            )
            return "retrying"
        await self._dead(
            keys=[
                self.job_key(job.id),
                self.k_processing,
                self.k_inflight,
                self.k_dead,
                self.k_stats,
            ],
            args=[job.id, error],
        )
        return "dead"

    async def reap(self) -> int:
        """Reclaim jobs whose visibility deadline passed. Returns how many were reclaimed."""
        t = await self.r.time()
        now = t[0] + t[1] / 1_000_000
        reclaimed = 0

        for job_id in await self.r.zrangebyscore(self.k_inflight, "-inf", now):
            # ZREM returning 1 means this reaper owns the expiry; concurrent reapers skip it.
            if await self.r.zrem(self.k_inflight, job_id) != 1:
                continue
            meta = await self.r.hmget(self.job_key(job_id), "status", "attempts", "max_attempts", "type")
            status, attempts, max_attempts, jtype = meta
            if jtype is None or status in ("succeeded", "dead"):
                await self.r.lrem(self.k_processing, 1, job_id)
                continue
            job = Job(job_id, jtype, {}, int(attempts or 0), int(max_attempts or 1))
            await self.fail(job, "visibility timeout exceeded (worker crashed or hung)")
            await self.r.hincrby(self.k_stats, "reaped", 1)
            reclaimed += 1

        for job_id in await self.r.lrange(self.k_processing, 0, -1):
            await self._adopt(
                keys=[self.k_processing, self.k_inflight],
                args=[job_id, self.s.visibility_timeout_s],
            )
        return reclaimed

    # ------------------------------------------------------------------ introspection

    async def stats(self) -> dict[str, Any]:
        async with self.r.pipeline(transaction=False) as pipe:
            pipe.llen(self.k_ready)
            pipe.zcard(self.k_delayed)
            pipe.llen(self.k_processing)
            pipe.llen(self.k_dead)
            pipe.hgetall(self.k_stats)
            ready, delayed, processing, dead, counters = await pipe.execute()
        return {
            "depth": {"ready": ready, "delayed": delayed, "processing": processing, "dead": dead},
            "counters": {k: int(v) for k, v in counters.items()},
        }
