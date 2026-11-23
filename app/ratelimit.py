"""Distributed token-bucket rate limiter.

The whole read-modify-write runs inside one Lua script, so it is atomic across any number
of API instances. Time comes from the Redis server clock (``TIME``), which avoids clock
skew between app servers.
"""

from dataclasses import dataclass

from redis.asyncio import Redis

BUCKET_LUA = """
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate)
local allowed = 0
local retry_after = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after = (cost - tokens) / rate
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('EXPIRE', KEYS[1], math.ceil(capacity / rate) * 2 + 1)
return {allowed, tostring(tokens), tostring(retry_after)}
"""


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    allowed: bool
    remaining: int
    retry_after_s: float


class RateLimiter:
    def __init__(self, redis: Redis, *, capacity: int, refill_per_sec: float, prefix: str = "tf"):
        if capacity < 1 or refill_per_sec <= 0:
            raise ValueError("capacity must be >= 1 and refill_per_sec > 0")
        self._script = redis.register_script(BUCKET_LUA)
        self.capacity = capacity
        self.refill_per_sec = refill_per_sec
        self._prefix = prefix

    async def check(self, identity: str, cost: int = 1) -> RateLimitResult:
        allowed, tokens, retry_after = await self._script(
            keys=[f"{self._prefix}:rl:{identity}"],
            args=[self.capacity, self.refill_per_sec, cost],
        )
        return RateLimitResult(
            allowed=bool(int(allowed)),
            remaining=int(float(tokens)),
            retry_after_s=float(retry_after),
        )
