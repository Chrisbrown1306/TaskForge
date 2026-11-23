"""Job handlers. Register new ones with ``@register("name")``.

Handlers receive the job payload and a ``JobContext`` and return a JSON-serialisable result.
Because delivery is at-least-once, handlers must be safe to run more than once.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class JobContext:
    job_id: str
    attempt: int  # 1-based


Handler = Callable[[dict[str, Any], JobContext], Awaitable[Any]]
HANDLERS: dict[str, Handler] = {}


def register(name: str) -> Callable[[Handler], Handler]:
    def deco(fn: Handler) -> Handler:
        HANDLERS[name] = fn
        return fn

    return deco


@register("echo")
async def echo(payload: dict[str, Any], ctx: JobContext) -> Any:
    return payload


@register("sum")
async def sum_numbers(payload: dict[str, Any], ctx: JobContext) -> Any:
    numbers = payload.get("numbers", [])
    if not isinstance(numbers, list) or not all(isinstance(n, int | float) for n in numbers):
        raise ValueError("payload.numbers must be a list of numbers")
    return {"sum": sum(numbers), "count": len(numbers)}


@register("sleep")
async def sleep(payload: dict[str, Any], ctx: JobContext) -> Any:
    seconds = min(float(payload.get("seconds", 0.1)), 20.0)
    await asyncio.sleep(seconds)
    return {"slept": seconds}


@register("fail")
async def fail(payload: dict[str, Any], ctx: JobContext) -> Any:
    """Always raises. Useful to demo retries and the dead-letter queue."""
    raise RuntimeError(str(payload.get("message", "boom")))


@register("flaky")
async def flaky(payload: dict[str, Any], ctx: JobContext) -> Any:
    """Fails until ``payload.succeed_on_attempt`` (default 3). Demonstrates backoff."""
    succeed_on = int(payload.get("succeed_on_attempt", 3))
    if ctx.attempt < succeed_on:
        raise RuntimeError(f"transient failure on attempt {ctx.attempt}")
    return {"attempts_needed": ctx.attempt}
