import logging
from typing import Optional

from async_lru import alru_cache
from opentelemetry import trace
from prometheus import redis_commands
from redis import ResponseError
from redis.asyncio.client import Redis

logger = logging.getLogger("redis_cache")


@alru_cache(ttl=5)
async def check_cache(redis: Redis, key: str) -> Optional[str]:
    try:
        # Redis operations are auto-instrumented, but we can add custom attributes
        span = trace.get_current_span()
        if span.is_recording():
            span.set_attribute("redis.key", key)
            span.set_attribute("redis.operation", "get")

        item = await redis.get(key)
        redis_commands.labels("get").inc()

        if span.is_recording():
            span.set_attribute("redis.cache_hit", item is not None)

        if item:
            return item.decode("utf-8")
    except ResponseError as err:
        logger.error(err)
    return None


async def write_cache(redis: Redis, key: str, data: str, exp_sec: int) -> None:
    try:
        # Add custom attributes to auto-instrumented span
        span = trace.get_current_span()
        if span.is_recording():
            span.set_attribute("redis.key", key)
            span.set_attribute("redis.operation", "set")
            span.set_attribute("redis.ttl", exp_sec)

        await redis.set(key, value=data, ex=exp_sec)
        redis_commands.labels("set").inc()
    except ResponseError as err:
        logger.error(err)


async def delete_cache(redis: Redis, key: str) -> None:
    try:
        # Add custom attributes to auto-instrumented span
        span = trace.get_current_span()
        if span.is_recording():
            span.set_attribute("redis.key", key)
            span.set_attribute("redis.operation", "delete")

        await redis.delete(key)
        redis_commands.labels("delete").inc()
    except ResponseError as err:
        logger.error(err)
