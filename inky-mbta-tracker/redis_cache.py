import logging
from typing import Optional

from async_lru import alru_cache
from opentelemetry import trace
from otel_utils import add_cache_key_attributes, add_span_attributes
from prometheus import redis_commands
from redis import ResponseError
from redis.asyncio.client import Redis

logger = logging.getLogger("redis_cache")


@alru_cache(ttl=5)
async def get_cache(redis: Redis, key: str) -> Optional[str]:
    try:
        # Redis operations are auto-instrumented, but we can add custom attributes
        span = trace.get_current_span()
        add_span_attributes(span, {"redis.operation": "get"})
        add_cache_key_attributes(span, key, attr_prefix="redis.key")

        item = await redis.get(key)
        redis_commands.labels("get").inc()

        add_span_attributes(span, {"redis.cache_hit": item is not None})

        if item:
            return item.decode("utf-8")
    except ResponseError as err:
        logger.error(err)
    return None


async def write_cache(redis: Redis, key: str, data: str, exp_sec: int) -> None:
    try:
        # Add custom attributes to auto-instrumented span
        span = trace.get_current_span()
        add_span_attributes(span, {"redis.operation": "set", "redis.ttl": exp_sec})
        add_cache_key_attributes(span, key, attr_prefix="redis.key")

        await redis.set(key, value=data, ex=exp_sec)
        redis_commands.labels("set").inc()
    except ResponseError as err:
        logger.error(err)


async def delete_cache(redis: Redis, key: str) -> None:
    try:
        # Add custom attributes to auto-instrumented span
        span = trace.get_current_span()
        add_span_attributes(span, {"redis.operation": "delete"})
        add_cache_key_attributes(span, key, attr_prefix="redis.key")

        await redis.delete(key)
        redis_commands.labels("delete").inc()
    except ResponseError as err:
        logger.error(err)
