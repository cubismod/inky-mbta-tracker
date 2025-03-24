import logging
from typing import Optional

from prometheus import redis_commands
from redis import ResponseError
from redis.asyncio.client import Redis

logger = logging.getLogger("redis_cache")


async def check_cache(redis: Redis, key: str) -> Optional[str]:
    try:
        item = await redis.get(key)
        redis_commands.labels("get").inc()
        if item:
            return item.decode("utf-8")
    except ResponseError as err:
        logger.error(err)
    return None


async def write_cache(redis: Redis, key: str, data: str, exp_sec: int) -> None:
    try:
        await redis.set(key, value=data, ex=exp_sec)
        redis_commands.labels("set").inc()
    except ResponseError as err:
        logger.error(err)
