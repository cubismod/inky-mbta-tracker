import hashlib
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from time import monotonic, time
from typing import Any, cast

from aiohttp import ClientResponse, ClientSession
from anyio import Lock, sleep
from prometheus import record_mbta_api_rate_limit_hit
from redis.asyncio.client import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    def __init__(self, rate_per_minute: int, burst: int) -> None:
        self.rate_per_second = max(rate_per_minute, 1) / 60
        self.capacity = max(1, min(burst, rate_per_minute))
        self.tokens = float(self.capacity)
        self.updated_at = monotonic()
        self._lock = Lock()

    def _refill(self, now: float) -> None:
        elapsed = max(0.0, now - self.updated_at)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_second)
        self.updated_at = now

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = monotonic()
                self._refill(now)
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait_time = (1 - self.tokens) / self.rate_per_second

            if wait_time >= 1:
                logger.warning(
                    "Waiting %.2fs for local MBTA rate limiter token", wait_time
                )
            await sleep(wait_time)


class RedisTokenBucketRateLimiter:
    _SCRIPT = """
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local rate_per_second = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'updated_at_ms')
local tokens = tonumber(bucket[1])
local updated_at_ms = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
end

if updated_at_ms == nil then
    updated_at_ms = now_ms
end

local elapsed_ms = math.max(0, now_ms - updated_at_ms)
tokens = math.min(capacity, tokens + (elapsed_ms / 1000.0) * rate_per_second)

local allowed = 0
local wait_ms = 0

if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
else
    wait_ms = math.ceil(((1 - tokens) / rate_per_second) * 1000)
end

redis.call('HSET', key, 'tokens', tokens, 'updated_at_ms', now_ms)
redis.call('PEXPIRE', key, ttl_ms)

return {allowed, wait_ms}
"""

    def __init__(self, rate_per_minute: int, burst: int, bucket_key: str) -> None:
        self.rate_per_second = max(rate_per_minute, 1) / 60
        self.capacity = max(1, min(burst, rate_per_minute))
        self.bucket_key = bucket_key
        self.ttl_ms = max(int((self.capacity / self.rate_per_second) * 2000), 60_000)

    async def acquire(self, redis: Redis) -> None:
        while True:
            result = cast(
                list[int | str],
                await cast(Any, redis).eval(
                    self._SCRIPT,
                    1,
                    self.bucket_key,
                    int(time() * 1000),
                    self.rate_per_second,
                    self.capacity,
                    self.ttl_ms,
                ),
            )
            allowed, wait_ms = result

            if bool(int(allowed)):
                return

            delay = max(int(wait_ms), 1) / 1000
            if delay >= 1:
                logger.warning(
                    "Waiting %.2fs for shared MBTA rate limiter token", delay
                )
            await sleep(delay)


def _build_bucket_key() -> str:
    key_override = os.getenv("IMT_MBTA_RATE_LIMIT_KEY")
    if key_override:
        return f"mbta:rate-limit:{key_override}"

    auth_token = os.getenv("AUTH_TOKEN", "")
    auth_hash = hashlib.sha1(auth_token.encode("utf-8")).hexdigest()[:12]
    return f"mbta:rate-limit:{auth_hash or 'default'}"


MBTA_RATE_LIMIT_PER_MINUTE = max(
    1, int(os.getenv("IMT_MBTA_RATE_LIMIT_PER_MINUTE", "900"))
)
MBTA_RATE_LIMIT_BURST = max(1, int(os.getenv("IMT_MBTA_RATE_LIMIT_BURST", "30")))
local_mbta_rate_limiter = TokenBucketRateLimiter(
    rate_per_minute=MBTA_RATE_LIMIT_PER_MINUTE,
    burst=MBTA_RATE_LIMIT_BURST,
)
shared_mbta_rate_limiter = RedisTokenBucketRateLimiter(
    rate_per_minute=MBTA_RATE_LIMIT_PER_MINUTE,
    burst=MBTA_RATE_LIMIT_BURST,
    bucket_key=_build_bucket_key(),
)


@asynccontextmanager
async def rate_limited_get(
    session: ClientSession, redis: Redis, url: str, **kwargs: Any
) -> AsyncIterator[ClientResponse]:
    try:
        await shared_mbta_rate_limiter.acquire(redis)
    except RedisError as err:
        logger.warning(
            "Shared MBTA rate limiter unavailable, falling back to local limiter",
            exc_info=err,
        )
        await local_mbta_rate_limiter.acquire()

    async with session.get(url, **kwargs) as response:
        if response.status == 429:
            record_mbta_api_rate_limit_hit(url)
        yield response
