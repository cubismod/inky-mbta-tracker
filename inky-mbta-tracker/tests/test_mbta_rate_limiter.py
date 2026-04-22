from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from anyio import current_time
from mbta_rate_limiter import (
    RedisTokenBucketRateLimiter,
    TokenBucketRateLimiter,
    rate_limited_get,
)
from redis.exceptions import RedisError


@pytest.mark.anyio("asyncio")
async def test_rate_limiter_honors_burst_capacity() -> None:
    limiter = TokenBucketRateLimiter(rate_per_minute=60, burst=2)

    start = current_time()
    await limiter.acquire()
    await limiter.acquire()

    assert current_time() - start < 0.05


@pytest.mark.anyio("asyncio")
async def test_rate_limiter_waits_when_bucket_is_empty() -> None:
    limiter = TokenBucketRateLimiter(rate_per_minute=1200, burst=1)

    await limiter.acquire()
    start = current_time()
    await limiter.acquire()

    assert current_time() - start >= 0.04


@pytest.mark.anyio("asyncio")
async def test_redis_rate_limiter_acquires_token() -> None:
    redis = AsyncMock()
    redis.eval.return_value = [1, 0]
    limiter = RedisTokenBucketRateLimiter(
        rate_per_minute=900,
        burst=30,
        bucket_key="mbta:rate-limit:test",
    )

    await limiter.acquire(redis)

    redis.eval.assert_awaited_once()


@pytest.mark.anyio("asyncio")
async def test_rate_limited_get_falls_back_to_local_limiter() -> None:
    redis = AsyncMock()
    redis.eval.side_effect = RedisError("boom")

    response = AsyncMock()
    response_cm = AsyncMock()
    response_cm.__aenter__.return_value = response
    response_cm.__aexit__.return_value = None

    session = MagicMock()
    session.get.return_value = response_cm

    with patch(
        "mbta_rate_limiter.local_mbta_rate_limiter.acquire", new=AsyncMock()
    ) as acquire:
        async with rate_limited_get(session, redis, "/stops/test"):
            pass

    acquire.assert_awaited_once()
    session.get.assert_called_once_with("/stops/test")
