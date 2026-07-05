import json
from unittest.mock import AsyncMock

import pytest
from redis import ResponseError
from redis_cache import get_cache, write_cache


@pytest.mark.anyio("asyncio")
async def test_get_cache_hit_decodes_bytes() -> None:
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b'{"ok": 1}'

    res = await get_cache(mock_redis, "key")
    assert res == json.dumps({"ok": 1})
    mock_redis.get.assert_called_once_with("key")


@pytest.mark.anyio("asyncio")
async def test_get_cache_miss_returns_none() -> None:
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None

    res = await get_cache(mock_redis, "missing")
    assert res is None
    mock_redis.get.assert_called_once_with("missing")


@pytest.mark.anyio("asyncio")
async def test_get_cache_response_error_logged_and_none() -> None:
    mock_redis = AsyncMock()
    mock_redis.get.side_effect = ResponseError("boom")

    res = await get_cache(mock_redis, "err")
    assert res is None


@pytest.mark.anyio("asyncio")
async def test_get_cache_calls_redis_each_time() -> None:
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b"cached"

    first = await get_cache(mock_redis, "ttl-key")
    second = await get_cache(mock_redis, "ttl-key")

    assert first == "cached"
    assert second == "cached"
    assert mock_redis.get.call_count == 2


@pytest.mark.anyio("asyncio")
async def test_write_cache_success_sets_value_with_expiry() -> None:
    mock_redis = AsyncMock()

    await write_cache(mock_redis, "k", "data", 123)
    mock_redis.set.assert_called_once_with("k", value="data", ex=123)


@pytest.mark.anyio("asyncio")
async def test_write_cache_response_error_is_logged() -> None:
    mock_redis = AsyncMock()
    mock_redis.set.side_effect = ResponseError("nope")

    # Should not raise
    await write_cache(mock_redis, "k", "data", 10)
