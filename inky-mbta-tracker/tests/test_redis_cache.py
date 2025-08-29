import json
from unittest.mock import AsyncMock

import pytest
from redis import ResponseError
from redis_cache import check_cache, write_cache


@pytest.fixture(autouse=True)
def clear_alru_cache() -> None:
    # Ensure per-test isolation for alru_cache
    try:
        check_cache.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass


@pytest.mark.anyio("asyncio")
async def test_check_cache_hit_decodes_bytes() -> None:
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b'{"ok": 1}'

    res = await check_cache(mock_redis, "key")
    assert res == json.dumps({"ok": 1})
    mock_redis.get.assert_called_once_with("key")


@pytest.mark.anyio("asyncio")
async def test_check_cache_miss_returns_none() -> None:
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None

    res = await check_cache(mock_redis, "missing")
    assert res is None
    mock_redis.get.assert_called_once_with("missing")


@pytest.mark.anyio("asyncio")
async def test_check_cache_response_error_logged_and_none() -> None:
    mock_redis = AsyncMock()
    mock_redis.get.side_effect = ResponseError("boom")

    res = await check_cache(mock_redis, "err")
    assert res is None


@pytest.mark.anyio("asyncio")
async def test_check_cache_ttl_prevents_duplicate_calls() -> None:
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b"cached"

    first = await check_cache(mock_redis, "ttl-key")
    second = await check_cache(mock_redis, "ttl-key")

    assert first == "cached"
    assert second == "cached"
    # Due to alru_cache(ttl=5), only first call hits Redis
    mock_redis.get.assert_called_once()


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
