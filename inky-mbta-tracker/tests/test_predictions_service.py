from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, cast

import pytest
from aiohttp import ClientSession
from api.services import predictions
from redis.asyncio import Redis

PREDICTION_BODY = """
{
  "data": [
    {
      "type": "prediction",
      "id": "prediction-1",
      "relationships": {
        "vehicle": null,
        "stop": {"data": {"type": "stop", "id": "stop-1"}},
        "trip": {"data": {"type": "trip", "id": "trip-1"}},
        "route": {"data": {"type": "route", "id": "Red"}}
      },
      "attributes": {
        "arrival_time": "2026-06-06T12:00:00-04:00",
        "revenue": "REVENUE"
      }
    }
  ]
}
"""


class FakeResponse:
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self._body = body

    async def text(self) -> str:
        return self._body


async def _null_cache(*args: Any, **kwargs: Any) -> None:
    return None


@pytest.mark.anyio
async def test_batch_fetch_returns_predicted_arrival_times(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api_calls: list[str] = []

    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        api_calls.append(url)
        yield FakeResponse(200, PREDICTION_BODY)

    monkeypatch.setattr(predictions, "get_cache", _null_cache)
    monkeypatch.setattr(predictions, "write_cache", _null_cache)
    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)

    result = await predictions.batch_fetch_trip_predictions(
        cast(ClientSession, None), cast(Redis, None), ["trip-1"]
    )

    assert len(result) == 1
    assert ("trip-1", "stop-1") in result
    assert isinstance(result[("trip-1", "stop-1")], datetime)
    assert result[("trip-1", "stop-1")] == datetime.fromisoformat(
        "2026-06-06T12:00:00-04:00"
    )
    assert len(api_calls) == 1


@pytest.mark.anyio
async def test_batch_fetch_handles_api_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        yield FakeResponse(500, "{}")

    monkeypatch.setattr(predictions, "get_cache", _null_cache)
    monkeypatch.setattr(predictions, "write_cache", _null_cache)
    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)

    result = await predictions.batch_fetch_trip_predictions(
        cast(ClientSession, None), cast(Redis, None), ["trip-1"]
    )

    assert result == {}


@pytest.mark.anyio
async def test_batch_fetch_uses_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api_calls: list[str] = []

    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        api_calls.append(url)
        yield FakeResponse(200, PREDICTION_BODY)

    monkeypatch.setattr(predictions, "get_cache", _null_cache)
    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)

    async def fake_get_cache(redis: Redis, key: str) -> str | None:
        return PREDICTION_BODY

    monkeypatch.setattr(predictions, "get_cache", fake_get_cache)

    result = await predictions.batch_fetch_trip_predictions(
        cast(ClientSession, None), cast(Redis, None), ["trip-1", "trip-1"]
    )

    assert ("trip-1", "stop-1") in result
    assert result[("trip-1", "stop-1")] == datetime.fromisoformat(
        "2026-06-06T12:00:00-04:00"
    )
    assert len(api_calls) == 0
