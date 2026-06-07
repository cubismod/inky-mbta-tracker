from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import pytest
from aiohttp import ClientSession
from api.services import predictions
from api.services.predictions import MBTAUpstreamError, fetch_predictions
from fastapi.routing import APIRoute
from redis.asyncio import Redis

PREDICTIONS_BODY = """
{
  "data": [
    {
      "type": "prediction",
      "id": "prediction-1",
      "relationships": {
        "vehicle": null,
        "trip": {"data": {"type": "trip", "id": "trip-123"}},
        "stop": {"data": {"type": "stop", "id": "place-sstat"}},
        "route": {"data": {"type": "route", "id": "Red"}}
      },
      "attributes": {
        "arrival_time": "2026-06-06T12:00:00-04:00",
        "departure_time": "2026-06-06T12:01:00-04:00",
        "direction_id": 0,
        "revenue": "REVENUE"
      }
    }
  ]
}
"""


class FakeSession:
    closed = False


class FakeResponse:
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self._body = body

    async def text(self) -> str:
        return self._body


@pytest.mark.anyio
async def test_fetch_predictions_for_trip_requests_mbta_trip_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        calls.append(url)
        yield FakeResponse(200, PREDICTIONS_BODY)

    monkeypatch.setattr(predictions, "MBTA_AUTH", "secret")
    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)

    result = await fetch_predictions(
        cast(ClientSession, FakeSession()), cast(Redis, object()), trip_id="trip 123"
    )

    assert result.body == PREDICTIONS_BODY
    assert result.count == 1
    assert calls == ["/predictions?filter%5Btrip%5D=trip+123&api_key=secret"]


@pytest.mark.anyio
async def test_fetch_predictions_supports_latitude_longitude_radius_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        calls.append(url)
        yield FakeResponse(200, PREDICTIONS_BODY)

    monkeypatch.setattr(predictions, "MBTA_AUTH", None)
    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)

    result = await fetch_predictions(
        cast(ClientSession, FakeSession()),
        cast(Redis, object()),
        trip_id="trip-123",
        latitude=42.352271,
        longitude=-71.055242,
        radius=0.02,
    )

    assert result.count == 1
    assert calls == [
        "/predictions?filter%5Btrip%5D=trip-123&filter%5Blatitude%5D=42.352271&filter%5Blongitude%5D=-71.055242&filter%5Bradius%5D=0.02"
    ]


@pytest.mark.anyio
async def test_fetch_predictions_for_trip_raises_for_non_200(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        yield FakeResponse(404, '{"errors":[]}')

    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)

    with pytest.raises(MBTAUpstreamError) as err:
        await fetch_predictions(
            cast(ClientSession, FakeSession()),
            cast(Redis, object()),
            trip_id="trip-123",
        )

    assert err.value.status_code == 404


def test_api_server_registers_predictions_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("IMT_RATE_LIMITING_ENABLED", "false")

    from api_server import create_app

    app = create_app()

    assert any(
        route.path == "/predictions"
        for route in app.routes
        if isinstance(route, APIRoute)
    )
