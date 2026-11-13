import json
from contextlib import asynccontextmanager
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from aiohttp import ClientSession
from api.core import DIParams
from api.endpoints.stops import get_route_stops
from api.middleware import cache_middleware
from api.models import RouteStopAttributes
from api.services import stops
from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRoute, iter_route_contexts
from redis.asyncio import Redis

ATTRIBUTES = {
    "name": "Alewife",
    "latitude": 42.395428,
    "longitude": -71.142483,
    "vehicle_type": 1,
    "wheelchair_boarding": 1,
    "description": "Alewife - Red Line",
    "platform_name": "Red Line",
    "platform_code": None,
}


@pytest.fixture
def upstream(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    @asynccontextmanager
    async def fake_get(session: ClientSession, redis: Redis, url: str, **kwargs: Any):
        calls.append({"url": url, **kwargs})
        response = AsyncMock()
        response.status = 200
        response.text.return_value = json.dumps(
            {
                "data": [
                    {
                        "id": "place-alfcl",
                        "type": "stop",
                        "attributes": {
                            **ATTRIBUTES,
                            "location_type": 1,
                            "address": "extra",
                        },
                        "relationships": {},
                    }
                ]
            }
        )
        yield response

    monkeypatch.setattr(stops, "rate_limited_get", fake_get)
    monkeypatch.setattr(stops, "MBTA_AUTH", "test-key")
    return calls


@pytest.mark.anyio
async def test_fetch_stops_filters_route_and_attributes(
    upstream: list[dict[str, Any]],
) -> None:
    result = await stops.fetch_route_stops(
        cast(ClientSession, object()), cast(Redis, object()), "Green-B"
    )
    assert result.model_dump() == {
        "data": [
            {
                "id": "place-alfcl",
                "type": "stop",
                "attributes": ATTRIBUTES,
            }
        ]
    }
    assert upstream == [
        {
            "url": "/stops",
            "params": {
                "filter[route]": "Green-B",
                "fields[stop]": ",".join(RouteStopAttributes.model_fields),
                "api_key": "test-key",
            },
        }
    ]


@pytest.mark.anyio
async def test_route_stops_cache_uses_month_and_separates_routes(
    monkeypatch: pytest.MonkeyPatch, upstream: list[dict[str, Any]]
) -> None:
    store: dict[str, str] = {}
    writes: list[tuple[str, int]] = []

    async def get_cache(redis: Redis, key: str) -> str | None:
        return store.get(key)

    async def write_cache(redis: Redis, key: str, body: str, ttl: int) -> None:
        store[key] = body
        writes.append((key, ttl))

    monkeypatch.setattr(cache_middleware, "get_cache", get_cache)
    monkeypatch.setattr(cache_middleware, "write_cache", write_cache)
    middleware = cache_middleware.create_cache_middleware(include_paths={"/stops/*"})
    commons = DIParams(cast(ClientSession, object()), cast(Redis, object()))

    async def call_next(request: Request) -> StreamingResponse:
        request.scope["endpoint"] = get_route_stops
        response = await get_route_stops(
            request, commons, request.path_params["route_id"]
        )
        return StreamingResponse(iter([response.body]), media_type="application/json")

    for route_id in ("Red", "Red", "Blue"):
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": f"/stops/{route_id}",
                "path_params": {"route_id": route_id},
                "headers": [],
                "query_string": b"",
                "scheme": "http",
                "server": ("testserver", 80),
                "client": ("127.0.0.1", 1234),
            }
        )
        response = await middleware(request, call_next)
        assert response.status_code == 200
        assert json.loads(bytes(response.body))["data"][0]["attributes"] == ATTRIBUTES

    assert len(upstream) == 2
    assert writes == [
        ("api_cache:GET:/stops/Red", 2592000),
        ("api_cache:GET:/stops/Blue", 2592000),
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("status", [400, 429, 503])
async def test_upstream_errors_are_not_returned_as_success(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    @asynccontextmanager
    async def fake_get(*args: Any, **kwargs: Any):
        response = AsyncMock()
        response.status = status
        yield response

    monkeypatch.setattr(stops, "rate_limited_get", fake_get)
    commons = DIParams(cast(ClientSession, object()), cast(Redis, object()))
    request = Request(
        {"type": "http", "method": "GET", "path": "/stops/Red", "headers": []}
    )
    with pytest.raises(HTTPException) as error:
        await get_route_stops(request, commons, "Red")
    assert error.value.status_code == status


def test_api_registers_stops_and_enables_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    import api_server

    factory = api_server.create_cache_middleware
    observed: dict[str, Any] = {}

    def capture(**kwargs: Any):
        observed.update(kwargs)
        return factory(**kwargs)

    monkeypatch.setattr(api_server, "create_cache_middleware", capture)
    app = api_server.create_app()
    assert "/stops/*" in observed["include_paths"]
    assert any(
        ctx.path == "/stops/{route_id}"
        for ctx in iter_route_contexts(app.routes)
        if isinstance(ctx.original_route, APIRoute)
    )
