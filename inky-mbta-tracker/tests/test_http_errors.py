from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest
from api.core import GET_DI
from api.limits import limiter
from api.services.predictions import MBTAUpstreamError
from api.services.stops import StopsUpstreamError
from fastapi import FastAPI, HTTPException, Request, Response
from redis.exceptions import RedisError
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.exceptions import HTTPException as StarletteHTTPException

from tests.asgi_helpers import get_response


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    # Use the real routers, dependency, and middleware, mocking only external I/O.
    from api_server import create_app

    monkeypatch.setattr("api.core.load_config", lambda: object())
    monkeypatch.setattr(limiter, "enabled", False, raising=False)
    monkeypatch.setattr(
        "api.middleware.cache_middleware.get_cache", AsyncMock(return_value=None)
    )
    cache_write = AsyncMock()
    monkeypatch.setattr("api.middleware.cache_middleware.write_cache", cache_write)
    app = create_app()
    app.state.session = MagicMock()
    app.state.r_client = MagicMock()
    app.state.cache_write = cache_write
    return app


def assert_error_response(
    app: FastAPI, response: Response, status: int, detail: str
) -> None:
    assert response.status_code == status
    assert response.headers["content-type"] == "application/json"
    assert response.headers["x-user-query-transaction-id"]
    assert orjson.loads(response.body) == {"detail": detail}
    app.state.cache_write.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "path,params,detail",
    [
        ("/predictions", {}, "Specify trip_id or both latitude and longitude"),
        (
            "/predictions",
            {"latitude": "42"},
            "latitude and longitude must be specified together",
        ),
        (
            "/predictions",
            {"longitude": "-71"},
            "latitude and longitude must be specified together",
        ),
        (
            "/predictions",
            {"trip_id": "trip", "radius": "1"},
            "radius requires latitude and longitude",
        ),
        ("/alerts/bus/invalid", {}, "Invalid route_id"),
        ("/alerts/bus/invalid.json", {}, "Invalid route_id"),
    ],
)
async def test_bad_requests(
    app: FastAPI, path: str, params: dict[str, str], detail: str
) -> None:
    response = await get_response(app, path, params)
    assert_error_response(app, response, 400, detail)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "path,params",
    [
        ("/predictions", {"latitude": "91", "longitude": "0"}),
        ("/predictions", {"latitude": "0", "longitude": "181"}),
        ("/predictions", {"trip_id": "trip", "radius": "0"}),
        ("/predictions/departures", {}),
        ("/predictions/departures", {"stop": "stop", "direction": "2"}),
        ("/predictions/departures", {"stop": "stop", "limit": "51"}),
        ("/stop", {}),
        ("/vehicles", {"frequent_buses": "invalid"}),
        ("/vehicles/stream", {"compress": "invalid"}),
        ("/shapes", {"frequent_buses": "invalid"}),
        ("/shapes.json", {"frequent_buses": "invalid"}),
        ("/historical/vehicles", {}),
    ],
)
async def test_query_validation(
    app: FastAPI, path: str, params: dict[str, str]
) -> None:
    response = await get_response(app, path, params)
    assert response.status_code == 422
    assert response.headers["content-type"] == "application/json"
    assert response.headers["x-user-query-transaction-id"]
    assert isinstance(orjson.loads(response.body)["detail"], list)
    app.state.cache_write.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("status", [400, 401, 403, 404, 422, 429, 503])
@pytest.mark.parametrize(
    "path,params,service,error_type",
    [
        (
            "/predictions",
            {"trip_id": "trip"},
            "api.endpoints.predictions.fetch_predictions",
            MBTAUpstreamError,
        ),
        (
            "/predictions/departures",
            {"stop": "stop"},
            "api.endpoints.predictions.fetch_stop_departures",
            MBTAUpstreamError,
        ),
        (
            "/stops/Red",
            {},
            "api.endpoints.stops.fetch_route_stops",
            StopsUpstreamError,
        ),
    ],
)
async def test_upstream_http_errors(
    app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    path: str,
    params: dict[str, str],
    service: str,
    error_type: type[MBTAUpstreamError] | type[StopsUpstreamError],
) -> None:
    fetch = AsyncMock(side_effect=error_type(status))
    monkeypatch.setattr(service, fetch)
    response = await get_response(app, path, params)
    assert_error_response(app, response, status, "MBTA API request failed")
    fetch.assert_awaited_once()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "path,params,service,error_type",
    [
        ("/alerts", {}, "api.endpoints.alerts.fetch_alerts_with_retry", RedisError),
        ("/alerts/bus/77", {}, "api.endpoints.alerts.fetch_bus_alerts", RedisError),
        ("/shapes", {}, "api.endpoints.shapes.get_shapes_features", ConnectionError),
        (
            "/vehicles",
            {},
            "api.endpoints.vehicles.get_vehicle_features",
            ConnectionError,
        ),
        (
            "/vehicles/counts",
            {},
            "api.endpoints.vehicles.get_vehicle_route_counts",
            ConnectionError,
        ),
        ("/stop", {"id": "stop"}, "api.endpoints.stops.light_get_stop", RedisError),
        (
            "/historical/vehicles/earliest",
            {},
            "api.endpoints.historical.fetch_earliest_historical_vehicle_timestamp",
            RedisError,
        ),
        (
            "/historical/vehicles/latest",
            {},
            "api.endpoints.historical.fetch_latest_historical_vehicle_timestamp",
            RedisError,
        ),
        (
            "/historical/vehicles/counts",
            {},
            "api.endpoints.historical.fetch_historical_vehicle_counts",
            RedisError,
        ),
        (
            "/historical/vehicles/speeds",
            {},
            "api.endpoints.historical.fetch_historical_vehicle_speeds",
            RedisError,
        ),
    ],
)
async def test_handled_server_errors(
    app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    params: dict[str, str],
    service: str,
    error_type: type[Exception],
) -> None:
    fetch = AsyncMock(side_effect=error_type("unavailable"))
    monkeypatch.setattr(service, fetch)
    response = await get_response(app, path, params)
    assert_error_response(app, response, 500, "Internal server error")
    fetch.assert_awaited_once()


@pytest.mark.anyio
async def test_missing_stop(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "api.endpoints.stops.light_get_stop", AsyncMock(return_value=None)
    )
    response = await get_response(app, "/stop", {"id": "missing"})
    assert response.status_code == 404
    assert not response.body
    app.state.cache_write.assert_not_awaited()


@pytest.mark.anyio
async def test_disabled_stream(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("api.endpoints.vehicles.SSE_ENABLED", False)
    response = await get_response(app, "/vehicles/stream")
    assert_error_response(app, response, 503, "SSE streaming disabled")


@pytest.mark.anyio
async def test_upstream_connection_failure(
    app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "api.endpoints.stops.fetch_route_stops",
        AsyncMock(side_effect=ConnectionError("unavailable")),
    )
    response = await get_response(app, "/stops/Red")
    assert_error_response(app, response, 502, "MBTA API connection failed")


@pytest.mark.anyio
async def test_rate_limit_handler(app: FastAPI) -> None:
    # Keep the real SlowAPI handler and dependency lifecycle without using Redis.
    test_limiter = Limiter(
        key_func=get_remote_address, storage_uri="memory://", headers_enabled=True
    )
    app.state.limiter = test_limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

    @app.get("/test-rate-limit")
    @test_limiter.limit("1/minute")
    async def endpoint(request: Request, commons: GET_DI) -> Response:
        return Response(content='{"ok":true}', media_type="application/json")

    first = await get_response(app, "/test-rate-limit")
    assert first.status_code == 200
    response = await get_response(app, "/test-rate-limit")
    assert response.status_code == 429
    assert response.headers["content-type"] == "application/json"
    assert "Rate limit exceeded" in orjson.loads(response.body)["error"]
    assert response.headers["retry-after"]
    assert response.headers["x-user-query-transaction-id"]
    app.state.cache_write.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("error_type", [HTTPException, StarletteHTTPException])
@pytest.mark.parametrize(
    "status,headers",
    [
        (401, {"WWW-Authenticate": "Bearer"}),
        (403, {}),
        (409, {}),
        (429, {"Retry-After": "60"}),
    ],
)
async def test_http_exception_headers(
    app: FastAPI,
    error_type: type[StarletteHTTPException],
    status: int,
    headers: dict[str, str],
) -> None:
    @app.get("/test-http-error")
    async def endpoint(commons: GET_DI) -> None:
        raise error_type(status_code=status, detail="Request rejected", headers=headers)

    response = await get_response(app, "/test-http-error")
    assert_error_response(app, response, status, "Request rejected")
    for name, value in headers.items():
        assert response.headers[name] == value
