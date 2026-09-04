from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from api.models import HistoricalVehicleSpeedsResponse
from api.services.historical import fetch_historical_vehicle_speeds
from fastapi.routing import APIRoute
from redis.asyncio import Redis


def _feature(route: str, speed: float | None) -> dict[str, Any]:
    properties: dict[str, Any] = {"route": route}
    if speed is not None:
        properties["speed"] = speed
    return {"geometry": None, "properties": properties}


def _snapshot(vehicles: dict[str, dict[str, Any]]) -> bytes:
    import orjson

    return orjson.dumps(vehicles)


def _redis_for(snapshots: dict[str, bytes]) -> Any:
    redis = MagicMock()
    raw = {ts.encode(): body for ts, body in snapshots.items()}
    redis.hgetall = AsyncMock(return_value=raw)
    return redis


@pytest.mark.anyio("asyncio")
async def test_speeds_computed_per_line_per_snapshot() -> None:
    redis = _redis_for(
        {
            "100.0": _snapshot(
                {
                    "v1": _feature("Red", 20),
                    "v2": _feature("Red", 30),
                    "v3": _feature("Green-B", 10),
                }
            )
        }
    )

    result = await fetch_historical_vehicle_speeds(cast(Redis, redis))

    assert isinstance(result, HistoricalVehicleSpeedsResponse)
    assert len(result.snapshots) == 1
    snapshot = result.snapshots[0]
    assert snapshot.timestamp == "1970-01-01T00:01:40.000Z"
    assert snapshot.lines["RL"].vehicle_count == 2
    assert snapshot.lines["RL"].avg_speed == 25
    assert snapshot.lines["RL"].min_speed == 20
    assert snapshot.lines["RL"].max_speed == 30
    assert snapshot.lines["GL"].vehicle_count == 1
    assert snapshot.lines["GL"].avg_speed == 10


@pytest.mark.anyio("asyncio")
async def test_speeds_exclude_zero_missing_and_unclassified() -> None:
    redis = _redis_for(
        {
            "100.0": _snapshot(
                {
                    "v1": _feature("Red", 0),
                    "v2": _feature("Red", None),
                    "v3": _feature("Red", 25),
                    "v4": _feature("32", 40),
                }
            )
        }
    )

    result = await fetch_historical_vehicle_speeds(cast(Redis, redis))

    snapshot = result.snapshots[0]
    assert set(snapshot.lines) == {"RL"}
    assert snapshot.lines["RL"].vehicle_count == 1
    assert snapshot.lines["RL"].avg_speed == 25


@pytest.mark.anyio("asyncio")
async def test_speeds_map_silver_line_numeric_routes() -> None:
    redis = _redis_for({"100.0": _snapshot({"v1": _feature("742", 18)})})

    result = await fetch_historical_vehicle_speeds(cast(Redis, redis))

    assert set(result.snapshots[0].lines) == {"SL"}
    assert result.snapshots[0].lines["SL"].vehicle_count == 1


@pytest.mark.anyio("asyncio")
async def test_snapshots_sorted_by_timestamp() -> None:
    redis = _redis_for(
        {
            "300.0": _snapshot({"v1": _feature("Red", 20)}),
            "100.0": _snapshot({"v1": _feature("Red", 10)}),
            "200.0": _snapshot({"v1": _feature("Red", 15)}),
        }
    )

    result = await fetch_historical_vehicle_speeds(cast(Redis, redis))

    timestamps = [s.timestamp for s in result.snapshots]
    assert timestamps == sorted(timestamps)


@pytest.mark.anyio("asyncio")
async def test_empty_redis_returns_no_snapshots() -> None:
    redis = _redis_for({})

    result = await fetch_historical_vehicle_speeds(cast(Redis, redis))

    assert result.snapshots == []


def test_api_server_registers_historical_speeds_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("IMT_RATE_LIMITING_ENABLED", "false")

    from api_server import create_app

    app = create_app()

    assert any(
        route.path == "/historical/vehicles/speeds"
        for route in app.routes
        if isinstance(route, APIRoute)
    )
