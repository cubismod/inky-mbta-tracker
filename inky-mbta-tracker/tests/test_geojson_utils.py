from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import ClientSession
from geojson import Feature, Point
from geojson_utils import (
    calculate_bearing,
    calculate_stop_eta,
    light_get_alerts_batch,
    lookup_vehicle_color,
)
from shared_types.shared_types import VehicleRedisSchema


class MockResp:
    def __init__(self, status: int, data: Any) -> None:
        self.status = status
        self._data = data

    async def __aenter__(self) -> "MockResp":  # type: ignore[name-defined]
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        return None

    async def json(self) -> Any:
        return self._data


class MockSession:
    def __init__(self, status: int, data: Any) -> None:
        self._status = status
        self._data = data
        self.calls: list[str] = []

    def get(self, endpoint: str) -> Any:  # return Any to appease type checking
        self.calls.append(endpoint)
        return MockResp(self._status, self._data)


def build_alert_payload(
    with_valid_data: bool = True,
) -> dict[str, Any] | list[Any] | str:
    if not with_valid_data:
        return "not-a-dict"
    attrs = {
        "timeframe": None,
        "image_alternative_text": None,
        "cause": "UNKNOWN_CAUSE",
        "image": None,
        "created_at": datetime.now(UTC).isoformat(),
        "banner": None,
        "header": "Service delay",
        "url": None,
        "short_header": "Delay",
        "effect": "DELAY",
        "updated_at": datetime.now(UTC).isoformat(),
        "effect_name": None,
        "active_period": [{"start": None, "end": None}],
        "informed_entity": [{"route": "Red"}],
        "severity": 5,
    }
    return {
        "data": [
            {
                "type": "alert",
                "id": "A1",
                "attributes": attrs,
            }
        ]
    }


@pytest.mark.anyio("asyncio")
@patch("geojson_utils.OllamaClientIMT")
async def test_light_get_alerts_batch_success(mock_ollama: MagicMock) -> None:
    # Arrange a session that returns valid alert payload
    session = MockSession(200, build_alert_payload(True))
    r_client = AsyncMock()

    # Mock Ollama client to attach summary
    ollama_ctx = AsyncMock()
    ollama_ctx.__aenter__.return_value = ollama_ctx
    ollama_ctx.fetch_cached_summary = AsyncMock(return_value="summary")
    mock_ollama.return_value = ollama_ctx

    # Act
    alerts = await light_get_alerts_batch(
        "Red,Blue", cast(ClientSession, session), r_client
    )

    # Assert
    assert alerts is not None
    assert len(alerts) == 1
    assert alerts[0].ai_summary == "summary"
    assert "alerts?filter[route]=Red,Blue" in session.calls[0]


@pytest.mark.anyio("asyncio")
async def test_light_get_alerts_batch_rate_limited_returns_none() -> None:
    session = MockSession(429, build_alert_payload(True))
    r_client = AsyncMock()
    alerts = await light_get_alerts_batch("Red", cast(ClientSession, session), r_client)
    assert alerts is None


@pytest.mark.anyio("asyncio")
async def test_light_get_alerts_batch_non_200_returns_none() -> None:
    session = MockSession(500, build_alert_payload(True))
    r_client = AsyncMock()
    alerts = await light_get_alerts_batch("Red", cast(ClientSession, session), r_client)
    assert alerts is None


@pytest.mark.anyio("asyncio")
async def test_light_get_alerts_batch_invalid_shape_returns_none() -> None:
    session = MockSession(200, build_alert_payload(False))
    r_client = AsyncMock()
    alerts = await light_get_alerts_batch("Red", cast(ClientSession, session), r_client)
    assert alerts is None


@pytest.mark.anyio("asyncio")
async def test_light_get_alerts_batch_missing_data_returns_none() -> None:
    session = MockSession(200, {"foo": 1})
    r_client = AsyncMock()
    alerts = await light_get_alerts_batch("Red", cast(ClientSession, session), r_client)
    assert alerts is None


@pytest.mark.anyio("asyncio")
async def test_light_get_alerts_batch_data_not_list_returns_none() -> None:
    session = MockSession(200, {"data": {"not": "a list"}})
    r_client = AsyncMock()
    alerts = await light_get_alerts_batch("Red", cast(ClientSession, session), r_client)
    assert alerts is None


def test_lookup_vehicle_color_mapping() -> None:
    def mk(route: str) -> VehicleRedisSchema:
        return VehicleRedisSchema(
            action="add",
            id="v",
            current_status="",
            direction_id=0,
            latitude=0.0,
            longitude=0.0,
            speed=None,
            stop=None,
            route=route,
            update_time=datetime.now(UTC),
        )

    assert lookup_vehicle_color(mk("Amtrak NE Corridor")) == "#18567D"
    assert lookup_vehicle_color(mk("Green-B")) == "#008150"
    assert lookup_vehicle_color(mk("Blue")) == "#2F5DA6"
    assert lookup_vehicle_color(mk("CR-Providence")) == "#7B388C"
    assert lookup_vehicle_color(mk("Red")) == "#FA2D27"
    assert lookup_vehicle_color(mk("Mattapan")) == "#FA2D27"
    assert lookup_vehicle_color(mk("Orange")) == "#FD8A03"
    assert lookup_vehicle_color(mk("74")) == "#9A9C9D"
    assert lookup_vehicle_color(mk("SL3")) == "#9A9C9D"
    assert lookup_vehicle_color(mk("32")) == "#FFFF00"
    assert lookup_vehicle_color(mk("X-Unknown")) == ""


@pytest.mark.anyio("asyncio")
@patch("geojson_utils.distance", return_value=10.0)
async def test_calculate_stop_eta_uses_distance_and_speed(mock_dist: MagicMock) -> None:
    eta = calculate_stop_eta(
        Feature(geometry=Point((0, 0))), Feature(geometry=Point((1, 1))), speed=60.0
    )
    # 10 miles at 60 mph -> 10 minutes
    assert "10 minutes" in eta


def test_calculate_bearing_east_is_90() -> None:
    b = calculate_bearing(Point((0.0, 0.0)), Point((1.0, 0.0)))
    assert 85 <= b <= 95
