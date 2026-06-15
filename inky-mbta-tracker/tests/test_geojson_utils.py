from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import ClientSession
from config import Config
from geo_math import distance
from geojson import Feature, Point
from geojson_utils import (
    calculate_bearing,
    calculate_stop_eta,
    get_vehicle_features,
    light_get_alerts_batch,
    lookup_vehicle_color,
    vehicle_display_point,
)
from shared_types.shared_types import LightStop, VehicleRedisSchema


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


def make_redis_client() -> AsyncMock:
    client = AsyncMock()
    client.eval.return_value = [1, 0]
    return client


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
async def test_light_get_alerts_batch_success() -> None:
    # Arrange a session that returns valid alert payload
    session = MockSession(200, build_alert_payload(True))
    r_client = make_redis_client()

    # Act
    alerts = await light_get_alerts_batch(
        "Red,Blue", cast(ClientSession, session), r_client
    )

    # Assert
    assert alerts is not None
    assert len(alerts) == 1
    assert "alerts?filter[route]=Red,Blue" in session.calls[0]


@pytest.mark.anyio("asyncio")
async def test_light_get_alerts_batch_rate_limited_returns_none() -> None:
    session = MockSession(429, build_alert_payload(True))
    r_client = make_redis_client()
    alerts = await light_get_alerts_batch("Red", cast(ClientSession, session), r_client)
    assert alerts is None


@pytest.mark.anyio("asyncio")
async def test_light_get_alerts_batch_non_200_returns_none() -> None:
    session = MockSession(500, build_alert_payload(True))
    r_client = make_redis_client()
    alerts = await light_get_alerts_batch("Red", cast(ClientSession, session), r_client)
    assert alerts is None


@pytest.mark.anyio("asyncio")
async def test_light_get_alerts_batch_invalid_shape_returns_none() -> None:
    session = MockSession(200, build_alert_payload(False))
    r_client = make_redis_client()
    alerts = await light_get_alerts_batch("Red", cast(ClientSession, session), r_client)
    assert alerts is None


@pytest.mark.anyio("asyncio")
async def test_light_get_alerts_batch_missing_data_returns_none() -> None:
    session = MockSession(200, {"foo": 1})
    r_client = make_redis_client()
    alerts = await light_get_alerts_batch("Red", cast(ClientSession, session), r_client)
    assert alerts is None


@pytest.mark.anyio("asyncio")
async def test_light_get_alerts_batch_data_not_list_returns_none() -> None:
    session = MockSession(200, {"data": {"not": "a list"}})
    r_client = make_redis_client()
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
            bearing=0,
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


@patch("geojson_utils.distance", return_value=10.0)
def test_calculate_stop_eta_uses_predicted_arrival_when_provided(mock_dist: MagicMock) -> None:
    future = datetime.now(UTC).replace(microsecond=0)
    future = future.replace(hour=future.hour + 1)
    eta = calculate_stop_eta(
        Feature(geometry=Point((0, 0))),
        Feature(geometry=Point((1, 1))),
        speed=60.0,
        predicted_arrival=future,
    )
    # Should NOT call distance, should use predicted_arrival
    mock_dist.assert_not_called()
    assert "hour" in eta


@patch("geojson_utils.distance", return_value=10.0)
def test_calculate_stop_eta_falls_back_to_speed_when_no_prediction(mock_dist: MagicMock) -> None:
    eta = calculate_stop_eta(
        Feature(geometry=Point((0, 0))), Feature(geometry=Point((1, 1))), speed=60.0
    )
    mock_dist.assert_called_once()
    assert "10 minutes" in eta


@patch("geojson_utils.distance", return_value=10.0)
def test_calculate_stop_eta_falls_back_when_prediction_is_past(mock_dist: MagicMock) -> None:
    past = datetime(2020, 1, 1, tzinfo=UTC)
    eta = calculate_stop_eta(
        Feature(geometry=Point((0, 0))),
        Feature(geometry=Point((1, 1))),
        speed=60.0,
        predicted_arrival=past,
    )
    mock_dist.assert_called_once()
    assert "10 minutes" in eta


def test_calculate_bearing_east_is_90() -> None:
    b = calculate_bearing(Point((0.0, 0.0)), Point((1.0, 0.0)))
    assert 85 <= b <= 95


def test_calculate_bearing_west_is_negative_90() -> None:
    b = calculate_bearing(Point((0.0, 0.0)), Point((-1.0, 0.0)))
    assert -95 <= b <= -85


def test_vehicle_display_point_uses_stop_point_when_stopped_at() -> None:
    vehicle_point = Point((-71.0, 42.0))
    display_point = vehicle_display_point(
        vehicle_point, -71.1, 42.1, "STOPPED_AT", "vehicle-123"
    )

    assert display_point != Point((-71.1, 42.1))
    assert distance(display_point, Point((-71.1, 42.1)), "m") < 9


def test_vehicle_display_point_keeps_vehicle_point_when_not_stopped() -> None:
    vehicle_point = Point((-71.0, 42.0))

    assert (
        vehicle_display_point(
            vehicle_point, -71.1, 42.1, "IN_TRANSIT_TO", "vehicle-123"
        )
        == vehicle_point
    )


def test_vehicle_display_point_offsets_vehicles_at_same_stop_differently() -> None:
    vehicle_point = Point((-71.0, 42.0))

    first_display_point = vehicle_display_point(
        vehicle_point, -71.1, 42.1, "STOPPED_AT", "vehicle-123"
    )
    second_display_point = vehicle_display_point(
        vehicle_point, -71.1, 42.1, "STOPPED_AT", "vehicle-456"
    )

    assert first_display_point != second_display_point


@pytest.mark.anyio("asyncio")
@patch("geojson_utils.light_get_stop")
async def test_get_vehicle_features_places_stopped_vehicle_at_stop_coordinates(
    mock_light_get_stop: AsyncMock,
) -> None:
    vehicle = VehicleRedisSchema(
        action="add",
        id="vehicle-123",
        current_status="STOPPED_AT",
        direction_id=0,
        latitude=42.0,
        longitude=-71.0,
        speed=0,
        bearing=0,
        stop="place-davis",
        route="Red",
        update_time=datetime(2026, 1, 1, tzinfo=UTC),
    )
    redis = MagicMock()
    redis.smembers = AsyncMock(return_value={b"vehicle:vehicle-123"})
    pipeline = MagicMock()
    pipeline.get = AsyncMock()
    pipeline.execute = AsyncMock(return_value=[vehicle.model_dump_json().encode()])
    redis.pipeline.return_value = pipeline
    mock_light_get_stop.return_value = LightStop(
        stop_id="Davis",
        mbta_stop_id="place-davis",
        parent_stop_id=None,
        long=-71.1218,
        lat=42.3967,
    )

    features = await get_vehicle_features(
        redis, Config(vehicles_by_route=["Red"]), cast(Any, object())
    )

    display_point = Point(features["vehicle-123"]["geometry"]["coordinates"])

    assert display_point != Point((-71.1218, 42.3967))
    assert distance(display_point, Point((-71.1218, 42.3967)), "m") < 9
    assert features["vehicle-123"]["properties"]["stop-coordinates"] == (
        -71.1218,
        42.3967,
    )


@pytest.mark.anyio("asyncio")
@patch("geojson_utils.light_get_stop")
async def test_get_vehicle_features_keeps_in_transit_vehicle_coordinates(
    mock_light_get_stop: AsyncMock,
) -> None:
    vehicle = VehicleRedisSchema(
        action="add",
        id="vehicle-456",
        current_status="IN_TRANSIT_TO",
        direction_id=0,
        latitude=42.0,
        longitude=-71.0,
        speed=15,
        bearing=0,
        stop="place-davis",
        route="Red",
        update_time=datetime(2026, 1, 1, tzinfo=UTC),
    )
    redis = MagicMock()
    redis.smembers = AsyncMock(return_value={b"vehicle:vehicle-456"})
    pipeline = MagicMock()
    pipeline.get = AsyncMock()
    pipeline.execute = AsyncMock(return_value=[vehicle.model_dump_json().encode()])
    redis.pipeline.return_value = pipeline
    mock_light_get_stop.return_value = LightStop(
        stop_id="Davis",
        mbta_stop_id="place-davis",
        parent_stop_id=None,
        long=-71.1218,
        lat=42.3967,
    )

    features = await get_vehicle_features(
        redis, Config(vehicles_by_route=["Red"]), cast(Any, object())
    )

    assert features["vehicle-456"]["geometry"]["coordinates"] == [-71.0, 42.0]
    assert features["vehicle-456"]["properties"]["stop-coordinates"] == (
        -71.1218,
        42.3967,
    )
