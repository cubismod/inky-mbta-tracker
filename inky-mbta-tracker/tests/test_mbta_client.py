from asyncio import CancelledError
from datetime import UTC
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest
from aiohttp import ClientResponseError
from exceptions import WatcherRefreshRequested
from mbta_client import (
    MBTAApi,
    occupancy_status_human_readable,
    silver_line_lookup,
)
from mbta_client_extended import (
    light_get_stop,
    watch_mbta_server_side_events,
)
from mbta_responses import (
    CarriageStatus,
    PredictionAttributes,
    Vehicle,
    VehicleAttributes,
    VehicleResource,
)
from redis.asyncio.client import Redis as RedisClient
from shared_types.shared_types import LightStop, TaskType


@pytest.mark.anyio("asyncio")
async def test_watch_mbta_server_side_events_records_rate_limit_hit() -> None:
    endpoint = "https://api-v3.mbta.com/vehicles?api_key=secret"
    aiosseclient_kwargs = {}

    async def monitor_health(_tg) -> None:  # type: ignore[no-untyped-def]
        return None

    async def fake_aiosseclient(*_args, **kwargs):  # type: ignore[no-untyped-def]
        aiosseclient_kwargs.update(kwargs)
        raise ClientResponseError(
            request_info=MagicMock(),
            history=(),
            status=429,
            message="Too Many Requests",
        )
        yield

    watcher = MagicMock()
    watcher.monitor_health = monitor_health

    with (
        patch("mbta_client_extended.aiosseclient", new=fake_aiosseclient),
        patch("mbta_client_extended.record_mbta_api_rate_limit_hit") as record,
        patch("mbta_client_extended.sleep", new=AsyncMock(side_effect=CancelledError)),
    ):
        with pytest.raises(CancelledError):
            await watch_mbta_server_side_events(
                watcher,
                endpoint,
                {},
                None,
                MagicMock(),
                0,
                MagicMock(),
            )

    record.assert_called_once_with(endpoint)
    assert aiosseclient_kwargs["raise_for_status"] is True


@pytest.mark.anyio("asyncio")
async def test_watch_mbta_server_side_events_reconnects_after_health_refresh() -> None:
    monitor_starts = 0
    sleep_mock = AsyncMock(side_effect=[None, CancelledError])

    async def monitor_health(_tg) -> None:  # type: ignore[no-untyped-def]
        nonlocal monitor_starts
        monitor_starts += 1
        raise WatcherRefreshRequested

    async def fake_aiosseclient(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        await anyio.sleep(999)
        yield

    watcher = MagicMock()
    watcher.monitor_health = monitor_health
    watcher.gen_unique_id.return_value = "vehicle-red"

    with (
        patch("mbta_client_extended.aiosseclient", new=fake_aiosseclient),
        patch("mbta_client_extended.sleep", new=sleep_mock),
    ):
        with pytest.raises(CancelledError):
            await watch_mbta_server_side_events(
                watcher,
                "https://api-v3.mbta.com/vehicles?filter[route]=Red",
                {},
                None,
                MagicMock(),
                0,
                MagicMock(),
            )

    assert monitor_starts == 2


class TestSilverLineLookup:
    def test_silver_line_lookup_known_routes(self) -> None:
        assert silver_line_lookup("741") == "SL1"
        assert silver_line_lookup("742") == "SL2"
        assert silver_line_lookup("743") == "SL3"
        assert silver_line_lookup("746") == "SLW"
        assert silver_line_lookup("749") == "SL5"
        assert silver_line_lookup("751") == "SL4"

    def test_silver_line_lookup_unknown_route(self) -> None:
        assert silver_line_lookup("999") == "999"
        assert silver_line_lookup("Red") == "Red"


class TestLightStop:
    def test_light_stop_creation(self) -> None:
        stop = LightStop(
            stop_id="place-davis", mbta_stop_id="place-davis", parent_stop_id=None
        )
        assert stop.stop_id == "place-davis"
        assert stop.long is None
        assert stop.lat is None
        assert stop.platform_prediction is None

    def test_light_stop_with_coordinates(self) -> None:
        stop = LightStop(
            stop_id="place-davis",
            long=-71.1218,
            lat=42.3967,
            mbta_stop_id="place-davis",
            parent_stop_id=None,
        )
        assert stop.stop_id == "place-davis"
        assert stop.long == -71.1218
        assert stop.lat == 42.3967


class TestMBTAApi:
    def test_vehicle_resource_matches_swagger_schema(self) -> None:
        vehicle = VehicleResource.model_validate(
            {
                "id": "y1817",
                "type": "vehicle",
                "links": {},
                "attributes": {
                    "bearing": 174,
                    "carriages": [
                        {
                            "label": "some-carriage",
                            "occupancy_percentage": 80,
                            "occupancy_status": "MANY_SEATS_AVAILABLE",
                        }
                    ],
                    "current_status": "IN_TRANSIT_TO",
                    "current_stop_sequence": 8,
                    "direction_id": 0,
                    "label": "1817",
                    "latitude": -71.27239990234375,
                    "longitude": 42.32941818237305,
                    "occupancy_status": "FEW_SEATS_AVAILABLE",
                    "revenue_status": "REVENUE",
                    "speed": 16,
                    "updated_at": "2017-08-14T16:04:44-04:00",
                },
                "relationships": {
                    "route": {
                        "data": {"id": "Red", "type": "route"},
                        "links": {
                            "related": "/routes/Red",
                            "self": "/vehicles/y1817/relationships/route",
                        },
                    },
                    "stop": {
                        "data": {"id": "place-davis", "type": "stop"},
                        "links": {
                            "related": "/stops/place-davis",
                            "self": "/vehicles/y1817/relationships/stop",
                        },
                    },
                    "trip": {
                        "data": {"id": "trip-1", "type": "trip"},
                        "links": {
                            "related": "/trips/trip-1",
                            "self": "/vehicles/y1817/relationships/trip",
                        },
                    },
                },
            }
        )

        assert vehicle.attributes.revenue_status == "REVENUE"
        assert vehicle.attributes.updated_at == "2017-08-14T16:04:44-04:00"
        assert vehicle.relationships is not None
        assert vehicle.relationships.route.links is not None
        assert vehicle.relationships.route.links.related == "/routes/Red"

    def test_vehicle_document_wraps_vehicle_resource(self) -> None:
        vehicle = Vehicle.model_validate(
            {
                "links": {"self": "/vehicles/y1817"},
                "included": [{"id": "trip-1", "type": "trip"}],
                "data": {
                    "id": "y1817",
                    "type": "vehicle",
                    "links": {},
                    "attributes": {
                        "current_status": "IN_TRANSIT_TO",
                        "direction_id": 0,
                        "latitude": 42.3601,
                        "longitude": -71.0589,
                    },
                    "relationships": {
                        "route": {
                            "data": {"id": "Red", "type": "route"},
                        }
                    },
                },
            }
        )

        assert vehicle.links is not None
        assert vehicle.links.self == "/vehicles/y1817"
        assert vehicle.data.id == "y1817"
        assert vehicle.included is not None
        assert vehicle.included[0].id == "trip-1"

    def test_init_default_values(self) -> None:
        api = MBTAApi(cast(RedisClient, MagicMock()))
        assert api.stop_id is None
        assert api.route is None
        assert api.direction_filter is None
        assert api.watcher_type == TaskType.SCHEDULE_PREDICTIONS
        assert api.schedule_only is False
        assert api.show_on_display is True
        assert isinstance(api.routes, dict)

    def test_init_with_parameters(self) -> None:
        api = MBTAApi(
            cast(RedisClient, MagicMock()),
            stop_id="place-davis",
            route="Red",
            direction_filter=1,
            schedule_only=True,
            watcher_type=TaskType.VEHICLES,
            show_on_display=False,
        )
        assert api.stop_id == "place-davis"
        assert api.route == "Red"
        assert api.direction_filter == 1
        assert api.watcher_type == TaskType.VEHICLES
        assert api.schedule_only is True
        assert api.show_on_display is False

    def test_determine_time_arrival(self) -> None:
        attrs = PredictionAttributes(
            arrival_time="2023-12-01T10:30:00-05:00",
            departure_time=None,
            direction_id=0,
            revenue="REVENUE",
        )
        result = MBTAApi.determine_time(attrs)
        assert result is not None
        assert result.tzinfo == UTC

    def test_determine_time_departure(self) -> None:
        attrs = PredictionAttributes(
            arrival_time=None,
            departure_time="2023-12-01T10:30:00-05:00",
            direction_id=0,
            revenue="REVENUE",
        )
        result = MBTAApi.determine_time(attrs)
        assert result is not None
        assert result.tzinfo == UTC

    def test_determine_time_none(self) -> None:
        attrs = PredictionAttributes(
            arrival_time=None, departure_time=None, direction_id=0, revenue="REVENUE"
        )
        result = MBTAApi.determine_time(attrs)
        assert result is None

    def test_occupancy_status_human_readable(self) -> None:
        assert (
            occupancy_status_human_readable("MANY_SEATS_AVAILABLE")
            == "Many seats available"
        )
        assert occupancy_status_human_readable("FULL") == "Full"

    def test_abbreviate(self) -> None:
        assert MBTAApi.abbreviate("Massachusetts Avenue") == "Mass Ave"
        assert MBTAApi.abbreviate("Main Street") == "Main St"
        assert MBTAApi.abbreviate("Harvard Square") == "Harvard Sq"
        assert MBTAApi.abbreviate("Soldiers Field Road") == "Soldiers Field Rd"
        assert MBTAApi.abbreviate("Government Center") == "Gov't Center"
        assert MBTAApi.abbreviate("VFW Parkway") == "VFW Pkwy"

    def test_get_carriages_with_data(self) -> None:
        vehicle = VehicleResource(
            id="test-vehicle",
            type="vehicle",
            attributes=VehicleAttributes(
                direction_id=0,
                current_status="IN_TRANSIT_TO",
                latitude=42.3601,
                longitude=-71.0589,
                carriages=[
                    CarriageStatus(
                        label="Car1", occupancy_status="MANY_SEATS_AVAILABLE"
                    ),
                    CarriageStatus(
                        label="Car2", occupancy_status="MANY_SEATS_AVAILABLE"
                    ),
                    CarriageStatus(
                        label="Car3", occupancy_status="FEW_SEATS_AVAILABLE"
                    ),
                ],
            ),
        )

        carriages, status = MBTAApi.get_carriages(vehicle)
        assert carriages == ["Car1", "Car2", "Car3"]
        assert status == "MANY_SEATS_AVAILABLE"

    def test_get_carriages_no_data(self) -> None:
        vehicle = VehicleResource(
            id="test-vehicle",
            type="vehicle",
            attributes=VehicleAttributes(
                direction_id=0,
                current_status="IN_TRANSIT_TO",
                latitude=42.3601,
                longitude=-71.0589,
                carriages=None,
            ),
        )

        carriages, status = MBTAApi.get_carriages(vehicle)
        assert carriages == []
        assert status == ""

    @pytest.mark.anyio("asyncio")
    @patch("mbta_client.Stop.model_validate_json")
    @patch("mbta_client.Facilities.model_validate_json")
    @patch("mbta_client.get_cache")
    async def test_get_stop_can_skip_facilities_fetch(
        self,
        mock_get_cache: MagicMock,
        mock_facilities_validate: MagicMock,
        mock_stop_validate: MagicMock,
    ) -> None:
        mock_get_cache.return_value = None
        mock_stop_validate.return_value = None

        response = AsyncMock()
        response.status = 200
        response.text.return_value = "{}"

        response_cm = AsyncMock()
        response_cm.__aenter__.return_value = response
        response_cm.__aexit__.return_value = None

        session = MagicMock()
        session.closed = False
        session.get.return_value = response_cm

        redis = AsyncMock()
        redis.eval.return_value = [1, 0]
        api = MBTAApi(cast(RedisClient, redis), stop_id="place-davis")
        stop, facilities = await api.get_stop(
            session,
            "place-davis",
            include_facilities=False,
        )

        assert stop is None
        assert facilities is None
        assert session.get.call_count == 1
        mock_facilities_validate.assert_not_called()


@pytest.mark.anyio("asyncio")
class TestLightGetStop:
    @patch("mbta_client.get_cache")
    @patch("mbta_client.write_cache")
    @pytest.mark.anyio("asyncio")
    async def test_light_get_stop_cached(
        self, mock_write_cache: MagicMock, mock_get_cache: MagicMock
    ) -> None:
        mock_redis = AsyncMock()

        cached_data = '{"stop_id": "Davis", "long": -71.1218, "lat": 42.3967, "mbta_stop_id": "place-davis", "parent_stop_id": null}'
        mock_get_cache.return_value = cached_data

        async with anyio.create_task_group() as tg:
            result = await light_get_stop(mock_redis, "place-davis", tg)
            tg.cancel_scope.cancel()

        assert result is not None
        assert result.stop_id == "Davis"
        assert result.long == -71.1218
        assert result.lat == 42.3967
        assert result.parent_stop_id is None

        mock_get_cache.assert_called_once_with(mock_redis, "stop:place-davis:light")
        mock_write_cache.assert_not_called()

    @patch("mbta_client.get_cache")
    @patch("mbta_client.MBTAApi")
    @pytest.mark.anyio("asyncio")
    async def test_light_get_stop_not_cached(
        self, mock_mbta_api: MagicMock, mock_get_cache: MagicMock
    ) -> None:
        mock_redis = AsyncMock()

        mock_get_cache.return_value = None

        mock_watcher = AsyncMock()
        mock_mbta_api.return_value.__aenter__.return_value = mock_watcher
        mock_watcher.get_stop.return_value = None

        async with anyio.create_task_group() as tg:
            result = await light_get_stop(mock_redis, "place-davis", tg)
            tg.cancel_scope.cancel()

        assert result is None
        mock_get_cache.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__])
