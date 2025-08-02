from datetime import UTC
from queue import Queue
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mbta_client import (
    LightStop,
    MBTAApi,
    determine_station_id,
    light_get_alerts,
    light_get_stop,
    parse_shape_data,
    silver_line_lookup,
)
from mbta_responses import (
    CarriageStatus,
    PredictionAttributes,
    ShapeAttributes,
    ShapeResource,
    Shapes,
    Vehicle,
    VehicleAttributes,
)
from shared_types.shared_types import ScheduleEvent, TaskType, VehicleRedisSchema
from utils import thread_runner


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


class TestDetermineStationId:
    def test_north_station_variants(self) -> None:
        assert determine_station_id("North Station") == ("place-north", True)
        assert determine_station_id("BNT") == ("place-north", True)

    def test_south_station_variants(self) -> None:
        assert determine_station_id("South Station") == ("place-sstat", True)
        assert determine_station_id("NEC-2287") == ("place-sstat", True)

    def test_back_bay_variants(self) -> None:
        assert determine_station_id("Back Bay") == ("place-bbsta", True)
        assert determine_station_id("NEC-1851") == ("place-bbsta", True)

    def test_ruggles_variants(self) -> None:
        assert determine_station_id("Ruggles") == ("place-rugg", True)
        assert determine_station_id("NEC-2265") == ("place-rugg", True)

    def test_providence_variants(self) -> None:
        assert determine_station_id("Providence") == ("place-NEC-1851", True)

    def test_unknown_station(self) -> None:
        assert determine_station_id("place-davis") == ("place-davis", False)


class TestParseShapeData:
    @patch("mbta_client.SHAPE_POLYLINES", set())
    @patch("mbta_client.decode")
    def test_parse_shape_data_canonical(self, mock_decode: MagicMock) -> None:
        mock_decode.return_value = [(42.3601, -71.0589), (42.3611, -71.0599)]

        shape_data = Shapes(
            data=[
                ShapeResource(
                    type="shape",
                    id="canonical-1",
                    attributes=ShapeAttributes(polyline="test_polyline_1"),
                ),
                ShapeResource(
                    type="shape",
                    id="non-canonical",
                    attributes=ShapeAttributes(polyline="test_polyline_2"),
                ),
            ]
        )

        result = parse_shape_data(shape_data)

        assert len(result) == 2
        assert result[0] == [(42.3601, -71.0589), (42.3611, -71.0599)]

    @patch("mbta_client.SHAPE_POLYLINES", set())
    @patch("mbta_client.decode")
    def test_parse_shape_data_decimal_id(self, mock_decode: MagicMock) -> None:
        mock_decode.return_value = [(42.3601, -71.0589)]

        shape_data = Shapes(
            data=[
                ShapeResource(
                    type="shape",
                    id="123456",
                    attributes=ShapeAttributes(polyline="test_polyline"),
                ),
            ]
        )

        result = parse_shape_data(shape_data)

        assert len(result) == 1
        assert result[0] == [(42.3601, -71.0589)]

    @patch("mbta_client.SHAPE_POLYLINES", {"already_processed"})
    @patch("mbta_client.decode")
    def test_parse_shape_data_skip_processed(self, mock_decode: MagicMock) -> None:
        shape_data = Shapes(
            data=[
                ShapeResource(
                    type="shape",
                    id="canonical-1",
                    attributes=ShapeAttributes(polyline="already_processed"),
                ),
            ]
        )

        result = parse_shape_data(shape_data)

        assert len(result) == 0
        mock_decode.assert_not_called()


class TestLightStop:
    def test_light_stop_creation(self) -> None:
        stop = LightStop(stop_id="place-davis")
        assert stop.stop_id == "place-davis"
        assert stop.long is None
        assert stop.lat is None
        assert stop.platform_prediction is None

    def test_light_stop_with_coordinates(self) -> None:
        stop = LightStop(stop_id="place-davis", long=-71.1218, lat=42.3967)
        assert stop.stop_id == "place-davis"
        assert stop.long == -71.1218
        assert stop.lat == 42.3967


class TestMBTAApi:
    def test_init_default_values(self) -> None:
        api = MBTAApi()
        assert api.stop_id is None
        assert api.route is None
        assert api.direction_filter is None
        assert api.watcher_type == TaskType.SCHEDULE_PREDICTIONS
        assert api.schedule_only is False
        assert api.show_on_display is True
        assert isinstance(api.routes, dict)

    def test_init_with_parameters(self) -> None:
        api = MBTAApi(
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

    def test_meters_per_second_to_mph(self) -> None:
        assert MBTAApi.meters_per_second_to_mph(10.0) == 22.37
        assert MBTAApi.meters_per_second_to_mph(None) is None

    def test_occupancy_status_human_readable(self) -> None:
        assert (
            MBTAApi.occupancy_status_human_readable("MANY_SEATS_AVAILABLE")
            == "Many seats available"
        )
        assert MBTAApi.occupancy_status_human_readable("FULL") == "Full"

    def test_abbreviate(self) -> None:
        assert MBTAApi.abbreviate("Massachusetts Avenue") == "Mass Ave"
        assert MBTAApi.abbreviate("Main Street") == "Main St"
        assert MBTAApi.abbreviate("Harvard Square") == "Harvard Sq"
        assert MBTAApi.abbreviate("Soldiers Field Road") == "Soldiers Field Rd"
        assert MBTAApi.abbreviate("Government Center") == "Gov't Center"
        assert MBTAApi.abbreviate("VFW Parkway") == "VFW Pkwy"

    def test_get_carriages_with_data(self) -> None:
        vehicle = Vehicle(
            id="test-vehicle",
            type="vehicle",
            attributes=VehicleAttributes(
                direction_id=0,
                current_status="IN_TRANSIT_TO",
                latitude=42.3601,
                longitude=-71.0589,
                revenue="REVENUE",
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
        vehicle = Vehicle(
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


class TestThreadRunner:
    @patch("mbta_client.Runner")
    def test_thread_runner_schedules(self, mock_runner: MagicMock) -> None:
        mock_instance = MagicMock()
        mock_runner.return_value.__enter__.return_value = mock_instance

        queue = Queue[ScheduleEvent | VehicleRedisSchema]()
        thread_runner(
            target=TaskType.SCHEDULES,
            queue=queue,
            stop_id="place-davis",
            route="Red",
            direction_filter=1,
            transit_time_min=5,
        )

        mock_instance.run.assert_called_once()

    @patch("mbta_client.Runner")
    def test_thread_runner_predictions(self, mock_runner: MagicMock) -> None:
        mock_instance = MagicMock()
        mock_runner.return_value.__enter__.return_value = mock_instance

        queue = Queue[ScheduleEvent | VehicleRedisSchema]()
        thread_runner(
            target=TaskType.SCHEDULE_PREDICTIONS,
            queue=queue,
            stop_id="place-davis",
            route="Red",
            direction_filter=1,
            transit_time_min=5,
        )

        mock_instance.run.assert_called_once()

    @patch("mbta_client.Runner")
    def test_thread_runner_vehicles(self, mock_runner: MagicMock) -> None:
        mock_instance = MagicMock()
        mock_runner.return_value.__enter__.return_value = mock_instance

        queue = Queue[ScheduleEvent | VehicleRedisSchema]()
        thread_runner(target=TaskType.VEHICLES, queue=queue, route="Red")

        mock_instance.run.assert_called_once()


@pytest.mark.asyncio
class TestLightGetStop:
    @patch("mbta_client.check_cache")
    @patch("mbta_client.write_cache")
    async def test_light_get_stop_cached(
        self, mock_write_cache: MagicMock, mock_check_cache: MagicMock
    ) -> None:
        mock_redis = AsyncMock()
        mock_session = AsyncMock()

        cached_data = '{"stop_id": "Davis", "long": -71.1218, "lat": 42.3967}'
        mock_check_cache.return_value = cached_data

        result = await light_get_stop(mock_redis, "place-davis", mock_session)

        assert result is not None
        assert result.stop_id == "Davis"
        assert result.long == -71.1218
        assert result.lat == 42.3967

        mock_check_cache.assert_called_once_with(mock_redis, "stop:place-davis:light")
        mock_write_cache.assert_not_called()

    @patch("mbta_client.check_cache")
    @patch("mbta_client.MBTAApi")
    async def test_light_get_stop_not_cached(
        self, mock_mbta_api: MagicMock, mock_check_cache: MagicMock
    ) -> None:
        mock_redis = AsyncMock()
        mock_session = AsyncMock()

        mock_check_cache.return_value = None

        mock_watcher = AsyncMock()
        mock_mbta_api.return_value.__aenter__.return_value = mock_watcher
        mock_watcher.get_stop.return_value = None

        result = await light_get_stop(mock_redis, "place-davis", mock_session)

        assert result is None
        mock_check_cache.assert_called_once()


@pytest.mark.asyncio
class TestLightGetAlerts:
    @patch("mbta_client.MBTAApi")
    async def test_light_get_alerts_success(self, mock_mbta_api: MagicMock) -> None:
        mock_session = AsyncMock()

        mock_watcher = AsyncMock()
        mock_mbta_api.return_value.__aenter__.return_value = mock_watcher
        mock_alerts = [MagicMock()]
        mock_watcher.get_alerts.return_value = mock_alerts

        result = await light_get_alerts("Red", mock_session)

        assert result == mock_alerts
        mock_watcher.get_alerts.assert_called_once_with(mock_session, route_id="Red")

    @patch("mbta_client.MBTAApi")
    async def test_light_get_alerts_none(self, mock_mbta_api: MagicMock) -> None:
        mock_session = AsyncMock()

        mock_watcher = AsyncMock()
        mock_mbta_api.return_value.__aenter__.return_value = mock_watcher
        mock_watcher.get_alerts.return_value = None

        result = await light_get_alerts("Red", mock_session)

        assert result is None


if __name__ == "__main__":
    pytest.main([__file__])
