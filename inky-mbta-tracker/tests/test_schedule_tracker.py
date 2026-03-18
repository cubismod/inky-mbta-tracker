import os
from datetime import UTC, datetime, timedelta
from queue import Queue
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from schedule_tracker import (
    Tracker,
    dummy_schedule_event,
    process_queue,
)
from shared_types.shared_types import ScheduleEvent, VehicleRedisSchema


class TestDummyScheduleEvent:
    def test_dummy_schedule_event_creation(self) -> None:
        event_id = "test-event-123"
        event = dummy_schedule_event(event_id)

        assert event.action == "remove"
        assert event.headsign == "N/A"
        assert event.route_id == "N/A"
        assert event.route_type == 1
        assert event.id == event_id
        assert event.stop == "N/A"
        assert event.transit_time_min == 0
        assert event.trip_id == "N/A"
        assert event.alerting is False
        assert event.bikes_allowed is False
        assert isinstance(event.time, datetime)


class TestTracker:
    @patch.dict(
        os.environ,
        {
            "IMT_REDIS_ENDPOINT": "test-host",
            "IMT_REDIS_PORT": "6380",
            "IMT_REDIS_PASSWORD": "test-pass",
        },
    )
    def test_tracker_init(self) -> None:
        tracker = Tracker()
        assert tracker.redis is not None
        assert tracker.redis.connection_pool.connection_kwargs["host"] == "test-host"
        assert tracker.redis.connection_pool.connection_kwargs["port"] == 6380
        assert (
            tracker.redis.connection_pool.connection_kwargs["password"] == "test-pass"
        )

    def test_str_timestamp(self) -> None:
        now = datetime.now(UTC)
        event = ScheduleEvent(
            action="add",
            time=now,
            route_id="Red",
            route_type=1,
            headsign="Alewife",
            id="test-id",
            stop="Davis",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )

        result = Tracker.str_timestamp(event)
        assert result == str(now.timestamp())

    def test_calculate_time_diff_future(self) -> None:
        future_time = datetime.now(UTC) + timedelta(minutes=10)
        event = ScheduleEvent(
            action="add",
            time=future_time,
            route_id="Red",
            route_type=1,
            headsign="Alewife",
            id="test-id",
            stop="Davis",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )

        result = Tracker.calculate_time_diff(event)
        assert result.total_seconds() > 0

    def test_calculate_time_diff_past(self) -> None:
        past_time = datetime.now(UTC) - timedelta(minutes=1)
        event = ScheduleEvent(
            action="add",
            time=past_time,
            route_id="Red",
            route_type=1,
            headsign="Alewife",
            id="test-id",
            stop="Davis",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )

        result = Tracker.calculate_time_diff(event)
        assert result == timedelta(minutes=5)

    def test_is_speed_reasonable(self) -> None:
        assert Tracker.is_speed_reasonable(50, "Orange") is True
        assert Tracker.is_speed_reasonable(60, "Orange") is False
        assert Tracker.is_speed_reasonable(45, "Red") is True
        assert Tracker.is_speed_reasonable(60, "Red") is False
        assert Tracker.is_speed_reasonable(40, "Blue") is True
        assert Tracker.is_speed_reasonable(55, "Blue") is False
        assert Tracker.is_speed_reasonable(60, "742") is True  # Silver Line
        assert Tracker.is_speed_reasonable(80, "CR-Franklin") is True  # Commuter Rail
        assert Tracker.is_speed_reasonable(90, "CR-Franklin") is False
        assert Tracker.is_speed_reasonable(35, "Green-B") is True
        assert Tracker.is_speed_reasonable(45, "Green-B") is False
        assert Tracker.is_speed_reasonable(35, "Mattapan") is True
        assert Tracker.is_speed_reasonable(45, "Mattapan") is False

    def test_get_route_icon(self) -> None:
        light_rail_event = ScheduleEvent(
            action="add",
            time=datetime.now(UTC),
            route_id="Green-B",
            route_type=0,
            headsign="Boston College",
            id="test-id",
            stop="Kenmore",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )
        assert Tracker.get_route_icon(light_rail_event) == "🚊"

        subway_event = ScheduleEvent(
            action="add",
            time=datetime.now(UTC),
            route_id="Red",
            route_type=1,
            headsign="Alewife",
            id="test-id",
            stop="Davis",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )
        assert Tracker.get_route_icon(subway_event) == "🚇"

        commuter_rail_event = ScheduleEvent(
            action="add",
            time=datetime.now(UTC),
            route_id="CR-Franklin",
            route_type=2,
            headsign="Franklin",
            id="test-id",
            stop="South Station",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )
        assert Tracker.get_route_icon(commuter_rail_event) == "🚆"

        bus_event = ScheduleEvent(
            action="add",
            time=datetime.now(UTC),
            route_id="1",
            route_type=3,
            headsign="Harvard",
            id="test-id",
            stop="Central",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )
        assert Tracker.get_route_icon(bus_event) == "🚍"

        ferry_event = ScheduleEvent(
            action="add",
            time=datetime.now(UTC),
            route_id="Boat-F1",
            route_type=4,
            headsign="Hingham",
            id="test-id",
            stop="Long Wharf",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )
        assert Tracker.get_route_icon(ferry_event) == "⛴️"

        unknown_event = ScheduleEvent(
            action="add",
            time=datetime.now(UTC),
            route_id="Unknown",
            route_type=99,
            headsign="Unknown",
            id="test-id",
            stop="Unknown",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )
        assert Tracker.get_route_icon(unknown_event) == ""

    def test_prediction_display_future(self) -> None:
        future_time = datetime.now(UTC) + timedelta(minutes=5)
        event = ScheduleEvent(
            action="add",
            time=future_time,
            route_id="Red",
            route_type=1,
            headsign="Alewife",
            id="test-id",
            stop="Davis",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )

        result = Tracker.prediction_display(event)
        assert result.startswith("🕒 ")
        assert (
            "5 minutes" in result or "4 minutes" in result
        )  # Allow for slight timing differences

    def test_prediction_display_boarding(self) -> None:
        now = datetime.now(UTC)
        event = ScheduleEvent(
            action="add",
            time=now,
            route_id="Red",
            route_type=1,
            headsign="Alewife",
            id="test-id",
            stop="Davis",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )

        result = Tracker.prediction_display(event)
        assert result == " BRD"

    def test_prediction_display_departed(self) -> None:
        past_time = datetime.now(UTC) - timedelta(minutes=1)
        event = ScheduleEvent(
            action="add",
            time=past_time,
            route_id="Red",
            route_type=1,
            headsign="Alewife",
            id="test-id",
            stop="Davis",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )

        result = Tracker.prediction_display(event)
        assert result == " DEP"

    # @pytest.mark.anyio("asyncio")
    # async def test_calculate_vehicle_speed_no_previous_data(self) -> None:
    #     tracker = Tracker()
    #     mock_redis = AsyncMock()
    #     tracker.redis = mock_redis
    #     mock_redis.get.return_value = None

    #     event = VehicleRedisSchema(
    #         longitude=-71.0589,
    #         latitude=42.3601,
    #         direction_id=0,
    #         current_status="IN_TRANSIT_TO",
    #         id="vehicle-123",
    #         action="add",
    #         route="Red",
    #         update_time=datetime.now(UTC),
    #         speed=25.0,
    #     )

    #     speed, approximate = await tracker.calculate_vehicle_speed(event)
    #     assert speed == 25.0
    #     assert approximate is False

    # @pytest.mark.anyio("asyncio")
    # async def test_calculate_vehicle_speed_with_previous_data(self) -> None:
    #     tracker = Tracker()
    #     mock_redis = AsyncMock()
    #     tracker.redis = mock_redis

    #     previous_time = datetime.now(UTC) - timedelta(seconds=30)
    #     previous_event = VehicleRedisSchema(
    #         longitude=-71.0599,
    #         latitude=42.3611,
    #         direction_id=0,
    #         current_status="IN_TRANSIT_TO",
    #         id="vehicle-123",
    #         action="add",
    #         route="Red",
    #         update_time=previous_time,
    #         speed=20.0,
    #         approximate_speed=False,
    #     )

    #     mock_redis.get.return_value = previous_event.model_dump_json().encode("utf-8")

    #     current_time = datetime.now(UTC)
    #     event = VehicleRedisSchema(
    #         longitude=-71.0589,
    #         latitude=42.3601,
    #         direction_id=0,
    #         current_status="IN_TRANSIT_TO",
    #         id="vehicle-123",
    #         action="update",
    #         route="Red",
    #         update_time=current_time,
    #         speed=None,
    #     )

    #     speed, approximate = await tracker.calculate_vehicle_speed(event)
    #     assert speed is not None
    #     assert approximate is True

    # @pytest.mark.anyio("asyncio")
    # async def test_calculate_vehicle_speed_stopped(self) -> None:
    #     tracker = Tracker()
    #     mock_redis = AsyncMock()
    #     tracker.redis = mock_redis

    #     event = VehicleRedisSchema(
    #         longitude=-71.0589,
    #         latitude=42.3601,
    #         direction_id=0,
    #         current_status="STOPPED_AT",
    #         id="vehicle-123",
    #         action="add",
    #         route="Red",
    #         update_time=datetime.now(UTC),
    #         speed=None,
    #     )

    #     speed, approximate = await tracker.calculate_vehicle_speed(event)
    #     assert speed is None
    #     assert approximate is False

    @pytest.mark.anyio("asyncio")
    async def test_cleanup(self) -> None:
        tracker = Tracker()
        mock_redis = AsyncMock()
        mock_pipeline = AsyncMock()
        tracker.redis = mock_redis
        # In cleanup(), tracker may call self.redis.pipeline() synchronously to create
        # a new pipeline. Ensure our mock behaves like a sync factory to avoid
        # un-awaited coroutine warnings.
        from unittest.mock import MagicMock

        mock_redis.pipeline = MagicMock(return_value=mock_pipeline)

        # Mock obsolete IDs from Redis
        obsolete_ids = [b"event-1", b"event-2"]
        mock_redis.zrange.return_value = obsolete_ids

        # Mock RedisLock to avoid actual locking during tests
        with patch("schedule_tracker.RedisLock") as mock_lock:
            mock_lock_instance = AsyncMock()
            mock_lock.return_value = mock_lock_instance
            mock_lock_instance.__aenter__.return_value = mock_lock_instance
            mock_lock_instance.__aexit__.return_value = None

            await tracker.cleanup(mock_pipeline)

        mock_redis.zrange.assert_called_once()
        # Should call rm for each obsolete ID
        assert mock_pipeline.delete.call_count == 2

    @pytest.mark.anyio("asyncio")
    async def test_add_schedule_event_future(self) -> None:
        tracker = Tracker()
        mock_redis = AsyncMock()
        mock_pipeline = AsyncMock()
        tracker.redis = mock_redis
        mock_redis.get.return_value = None

        future_time = datetime.now(UTC) + timedelta(minutes=10)
        event = ScheduleEvent(
            action="add",
            time=future_time,
            route_id="Red",
            route_type=1,
            headsign="Alewife",
            id="prediction:123",
            stop="Davis",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )

        await tracker.add(event, mock_pipeline, "add")

        # Should set the trip key, event key, and zadd to time index
        assert mock_pipeline.set.call_count == 2
        mock_pipeline.zadd.assert_called_once()

    @pytest.mark.anyio("asyncio")
    async def test_add_schedule_event_past(self) -> None:
        tracker = Tracker()
        mock_redis = AsyncMock()
        mock_pipeline = AsyncMock()
        tracker.redis = mock_redis

        past_time = datetime.now(UTC) - timedelta(minutes=10)
        event = ScheduleEvent(
            action="add",
            time=past_time,
            route_id="Red",
            route_type=1,
            headsign="Alewife",
            id="prediction:123",
            stop="Davis",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )

        await tracker.add(event, mock_pipeline, "add")

        # Should not add past events
        mock_pipeline.set.assert_not_called()
        mock_pipeline.zadd.assert_not_called()

    @pytest.mark.anyio("asyncio")
    async def test_add_vehicle_event(self) -> None:
        tracker = Tracker()
        mock_redis = AsyncMock()
        mock_pipeline = AsyncMock()
        tracker.redis = mock_redis
        mock_redis.get.return_value = None

        event = VehicleRedisSchema(
            longitude=-71.0589,
            latitude=42.3601,
            direction_id=0,
            current_status="IN_TRANSIT_TO",
            id="vehicle-123",
            action="add",
            route="Red",
            update_time=datetime.now(UTC),
            speed=25.0,
        )

        await tracker.add(event, mock_pipeline, "add")

        # Should set vehicle data and add to position set
        mock_pipeline.set.assert_called_once()
        mock_pipeline.sadd.assert_called_once()

    @pytest.mark.anyio("asyncio")
    async def test_rm_schedule_event(self) -> None:
        tracker = Tracker()
        mock_pipeline = AsyncMock()

        event = ScheduleEvent(
            action="remove",
            time=datetime.now(UTC),
            route_id="Red",
            route_type=1,
            headsign="Alewife",
            id="prediction:123",
            stop="Davis",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )

        await tracker.rm(event, mock_pipeline)

        # Should delete event and remove from time index
        mock_pipeline.delete.assert_called_once_with(event.id)
        mock_pipeline.zrem.assert_called_once()

    @pytest.mark.anyio("asyncio")
    async def test_rm_vehicle_event(self) -> None:
        tracker = Tracker()
        mock_pipeline = AsyncMock()

        event = VehicleRedisSchema(
            longitude=-71.0589,
            latitude=42.3601,
            direction_id=0,
            current_status="IN_TRANSIT_TO",
            id="vehicle-123",
            action="remove",
            route="Red",
            update_time=datetime.now(UTC),
        )

        await tracker.rm(event, mock_pipeline)

        # Should delete vehicle data
        mock_pipeline.delete.assert_called_once_with("vehicle-vehicle-123")

    @pytest.mark.anyio("asyncio")
    async def test_process_queue_item_reset(self) -> None:
        tracker = Tracker()
        mock_pipeline = AsyncMock()

        event = ScheduleEvent(
            action="reset",
            time=datetime.now(UTC) + timedelta(minutes=10),
            route_id="Red",
            route_type=1,
            headsign="Alewife",
            id="prediction:123",
            stop="Davis",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )

        with patch.object(tracker, "add") as mock_add:
            await tracker.process_queue_item(event, mock_pipeline)
            mock_add.assert_called_once_with(event, mock_pipeline, "reset")

    @pytest.mark.anyio("asyncio")
    async def test_process_queue_item_remove(self) -> None:
        tracker = Tracker()
        mock_pipeline = AsyncMock()

        event = ScheduleEvent(
            action="remove",
            time=datetime.now(UTC),
            route_id="Red",
            route_type=1,
            headsign="Alewife",
            id="prediction:123",
            stop="Davis",
            transit_time_min=5,
            trip_id="trip-123",
            alerting=False,
            bikes_allowed=True,
        )

        with patch.object(tracker, "rm") as mock_rm:
            await tracker.process_queue_item(event, mock_pipeline)
            mock_rm.assert_called_once_with(event, mock_pipeline)

    @pytest.mark.anyio("asyncio")
    @patch.dict(os.environ, {"IMT_ENABLE_MQTT": "true"})
    @patch("schedule_tracker.publish")
    async def test_send_mqtt_enabled(self, mock_publish: MagicMock) -> None:
        tracker = Tracker()

        with patch.object(tracker, "fetch_mqtt_events") as mock_fetch:
            future_time = datetime.now(UTC) + timedelta(minutes=10)
            mock_event = ScheduleEvent(
                action="add",
                time=future_time,
                route_id="Red",
                route_type=1,
                headsign="Alewife",
                id="prediction:123",
                stop="Davis",
                transit_time_min=5,
                trip_id="trip-123",
                alerting=False,
                bikes_allowed=True,
                show_on_display=True,
            )
            mock_fetch.return_value = [mock_event]

            await tracker.send_mqtt()

            mock_publish.multiple.assert_called_once()

    @pytest.mark.anyio("asyncio")
    @patch.dict(os.environ, {"IMT_ENABLE_MQTT": "false"})
    @patch("schedule_tracker.publish")
    async def test_send_mqtt_disabled(self, mock_publish: MagicMock) -> None:
        tracker = Tracker()

        await tracker.send_mqtt()

        mock_publish.multiple.assert_not_called()


class TestProcessQueue:
    @patch("schedule_tracker.Tracker")
    @patch("schedule_tracker.Runner")
    @patch("time.sleep")
    def test_process_queue(
        self,
        mock_sleep: MagicMock,
        mock_runner: MagicMock,
        mock_tracker_class: MagicMock,
    ) -> None:
        mock_tracker = MagicMock()
        mock_tracker_class.return_value = mock_tracker

        mock_runner_instance = MagicMock()
        mock_runner.return_value.__enter__.return_value = mock_runner_instance

        # Avoid un-awaited coroutine warnings by consuming the coroutine passed to run()
        def _consume(coro: object) -> None:
            try:
                # Close coroutine objects to suppress RuntimeWarning about un-awaited coroutines
                close = getattr(coro, "close", None)
                if callable(close):
                    close()
            except Exception:
                pass

        mock_runner_instance.run.side_effect = _consume

        queue = Queue[ScheduleEvent]()

        # Mock sleep to raise an exception after first iteration to exit the loop
        mock_sleep.side_effect = [None, KeyboardInterrupt()]

        with pytest.raises(KeyboardInterrupt):
            process_queue(queue)

        mock_tracker_class.assert_called_once()
        mock_runner_instance.run.assert_called()
        assert mock_sleep.call_count == 2


if __name__ == "__main__":
    pytest.main([__file__])
