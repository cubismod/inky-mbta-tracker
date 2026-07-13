import logging
import os
import time
from datetime import UTC, datetime, timedelta
from statistics import fmean
from typing import Optional
from zoneinfo import ZoneInfo

import anyio
import humanize
from anyio import to_thread
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream
from consts import HOUR
from geo_math import distance
from geojson import Feature, Point
from otel_config import get_tracer, is_otel_enabled
from otel_utils import (
    add_entity_id_attribute,
    add_event_to_span,
    add_span_attributes,
    add_transaction_ids_to_span,
    set_span_error,
    set_vehicle_track_transaction_id,
    should_trace_operation,
)
from paho.mqtt import MQTTException, publish
from prometheus import (
    batch_flushes,
    current_buffer_used,
    last_batch_flush_ts,
    last_vehicle_write_ts,
    max_buffer_size,
    open_receive_streams,
    open_send_streams,
    pos_data_count,
    redis_commands,
    schedule_batch_items,
    schedule_events,
    tasks_waiting_receive,
    tasks_waiting_send,
    vehicle_batch_items,
    vehicle_events,
    vehicle_speeds,
)
from pydantic import ValidationError
from redis import ResponseError
from redis.asyncio.client import Pipeline, Redis
from redis_cache import get_cache
from redis_lock.asyncio import RedisLock
from shared_types.shared_types import (
    ScheduleEvent,
    VehicleRedisSchema,
    VehicleSpeedHistory,
)

logger = logging.getLogger("schedule_tracker")
SPEED_HISTORY_TTL_SECONDS = 300
MIN_APPROXIMATE_SPEED_SAMPLE_SECONDS = 5
METERS_PER_SECOND_TO_MPH = 2.2369362921


def dummy_schedule_event(event_id: str) -> ScheduleEvent:
    return ScheduleEvent(
        action="remove",
        headsign="N/A",
        route_id="N/A",
        route_type=1,
        id=event_id,
        stop="N/A",
        time=datetime.now(UTC),
        transit_time_min=0,
        trip_id="N/A",
        alerting=False,
        bikes_allowed=False,
    )


class Tracker:
    redis: Redis

    def __init__(self) -> None:
        r = Redis(
            host=os.environ.get("IMT_REDIS_ENDPOINT") or "",
            port=int(os.environ.get("IMT_REDIS_PORT", "6379") or ""),
            password=os.environ.get("IMT_REDIS_PASSWORD") or "",
        )
        self.redis = r

    @staticmethod
    def calculate_time_diff(event: ScheduleEvent) -> timedelta:
        res = event.time - datetime.now().astimezone(UTC)
        if res < timedelta(seconds=5):
            return timedelta(minutes=5)
        return res

    @staticmethod
    def log_prediction(event: ScheduleEvent) -> None:
        logger.debug(
            "action=%s time=%s route_id=%s route_type=%s headsign=%s stop=%s id=%s, "
            "transit_time_min=%s, alerting=%s, bikes_allowed=%s",
            event.action,
            event.time.astimezone(ZoneInfo("America/New_York")).strftime("%c"),
            event.route_id,
            event.route_type,
            event.headsign,
            event.stop,
            event.id,
            event.transit_time_min,
            event.alerting,
            event.bikes_allowed,
        )

    @staticmethod
    def log_vehicle(event: VehicleRedisSchema) -> None:
        logger.debug(
            "action=%s route=%s vehicle_id=%s lat=%s long=%s status=%s speed=%s",
            event.action,
            event.route,
            event.id,
            event.latitude,
            event.longitude,
            event.current_status,
            event.speed,
        )

    async def write_event_heartbeat(self, route_id: str, pipeline: Optional[Pipeline] = None) -> None:
        heartbeat_key = f"heartbeat:events:{route_id}"
        try:
            target = pipeline or self.redis
            await target.set(heartbeat_key, datetime.now(UTC).isoformat(), ex=2 * HOUR)
            redis_commands.labels("set").inc()
        except ResponseError as err:
            logger.error("Failed to write event heartbeat", exc_info=err)

    @staticmethod
    def is_speed_reasonable(speed: float, line: str) -> bool:
        if line.startswith("CR") and speed <= 86:
            return True
        if (line == "Orange" or line == "Red") and speed <= 56:
            return True
        if line == "Blue" and speed <= 50:
            return True
        if (line.startswith("7") or line.isnumeric()) and speed <= 66:
            return True
        if (line.startswith("Green") or line == "Mattapan") and speed <= 40:
            return True
        return False

    async def write_vehicle_speed_history(
        self, cache_id: str, event: VehicleRedisSchema, speed: float,
        pipeline: Optional[Pipeline] = None,
    ) -> None:
        data = VehicleSpeedHistory(
            long=event.longitude,
            lat=event.latitude,
            speed=speed,
            update_time=event.update_time,
        ).model_dump_json()
        target = pipeline or self.redis
        await target.set(cache_id, value=data, ex=SPEED_HISTORY_TTL_SECONDS)
        redis_commands.labels("set").inc()

    # calculate an approximate vehicle speed using the previous position and timestamp
    # returns (the speed, if this was an approximate calculation)
    async def calculate_vehicle_speed(
        self, event: VehicleRedisSchema, pipeline: Optional[Pipeline] = None
    ) -> tuple[Optional[float], bool]:
        try:
            cache_id = f"vehicle:speed:history:{event.id}"

            if event.speed is not None:
                await self.write_vehicle_speed_history(cache_id, event, event.speed, pipeline)
                return event.speed, False

            if event.current_status == "STOPPED_AT":
                await self.write_vehicle_speed_history(cache_id, event, 0, pipeline)
                return None, False

            last_event = await get_cache(self.redis, cache_id)
            redis_commands.labels("get").inc()

            if not last_event:
                await self.write_vehicle_speed_history(cache_id, event, 0, pipeline)
                return None, False

            last_event_validated = VehicleSpeedHistory.model_validate_json(
                last_event, strict=False
            )

            duration_seconds = (
                event.update_time - last_event_validated.update_time
            ).total_seconds()
            if duration_seconds <= 0:
                logger.debug(
                    "Skipping speed calculation for %s vehicle %s: "
                    "non-positive duration %s seconds",
                    event.route,
                    event.id,
                    duration_seconds,
                )
                return None, False

            if duration_seconds < MIN_APPROXIMATE_SPEED_SAMPLE_SECONDS:
                if last_event_validated.speed > 0:
                    return last_event_validated.speed, True
                return None, False

            start = Feature(
                geometry=Point(
                    (
                        last_event_validated.long,
                        last_event_validated.lat,
                    )
                )
            )
            end = Feature(geometry=Point((event.longitude, event.latitude)))

            distance_meters = distance(start, end, "m")
            if distance_meters == 0:
                await self.write_vehicle_speed_history(cache_id, event, 0, pipeline)
                return 0, True

            meters_per_second = distance_meters / duration_seconds
            speed = meters_per_second * METERS_PER_SECOND_TO_MPH
            if (
                last_event_validated.speed != 0
                and duration_seconds < 30
                and self.is_speed_reasonable(last_event_validated.speed, event.route)
            ):
                speed = fmean([speed, last_event_validated.speed])

            if not self.is_speed_reasonable(speed, event.route):
                logger.debug(
                    "Rejecting speed calculation for %s vehicle %s: speed %s mph is unreasonable",
                    event.route,
                    event.id,
                    speed,
                )
                # throw out insane predictions
                if last_event_validated.speed > 0:
                    return (last_event_validated.speed, True)
                return None, False

            await self.write_vehicle_speed_history(cache_id, event, speed, pipeline)
            return speed, True
        except ResponseError as err:
            logger.error("unable to get redis event", exc_info=err)
        except ValidationError as err:
            logger.error("unable to validate obj", exc_info=err)
        return None, False

    async def cleanup(self, pipeline: Pipeline) -> None:
        try:
            async with RedisLock(
                self.redis, "cleanup_operation", blocking_timeout=5, expire_timeout=10
            ):
                obsolete_ids = await self.redis.zrange(
                    "time",
                    start=0,
                    end=int(datetime.now().astimezone(UTC).timestamp()),
                    withscores=False,
                    byscore=True,
                )
                redis_commands.labels("zrange").inc()
                batch_size = 10
                for i in range(0, len(obsolete_ids), batch_size):
                    batch = obsolete_ids[i : i + batch_size]
                    for item in batch:
                        dec_i = item.decode("utf-8")
                        if dec_i:
                            await self.rm(dummy_schedule_event(dec_i), pipeline)

                    if len(batch) > 0:
                        try:
                            await pipeline.execute()
                            redis_commands.labels("execute").inc()
                            # Create a new pipeline for the next batch
                            pipeline = self.redis.pipeline()
                        except ResponseError as err:
                            logger.error(
                                "Unable to execute cleanup batch %s",
                                i // batch_size,
                                exc_info=err,
                            )
                            break

        except ResponseError as err:
            logger.error("unable to cleanup old entries", exc_info=err)

    async def add(
        self, event: ScheduleEvent | VehicleRedisSchema, pipeline: Pipeline, action: str
    ) -> None:
        if isinstance(event, ScheduleEvent):
            # only add events in the future
            if event.time > datetime.now().astimezone(UTC):
                trip_redis_key = f"trip:{event.trip_id}:{event.stop.replace(' ', '_')}"
                existing_event = await self.redis.get(trip_redis_key)
                redis_commands.labels("get").inc()
                if existing_event:
                    dec_ee = existing_event.decode("utf-8")
                    if dec_ee != event.id and dec_ee.startswith("schedule"):
                        logger.info(
                            "Removing existing schedule entry with id %s as it has been replaced with %s, trip_id=%s",
                            dec_ee,
                            event.id,
                            event.trip_id,
                        )
                        await self.rm(dummy_schedule_event(existing_event), pipeline)
                    if dec_ee.startswith("prediction") and event.id.startswith(
                        "schedule"
                    ):
                        # don't override realtime predictions
                        return
                await pipeline.set(
                    trip_redis_key,
                    event.id,
                    ex=(event.time - datetime.now().astimezone(UTC))
                    + timedelta(hours=1),
                )
                redis_commands.labels("set").inc()

                await pipeline.set(
                    event.id,
                    event.model_dump_json(exclude={"trip_id"}),
                    ex=self.calculate_time_diff(event) + timedelta(minutes=1),
                )
                redis_commands.labels("set").inc()

                await pipeline.zadd("time", {event.id: int(event.time.timestamp())})
                redis_commands.labels("zadd").inc()

                schedule_events.labels(action, event.route_id, event.stop).inc()
                self.log_prediction(event)
                await self.write_event_heartbeat(event.route_id, pipeline)
        if isinstance(event, VehicleRedisSchema):
            # Generate vehicle tracking transaction ID for individual vehicle updates
            vehicle_track_txn_id = set_vehicle_track_transaction_id(event.id)
            logger.debug(
                "Processing vehicle %s with transaction ID: %s",
                event.id,
                vehicle_track_txn_id,
            )

            redis_key = f"vehicle:{event.id}"
            event.speed, approximate = await self.calculate_vehicle_speed(event, pipeline)
            event.approximate_speed = approximate

            exp_min = 4
            if event.speed:
                route = event.route
                if route.startswith("CR"):
                    route = "Commuter Rail"
                    exp_min = 10
                if route.startswith("7"):
                    route = "Silver Line"
                if route.startswith("Green"):
                    route = "Green"
                if route in ["Green", "Red", "Blue", "Orange", "Mattapan"]:
                    route += " Line"
                vehicle_speeds.labels(route_id=route, vehicle_id=event.id).set(
                    event.speed
                )

            await pipeline.set(
                redis_key, event.model_dump_json(), ex=timedelta(minutes=exp_min)
            )
            redis_commands.labels("set").inc()

            await pipeline.sadd("pos-data", redis_key)  # type: ignore[misc]
            vehicle_events.labels(action, event.route).inc()
            redis_commands.labels("sadd").inc()
            last_vehicle_write_ts.labels("tracker").set(time.time())

            self.log_vehicle(event)
            await self.write_event_heartbeat(event.route, pipeline)

    async def rm(
        self, event: ScheduleEvent | VehicleRedisSchema, pipeline: Pipeline
    ) -> None:
        try:
            if isinstance(event, ScheduleEvent):
                await pipeline.delete(event.id)
                redis_commands.labels("delete").inc()

                await pipeline.zrem("time", int(event.time.timestamp()))
                redis_commands.labels("zrem").inc()
                schedule_events.labels("remove", event.route_id, event.stop).inc()
                self.log_prediction(event)
            if isinstance(event, VehicleRedisSchema):
                redis_key = f"vehicle:{event.id}"
                await pipeline.delete(redis_key)
                redis_commands.labels("delete").inc()
                await pipeline.srem("pos-data", redis_key)  # type: ignore[misc]
                redis_commands.labels("srem").inc()
                vehicle_events.labels("remove", event.route).inc()

                self.log_vehicle(event)
        except ResponseError as err:
            logger.error("unable to get key from redis", exc_info=err)
        except ValidationError as err:
            logger.error("unable to validate ScheduleEvent model", exc_info=err)

    async def fetch_mqtt_events(self) -> list[ScheduleEvent]:
        ret = list[ScheduleEvent]()
        try:
            now = datetime.now().astimezone(UTC)
            max_time = now + timedelta(hours=12)
            events = await self.redis.zrange(
                "time",
                start=int(now.timestamp()),
                end=int(max_time.timestamp()),
                byscore=True,
                withscores=False,
                num=50,
                offset=0,
            )
            redis_commands.labels("zrange").inc()
            for event in events:
                res = await self.redis.get(event.decode("utf-8"))
                redis_commands.labels("get").inc()
                if res:
                    v_event = ScheduleEvent.model_validate_json(
                        res.decode("utf-8"), strict=False
                    )
                    if v_event.time - timedelta(
                        minutes=v_event.transit_time_min
                    ) > datetime.now().astimezone(UTC):
                        ret.append(v_event)
        except ResponseError as err:
            logger.error("unable to run redis command", exc_info=err)
        except ValidationError as err:
            logger.error("unable to validate schema", exc_info=err)

        return ret

    @staticmethod
    def get_route_icon(event: ScheduleEvent) -> str:
        match event.route_type:
            case 0:
                return "🚊"
            case 1:
                return "🚇"
            case 2:
                return "🚆"
            case 3:
                return "🚍"
            case 4:
                return "⛴️"
            case _:
                return ""

    async def send_mqtt(self) -> None:
        if os.getenv("IMT_ENABLE_MQTT", "true") == "true":
            msgs = list[tuple[str, str]]()
            events = await self.fetch_mqtt_events()
            for i, event in enumerate(events):
                if not event.show_on_display:
                    continue
                topic = f"imt/departure_time{i}"
                payload = self.prediction_display(event)
                msgs.append((topic, payload))

                topic = f"imt/destination_and_stop{i}"
                payload = f"{self.get_route_icon(event)} [{event.route_id}] {event.headsign}: {event.stop}"
                if event.id.startswith("prediction"):
                    payload = f"📶{payload}"
                if event.alerting:
                    payload = f"⚠️{payload}"
                if event.bikes_allowed:
                    payload = f"🚲{payload}"

            if len(msgs) > 0:
                try:
                    # Offload blocking MQTT publish using anyio thread helpers
                    await to_thread.run_sync(
                        publish.multiple,
                        msgs,  # type: ignore
                        os.getenv("IMT_MQTT_HOST", ""),
                        int(os.getenv("IMT_MQTT_PORT", "1883")),
                        "",
                        60,
                        None,
                        {
                            "username": os.getenv("IMT_MQTT_USER", ""),
                            "password": os.getenv("IMT_MQTT_PASS", ""),
                        },
                    )
                except MQTTException as err:
                    logger.error("unable to send messages to MQTT", exc_info=err)

    @staticmethod
    def prediction_display(event: ScheduleEvent) -> str:
        prediction_indicator = ""

        rounded_time = round(
            (event.time.timestamp() - datetime.now().astimezone(UTC).timestamp())
        )
        if rounded_time > 0:
            return f"🕒 {humanize.naturaldelta(timedelta(seconds=rounded_time))}"
        if rounded_time == 0:
            return f"{prediction_indicator} BRD"
        if rounded_time < 0:
            return f"{prediction_indicator} DEP"
        return ""

    async def process_queue_item(
        self, event: ScheduleEvent | VehicleRedisSchema, pipeline: Pipeline
    ) -> None:
        match event.action:
            case "reset":
                await self.add(event, pipeline, "reset")
            case "add":
                await self.add(event, pipeline, "add")
            case "update":
                await self.add(event, pipeline, "update")
            case "remove":
                await self.rm(event, pipeline)


async def process_queue_async(
    receive_stream: MemoryObjectReceiveStream[ScheduleEvent | VehicleRedisSchema],
    tg: TaskGroup,
) -> None:
    """Async consumer that micro-batches stream items into Redis pipelines.

    Improves throughput and correctness by:
    - Building a single pipeline per batch (transaction=False)
    - Processing items sequentially to avoid pipeline race conditions
    - Flushing on size or latency thresholds
    - Throttling MQTT/cleanup to a minimum interval

    Note: This is a long-running background task. Individual operations are traced
    via flush_batch spans rather than a single root span for the entire task.
    """
    tracker = Tracker()

    batch_max_items = int(os.getenv("IMT_QUEUE_BATCH_SIZE", "256"))
    batch_latency_ms = int(os.getenv("IMT_QUEUE_BATCH_LATENCY_MS", "50"))
    mqtt_min_interval = int(os.getenv("IMT_MQTT_MIN_INTERVAL_SEC", "5"))
    last_mqtt_ts = 0.0

    async def flush_batch(items: list[ScheduleEvent | VehicleRedisSchema]) -> None:
        if not items:
            return

        # Generate route monitoring transaction ID for this batch processing cycle
        route_ids = set()
        vehicle_count = 0
        schedule_count = 0
        for item in items:
            if isinstance(item, ScheduleEvent):
                route_ids.add(item.route_id)
                schedule_count += 1
            elif isinstance(item, VehicleRedisSchema):
                route_ids.add(item.route)
                vehicle_count += 1

        # Create transaction ID with multiple routes if applicable
        route_context = "_".join(sorted(route_ids)) if route_ids else "batch"
        vehicle_batch_items.labels("tracker").set(vehicle_count)
        schedule_batch_items.labels("tracker").set(schedule_count)

        # Use OTEL tracing with high-volume sampling for batch processing
        tracer = get_tracer(__name__) if is_otel_enabled() else None
        should_trace = tracer and should_trace_operation("high_volume")

        pipeline: Pipeline = tracker.redis.pipeline(transaction=False)

        if should_trace and tracer:
            with tracer.start_as_current_span("schedule_tracker.flush_batch") as span:
                add_span_attributes(
                    span,
                    {
                        "batch.size": len(items),
                        "batch.has_schedule_events": any(
                            isinstance(it, ScheduleEvent) for it in items
                        ),
                        "batch.has_vehicle_events": any(
                            isinstance(it, VehicleRedisSchema) for it in items
                        ),
                        "batch.route_context": route_context,
                    },
                )

                # Add transaction IDs to the span
                add_transaction_ids_to_span(span)

                try:
                    for it in items:
                        # Create individual spans for vehicle events to enable transaction visibility
                        if isinstance(it, VehicleRedisSchema):
                            with tracer.start_as_current_span(
                                "schedule_tracker.process_vehicle"
                            ) as vehicle_span:
                                # Set vehicle-specific transaction ID for this span
                                set_vehicle_track_transaction_id(it.id)
                                add_transaction_ids_to_span(vehicle_span)
                                add_entity_id_attribute(
                                    vehicle_span,
                                    "vehicle.id",
                                    it.id,
                                    entity_type="vehicle",
                                )
                                add_span_attributes(
                                    vehicle_span,
                                    {
                                        "vehicle.route": it.route,
                                        "vehicle.action": it.action,
                                        "vehicle.has_stop": bool(it.stop),
                                        "vehicle.has_occupancy": bool(
                                            it.occupancy_status
                                        ),
                                    },
                                )
                                await tracker.process_queue_item(it, pipeline)
                        else:
                            # Process non-vehicle events normally
                            add_event_to_span(
                                span,
                                "schedule_event_processed",
                                {
                                    "route.id": it.route_id,
                                    "event.action": it.action,
                                },
                            )
                            await tracker.process_queue_item(it, pipeline)

                    # Housekeeping: prune expired entries in the same round trip
                    pipeline.zremrangebyscore(
                        "time", "-inf", str(datetime.now().timestamp())
                    )
                    redis_commands.labels("zremrangebyscore").inc()

                    await pipeline.execute()
                    redis_commands.labels("execute").inc()
                    batch_flushes.labels("tracker", "ok").inc()
                    last_batch_flush_ts.labels("tracker").set(time.time())
                    try:
                        pos_data_size = await tracker.redis.scard("pos-data")  # type: ignore[misc]
                        pos_data_count.labels("tracker").set(pos_data_size)
                        redis_commands.labels("scard").inc()
                        span.set_attribute("redis.pos_data.size", pos_data_size)
                    except ResponseError as err:
                        logger.error("Unable to read pos-data size", exc_info=err)

                    add_event_to_span(
                        span, "batch_flushed", {"items_processed": len(items)}
                    )
                except ResponseError as err:
                    set_span_error(span, err)
                    logger.error(
                        "Unable to communicate with Redis during batch flush",
                        exc_info=err,
                    )
                    batch_flushes.labels("tracker", "redis_error").inc()
                except (
                    AttributeError,
                    TypeError,
                    ValueError,
                    RuntimeError,
                    OSError,
                ) as err:
                    set_span_error(span, err)
                    logger.error("Unexpected error during batch flush", exc_info=err)
                    batch_flushes.labels("tracker", "error").inc()
        else:
            # No tracing - original logic
            try:
                for it in items:
                    await tracker.process_queue_item(it, pipeline)

                pipeline.zremrangebyscore(
                    "time", "-inf", str(datetime.now().timestamp())
                )
                redis_commands.labels("zremrangebyscore").inc()

                await pipeline.execute()
                redis_commands.labels("execute").inc()
                batch_flushes.labels("tracker", "ok").inc()
                last_batch_flush_ts.labels("tracker").set(time.time())
                try:
                    pos_data_count.labels("tracker").set(
                        await tracker.redis.scard("pos-data")  # type: ignore[misc]
                    )
                    redis_commands.labels("scard").inc()
                except ResponseError as err:
                    logger.error("Unable to read pos-data size", exc_info=err)
            except ResponseError as err:
                logger.error(
                    "Unable to communicate with Redis during batch flush", exc_info=err
                )
                batch_flushes.labels("tracker", "redis_error").inc()
            except (
                AttributeError,
                TypeError,
                ValueError,
                RuntimeError,
                OSError,
            ) as err:
                logger.error("Unexpected error during batch flush", exc_info=err)
                batch_flushes.labels("tracker", "error").inc()

    # Main consumer loop: receive, then drain to form a batch
    while True:
        try:
            # Block for the first item to start a batch
            first = await receive_stream.receive()
            batch: list[ScheduleEvent | VehicleRedisSchema] = [first]

            # Compute deadline for latency-bounded batching
            deadline = anyio.current_time() + (batch_latency_ms / 1000.0)
            while len(batch) < batch_max_items:
                remaining = deadline - anyio.current_time()
                if remaining <= 0:
                    break
                # Try to pull more items until either batch is full or latency budget expires
                with anyio.move_on_after(remaining):
                    nxt = await receive_stream.receive()
                    batch.append(nxt)
                    continue
                break

            await flush_batch(batch)

            # Throttle MQTT/cleanup by time rather than per-item randomness
            now = time.time()
            if now - last_mqtt_ts >= mqtt_min_interval:
                try:
                    async with RedisLock(
                        tracker.redis,
                        "send_mqtt",
                        blocking_timeout=5,
                        expire_timeout=10,
                    ):
                        cleanup_pipeline = tracker.redis.pipeline(transaction=False)
                        await tracker.cleanup(cleanup_pipeline)
                        await cleanup_pipeline.execute()
                        redis_commands.labels("execute").inc()
                        await tracker.send_mqtt()

                        stats = receive_stream.statistics()

                        current_buffer_used.labels("main").set(
                            stats.current_buffer_used
                        )
                        max_buffer_size.labels("main").set(stats.max_buffer_size)
                        open_receive_streams.labels("main").set(
                            stats.open_receive_streams
                        )
                        open_send_streams.labels("main").set(stats.open_send_streams)
                        tasks_waiting_receive.labels("main").set(
                            stats.tasks_waiting_receive
                        )
                        tasks_waiting_send.labels("main").set(stats.tasks_waiting_send)
                    last_mqtt_ts = now
                except ResponseError:
                    logger.error("Error during MQTT/cleanup cycle", exc_info=True)
        except (RuntimeError, OSError) as err:
            logger.error("Failed in queue consumer loop", exc_info=err)
