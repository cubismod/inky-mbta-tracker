import logging
import os
import random
import threading
import time
from asyncio import QueueEmpty, Runner
from datetime import UTC, datetime, timedelta
from queue import Queue
from statistics import fmean
from typing import Optional
from zoneinfo import ZoneInfo

import anyio
import humanize
from anyio import to_thread
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream
from geojson import Feature, Point
from otel_config import get_tracer, is_otel_enabled
from otel_utils import (
    add_event_to_span,
    add_span_attributes,
    add_transaction_ids_to_span,
    set_span_error,
    set_vehicle_track_transaction_id,
    should_trace_operation,
)
from paho.mqtt import MQTTException, publish
from prometheus import (
    current_buffer_used,
    max_buffer_size,
    open_receive_streams,
    open_send_streams,
    redis_commands,
    schedule_events,
    tasks_waiting_receive,
    tasks_waiting_send,
    vehicle_events,
    vehicle_speeds,
)
from pydantic import ValidationError
from redis import ResponseError
from redis.asyncio.client import Pipeline, Redis
from redis_cache import check_cache, write_cache
from redis_lock.asyncio import RedisLock
from shared_types.schema_versioner import export_schema_key_counts
from shared_types.shared_types import (
    ScheduleEvent,
    VehicleRedisSchema,
    VehicleSpeedHistory,
)
from tenacity import (
    before_sleep_log,
    retry,
    wait_random_exponential,
)
from turfpy import measurement

logger = logging.getLogger("schedule_tracker")


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
    def str_timestamp(event: ScheduleEvent) -> str:
        return str(event.time.timestamp())

    @staticmethod
    def calculate_time_diff(event: ScheduleEvent) -> timedelta:
        res = event.time - datetime.now().astimezone(UTC)
        if res < timedelta(seconds=5):
            return timedelta(minutes=5)
        return res

    @staticmethod
    def log_prediction(event: ScheduleEvent) -> None:
        logger.debug(
            f"action={event.action} time={event.time.astimezone(ZoneInfo('US/Eastern')).strftime('%c')} route_id={event.route_id} route_type={event.route_type} headsign={event.headsign} stop={event.stop} id={event.id}, transit_time_min={event.transit_time_min}, alerting={event.alerting}, bikes_allowed={event.bikes_allowed}"
        )

    @staticmethod
    def log_vehicle(event: VehicleRedisSchema) -> None:
        logger.debug(
            f"action={event.action} route={event.route} vehicle_id={event.id} lat={event.latitude} long={event.longitude} status={event.current_status} speed={event.speed}"
        )

    @staticmethod
    def is_speed_reasonable(speed: float, line: str) -> bool:
        if (line == "Orange" or line == "Red") and speed <= 56:
            return True
        if line == "Blue" and speed <= 50:
            return True
        if line.startswith("7") and speed <= 66:
            return True
        if line.startswith("CR") and speed <= 86:
            return True
        if (line.startswith("Green") or line == "Mattapan") and speed <= 40:
            return True
        return False

    # calculate an approximate vehicle speed using the previous position and timestamp
    # returns (the speed, if this was an approximate calculation)
    async def calculate_vehicle_speed(
        self, event: VehicleRedisSchema
    ) -> tuple[Optional[float], bool]:
        try:
            cache_id = f"vehicle:speed:history:{event.id}"
            if event.current_status != "STOPPED_AT" and not event.speed:
                last_event = await check_cache(self.redis, cache_id)
                redis_commands.labels("get").inc()

                if last_event:
                    last_event_validated = VehicleSpeedHistory.model_validate_json(
                        last_event, strict=False
                    )

                    start = Feature(
                        geometry=Point(
                            (
                                last_event_validated.long,
                                last_event_validated.lat,
                            )
                        )
                    )
                    end = Feature(geometry=Point((event.longitude, event.latitude)))

                    distance: float = measurement.distance(start, end, "m")
                    duration = event.update_time - last_event_validated.update_time

                    if duration.seconds != 0 and distance != 0:
                        meters_per_second = distance / duration.seconds
                        speed = meters_per_second * 2.2369362921
                        if last_event_validated.speed != 0 and duration.seconds < 30:
                            speed = fmean([speed, last_event_validated.speed])

                        if not self.is_speed_reasonable(speed, event.route):
                            logger.debug(
                                f"Rejecting speed calculation for {event.route} vehicle {event.id}: speed {speed} mph is unreasonable"
                            )
                            # throw out insane predictions
                            return (round(last_event_validated.speed, 2), True)
                        else:
                            await write_cache(
                                self.redis,
                                cache_id,
                                VehicleSpeedHistory(
                                    long=event.longitude,
                                    lat=event.latitude,
                                    speed=speed,
                                    update_time=event.update_time,
                                ).model_dump_json(),
                                exp_sec=300,
                            )
                            return round(speed, 2), True
                else:
                    await write_cache(
                        self.redis,
                        cache_id,
                        VehicleSpeedHistory(
                            long=event.longitude,
                            lat=event.latitude,
                            speed=0,
                            update_time=event.update_time,
                        ).model_dump_json(),
                        exp_sec=300,
                    )
            if event.speed:
                return event.speed, False
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
                                f"Unable to execute cleanup batch {i // batch_size}",
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
                            f"Removing existing schedule entry with id {dec_ee} as it has been replaced with {event.id}, trip_id={event.trip_id}"
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
        if isinstance(event, VehicleRedisSchema):
            # Generate vehicle tracking transaction ID for individual vehicle updates
            vehicle_track_txn_id = set_vehicle_track_transaction_id(event.id)
            logger.debug(
                f"Processing vehicle {event.id} with transaction ID: {vehicle_track_txn_id}"
            )

            redis_key = f"vehicle:{event.id}"
            event.speed, approximate = await self.calculate_vehicle_speed(event)
            event.approximate_speed = approximate

            if event.speed:
                route = event.route
                if route.startswith("CR"):
                    route = "Commuter Rail"
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
                redis_key, event.model_dump_json(), ex=timedelta(minutes=10)
            )
            redis_commands.labels("set").inc()

            await pipeline.sadd("pos-data", redis_key)  # type: ignore[misc]
            vehicle_events.labels(action, event.route).inc()
            redis_commands.labels("sadd").inc()

            self.log_vehicle(event)

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
                await pipeline.delete(f"vehicle-{event.id}")
                redis_commands.labels("delete").inc()
                vehicle_events.labels("remove", event.route).inc()

                self.log_vehicle(event)
        except ResponseError as err:
            logger.error("unable to get key from redis", exc_info=err)
        except ValidationError as err:
            logger.error("unable to validate ScheduleEvent model", exc_info=err)

    @staticmethod
    def __determine_color(event: ScheduleEvent) -> str:
        if event.route_id == "Red":
            return "#DA291C"
        if event.route_id.startswith("Green"):
            return "#00843D"
        if event.route_id == "Orange":
            return "#ED8B00"
        if event.route_id == "Blue":
            return "#003DA5"
        if event.route_id.startswith("SL"):
            return "#7C878E"
        if event.route_id.startswith("CR"):
            return "#80276C"
        return "black"

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


async def execute(
    tracker: Tracker, queue: Queue[ScheduleEvent] | Queue[VehicleRedisSchema]
) -> None:
    pipeline = tracker.redis.pipeline()
    while queue.qsize() != 0:
        try:
            item = queue.get()
            await tracker.process_queue_item(item, pipeline)
        except QueueEmpty:
            break
    try:
        await pipeline.execute()
        redis_commands.labels("execute").inc()
        await tracker.redis.zremrangebyscore(
            "time", "-inf", str(datetime.now().timestamp())
        )
        redis_commands.labels("zremrangebyscore").inc()
    except ResponseError as err:
        logger.error("Unable to communicate with Redis", exc_info=err)

    should_send_mqtt = random.randint(0, 10) < 3  # Reduced from 5 to 3

    if should_send_mqtt:
        async with RedisLock(
            tracker.redis, "send_mqtt", blocking_timeout=15, expire_timeout=20
        ):
            cleanup_pipeline = tracker.redis.pipeline()
            await tracker.cleanup(cleanup_pipeline)
            try:
                await cleanup_pipeline.execute()
                redis_commands.labels("execute").inc()
            except ResponseError as err:
                logger.error("Unable to execute cleanup pipeline", exc_info=err)

            await tracker.send_mqtt()
            try:
                await export_schema_key_counts(tracker.redis)
            except ResponseError as e:
                logger.error("Failed to export schema key counts", exc_info=e)


@retry(
    wait=wait_random_exponential(multiplier=1, min=1, max=60),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
)
def process_queue(queue: Queue[ScheduleEvent]) -> None:
    tracker = Tracker()
    thread_id = threading.current_thread().ident or 0
    base_sleep = 5 + (thread_id % 10)

    with Runner() as runner:
        while True:
            try:
                runner.run(execute(tracker, queue))
                sleep_time = base_sleep + random.randint(0, 25)
                time.sleep(sleep_time)
            except Exception as e:
                logger.error(
                    f"Error in process_queue thread {threading.current_thread().name}: {e}"
                )
                time.sleep(min(60, base_sleep * 2))


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
        for item in items:
            if isinstance(item, ScheduleEvent):
                route_ids.add(item.route_id)
            elif isinstance(item, VehicleRedisSchema):
                route_ids.add(item.route)

        # Create transaction ID with multiple routes if applicable
        route_context = "_".join(sorted(route_ids)) if route_ids else "batch"

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
                            vehicle_span_name = (
                                f"schedule_tracker.process_vehicle_{it.id}"
                            )
                            with tracer.start_as_current_span(
                                vehicle_span_name
                            ) as vehicle_span:
                                # Set vehicle-specific transaction ID for this span
                                set_vehicle_track_transaction_id(it.id)
                                add_transaction_ids_to_span(vehicle_span)
                                vehicle_span.set_attribute("vehicle.id", it.id)
                                vehicle_span.set_attribute("vehicle.route", it.route)
                                await tracker.process_queue_item(it, pipeline)
                        else:
                            # Process non-vehicle events normally
                            await tracker.process_queue_item(it, pipeline)

                    # Housekeeping: prune expired entries in the same round trip
                    pipeline.zremrangebyscore(
                        "time", "-inf", str(datetime.now().timestamp())
                    )
                    redis_commands.labels("zremrangebyscore").inc()

                    await pipeline.execute()
                    redis_commands.labels("execute").inc()

                    add_event_to_span(
                        span, "batch_flushed", {"items_processed": len(items)}
                    )
                except ResponseError as err:
                    set_span_error(span, err)
                    logger.error(
                        "Unable to communicate with Redis during batch flush",
                        exc_info=err,
                    )
                except (
                    AttributeError,
                    TypeError,
                    ValueError,
                    RuntimeError,
                    OSError,
                ) as err:
                    set_span_error(span, err)
                    logger.error("Unexpected error during batch flush", exc_info=err)
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
            except ResponseError as err:
                logger.error(
                    "Unable to communicate with Redis during batch flush", exc_info=err
                )
            except (
                AttributeError,
                TypeError,
                ValueError,
                RuntimeError,
                OSError,
            ) as err:
                logger.error("Unexpected error during batch flush", exc_info=err)

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
