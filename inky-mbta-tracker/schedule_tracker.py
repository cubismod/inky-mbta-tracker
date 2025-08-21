import asyncio
import logging
import os
import random
import threading
import time
from asyncio import QueueEmpty, Runner
from datetime import UTC, datetime, timedelta
from queue import Queue
from typing import Optional, Union
from zoneinfo import ZoneInfo

import humanize
from geojson import Feature, Point
from paho.mqtt import MQTTException, publish
from prometheus import (
    coalesced_events_dropped,
    queue_backpressure_ratio,
    queue_processed_item,
    queue_size,
    redis_commands,
    schedule_events,
    vehicle_events,
    vehicle_speeds,
)
from pydantic import ValidationError
from redis import ResponseError
from redis.asyncio.client import Pipeline, Redis
from redis_lock.asyncio import RedisLock
from shared_types.schema_versioner import export_schema_key_counts
from shared_types.shared_types import ScheduleEvent, VehicleRedisSchema
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
            if event.current_status != "STOPPED_AT" and not event.speed:
                last_event = await self.redis.get(f"vehicle:{event.id}")
                redis_commands.labels("get").inc()

                if last_event:
                    last_event_validated = VehicleRedisSchema.model_validate_json(
                        last_event, strict=False
                    )

                    start = Feature(
                        geometry=Point(
                            (
                                last_event_validated.longitude,
                                last_event_validated.latitude,
                            )
                        )
                    )
                    end = Feature(geometry=Point((event.longitude, event.latitude)))

                    distance = measurement.distance(start, end, "m")
                    duration = event.update_time - last_event_validated.update_time
                    # Minimum thresholds: 10m distance, 5s time to filter GPS noise and rapid updates
                    if distance > 10 and duration.seconds >= 5:
                        meters_per_second = distance / duration.seconds
                        speed = meters_per_second * 2.2369362921

                        # Outlier detection: check for unreasonable acceleration (> 10 mph/s)
                        if (
                            last_event_validated.speed
                            and not last_event_validated.approximate_speed
                        ):
                            speed_diff = abs(speed - last_event_validated.speed)
                            max_accel_change = (
                                10 * duration.seconds
                            )  # 10 mph per second max
                            if speed_diff > max_accel_change:
                                logger.debug(
                                    f"Rejecting speed calculation for {event.route} vehicle {event.id}: "
                                    f"acceleration too high ({speed_diff:.1f} mph change in {duration.seconds}s)"
                                )
                                return (
                                    last_event_validated.speed,
                                    last_event_validated.approximate_speed,
                                )

                        if not self.is_speed_reasonable(speed, event.route):
                            logger.debug(
                                f"Rejecting speed calculation for {event.route} vehicle {event.id}: speed {speed} mph is unreasonable"
                            )
                            # throw out insane predictions
                            return (
                                last_event_validated.speed,
                                last_event_validated.approximate_speed,
                            )
                        else:
                            return round(speed, 2), True

                    else:
                        return (
                            last_event_validated.speed,
                            last_event_validated.approximate_speed,
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
                    # Offload blocking MQTT publish to a thread to avoid blocking the event loop
                    await asyncio.to_thread(
                        publish.multiple,
                        msgs,  # type: ignore
                        hostname=os.getenv("IMT_MQTT_HOST", ""),
                        port=int(os.getenv("IMT_MQTT_PORT", "1883")),
                        auth={
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
    queue_size.set(queue.qsize())
    while queue.qsize() != 0:
        try:
            item = queue.get()
            await tracker.process_queue_item(item, pipeline)
            if isinstance(item, ScheduleEvent):
                queue_processed_item.labels("schedule").inc()
            if isinstance(item, VehicleRedisSchema):
                queue_processed_item.labels("vehicle").inc()
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
                key_counts = await export_schema_key_counts(tracker.redis)
                logger.debug(f"Schema key counts: {key_counts}")
            except ResponseError as e:
                logger.error("Failed to export schema key counts", exc_info=e)


@retry(
    wait=wait_random_exponential(multiplier=1, min=1),
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
    queue: "asyncio.Queue[Union[ScheduleEvent, VehicleRedisSchema]]",
) -> None:
    """Async consumer that batches events into Redis pipelines.

    Processes up to a batch size or times out after a short interval
    to keep latency low while maximizing pipeline efficiency.
    """
    tracker = Tracker()
    BASE_BATCH = int(os.getenv("IMT_PIPELINE_BATCH", "200"))
    BASE_TIMEOUT_SEC = float(os.getenv("IMT_PIPELINE_TIMEOUT_SEC", "0.2"))
    # Backpressure logging configuration
    HIGH_RATIO = float(os.getenv("IMT_QUEUE_BACKPRESSURE_RATIO", "0.8"))
    SUSTAIN_SEC = float(os.getenv("IMT_QUEUE_BACKPRESSURE_SUSTAIN_SEC", "5"))
    RELOG_SEC = float(os.getenv("IMT_QUEUE_BACKPRESSURE_RELOG_SEC", "60"))
    high_since: float | None = None
    last_log: float = 0.0

    async def flush(pipeline: Pipeline) -> None:
        try:
            await pipeline.execute()
            redis_commands.labels("execute").inc()
            await tracker.redis.zremrangebyscore(
                "time", "-inf", str(datetime.now().timestamp())
            )
            redis_commands.labels("zremrangebyscore").inc()
        except ResponseError as err:
            logger.error("Unable to communicate with Redis", exc_info=err)

    try:
        while True:
            # Adaptive batch and timeout based on queue depth
            qdepth = queue.qsize()
            qmax = getattr(queue, "maxsize", 0) or 5000
            depth_ratio = min(1.0, max(0.0, qdepth / qmax))
            queue_backpressure_ratio.set(depth_ratio)
            batch_limit = int(BASE_BATCH * (1 + 4 * depth_ratio))  # up to 5x
            flush_timeout = max(0.02, BASE_TIMEOUT_SEC * (1 - 0.8 * depth_ratio))

            pipeline = tracker.redis.pipeline(transaction=False)
            processed = 0
            queue_size.set(qdepth)

            # Backpressure detection and rate-limited logging
            now = time.monotonic()
            qmax_int = int(qmax)
            if qdepth >= int(qmax_int * HIGH_RATIO):
                if high_since is None:
                    high_since = now
                if (now - high_since) >= SUSTAIN_SEC and (now - last_log) >= RELOG_SEC:
                    pct = (qdepth / qmax_int) * 100 if qmax_int else 0
                    logger.warning(
                        f"Queue backpressure: depth={qdepth}/{qmax_int} ({pct:.1f}%). "
                        f"Consumers may be saturated; consider increasing IMT_PROCESS_QUEUE_THREADS or tuning batching."
                    )
                    last_log = now
            else:
                high_since = None

            # Ensure at least one item or timeout
            # Coalescing buffers: by event id, and by trip key for schedules
            sched_buf: dict[str, ScheduleEvent] = {}
            sched_trip_buf: dict[tuple[str, str], ScheduleEvent] = {}
            veh_buf: dict[str, VehicleRedisSchema] = {}

            try:
                item = await asyncio.wait_for(queue.get(), timeout=flush_timeout)
                if isinstance(item, ScheduleEvent):
                    queue_processed_item.labels("schedule").inc()
                    # Only coalesce by trip when we have sufficient context
                    if item.trip_id and item.stop:
                        key = (item.trip_id, item.stop)
                        if key in sched_trip_buf:
                            coalesced_events_dropped.labels(
                                item_type="schedule", coalesce_by="trip"
                            ).inc()
                        sched_trip_buf[key] = item
                    else:
                        if item.id in sched_buf:
                            coalesced_events_dropped.labels(
                                item_type="schedule", coalesce_by="id"
                            ).inc()
                        sched_buf[item.id] = item
                else:
                    queue_processed_item.labels("vehicle").inc()
                    if item.id in veh_buf:
                        coalesced_events_dropped.labels(
                            item_type="vehicle", coalesce_by="id"
                        ).inc()
                    veh_buf[item.id] = item
                processed += 1
                queue.task_done()
            except asyncio.TimeoutError:
                # No items ready; small pause to yield
                await asyncio.sleep(0.01)
            except QueueEmpty:
                pass

            # Drain without blocking up to the adaptive limit
            while processed < batch_limit:
                try:
                    item2 = queue.get_nowait()
                except QueueEmpty:
                    break
                if isinstance(item2, ScheduleEvent):
                    queue_processed_item.labels("schedule").inc()
                    if item2.trip_id and item2.stop:
                        key2 = (item2.trip_id, item2.stop)
                        if key2 in sched_trip_buf:
                            coalesced_events_dropped.labels(
                                item_type="schedule", coalesce_by="trip"
                            ).inc()
                        sched_trip_buf[key2] = item2
                    else:
                        if item2.id in sched_buf:
                            coalesced_events_dropped.labels(
                                item_type="schedule", coalesce_by="id"
                            ).inc()
                        sched_buf[item2.id] = item2
                else:
                    queue_processed_item.labels("vehicle").inc()
                    if item2.id in veh_buf:
                        coalesced_events_dropped.labels(
                            item_type="vehicle", coalesce_by="id"
                        ).inc()
                    veh_buf[item2.id] = item2
                processed += 1
                queue.task_done()

                # Cooperative yield every 100 items to avoid starving producers
                if (processed % 100) == 0:
                    await asyncio.sleep(0)

            if processed == 0:
                # Nothing batched; continue loop
                continue

            # Apply coalesced items to the pipeline then flush
            # First, schedule events that didn't have trip info (or removals)
            for s_ev in sched_buf.values():
                await tracker.process_queue_item(s_ev, pipeline)
            # Then, the last-seen event per (trip_id, stop) key
            for st_ev in sched_trip_buf.values():
                await tracker.process_queue_item(st_ev, pipeline)
            for v_ev in veh_buf.values():
                await tracker.process_queue_item(v_ev, pipeline)

            # Flush pipeline and perform maintenance
            await flush(pipeline)

            # Occasionally: distributed cleanup + MQTT publish
            if random.randint(0, 10) < 3:
                async with RedisLock(
                    tracker.redis, "send_mqtt", blocking_timeout=15, expire_timeout=20
                ):
                    cleanup_pipeline = tracker.redis.pipeline(transaction=False)
                    await tracker.cleanup(cleanup_pipeline)
                    try:
                        await cleanup_pipeline.execute()
                        redis_commands.labels("execute").inc()
                    except ResponseError as err:
                        logger.error("Unable to execute cleanup pipeline", exc_info=err)

                    await tracker.send_mqtt()
                    try:
                        key_counts = await export_schema_key_counts(tracker.redis)
                        logger.debug(f"Schema key counts: {key_counts}")
                    except ResponseError as e:
                        logger.error("Failed to export schema key counts", exc_info=e)
    except asyncio.CancelledError:
        # Best-effort final flush: process any currently coalesced items if present
        try:
            # Attempt a small drain without blocking to clear immediate backlog
            pipeline = tracker.redis.pipeline(transaction=False)
            drained = 0
            while drained < 1000:  # cap final drain to avoid long shutdowns
                try:
                    item = queue.get_nowait()
                except QueueEmpty:
                    break
                if isinstance(item, ScheduleEvent):
                    await tracker.process_queue_item(item, pipeline)
                else:
                    await tracker.process_queue_item(item, pipeline)
                drained += 1
                queue.task_done()
            if drained > 0:
                await pipeline.execute()
                redis_commands.labels("execute").inc()
        finally:
            raise
