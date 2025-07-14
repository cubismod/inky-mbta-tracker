import logging
import os
import random
import time
from asyncio import QueueEmpty, Runner
from datetime import UTC, datetime, timedelta
from queue import Queue
from typing import Optional
from zoneinfo import ZoneInfo

import humanize
from geojson import Feature, Point
from paho.mqtt import MQTTException, publish
from prometheus import redis_commands, schedule_events, vehicle_events, vehicle_speeds
from pydantic import ValidationError
from redis import ResponseError
from redis.asyncio.client import Pipeline, Redis
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
                    if distance > 0 and duration.seconds > 0:
                        meters_per_second = distance / duration.seconds
                        speed = meters_per_second * 2.2369362921

                        if not self.is_speed_reasonable(speed, event.route):
                            logger.info(
                                f"lol lmao imagine a {event.route} train/bus going {speed} mph"
                            )
                            # throw out insane predictions
                            return None, False
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
            obsolete_ids = await self.redis.zrange(
                "time",
                start=0,
                end=int(datetime.now().astimezone(UTC).timestamp()),
                withscores=False,
                byscore=True,
            )
            redis_commands.labels("zrange").inc()
            for item in obsolete_ids:
                dec_i = item.decode("utf-8")
                if dec_i:
                    await self.rm(dummy_schedule_event(dec_i), pipeline)
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
                    publish.multiple(
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
    while queue.qsize() != 0:
        try:
            item = queue.get()
            await tracker.process_queue_item(item, pipeline)
        except QueueEmpty:
            break
    await tracker.cleanup(pipeline)
    try:
        await pipeline.execute()
        await tracker.redis.zremrangebyscore(
            "time", "-inf", str(datetime.now().timestamp())
        )
        redis_commands.labels("zremrangebyscore").inc()
    except ResponseError as err:
        logger.error("Unable to communicate with Redis", exc_info=err)
    await tracker.send_mqtt()


@retry(
    wait=wait_random_exponential(multiplier=1, min=1),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
)
def process_queue(queue: Queue[ScheduleEvent]) -> None:
    tracker = Tracker()
    with Runner() as runner:
        while True:
            runner.run(execute(tracker, queue))
            time.sleep(random.randint(10, 20))
