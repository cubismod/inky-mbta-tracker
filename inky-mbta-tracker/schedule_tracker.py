import logging
import os
import time
from asyncio import QueueEmpty, Runner
from datetime import UTC, datetime, timedelta
from os import environ
from queue import Queue
from typing import Optional

import humanize
from paho.mqtt import MQTTException, publish
from prometheus import schedule_events
from pydantic import BaseModel, ValidationError
from redis import ResponseError
from redis.asyncio.client import Pipeline, Redis
from tenacity import (
    before_sleep_log,
    retry,
    wait_exponential,
)
from zoneinfo import ZoneInfo

logger = logging.getLogger("schedule_tracker")


class ScheduleEvent(BaseModel):
    action: str
    time: datetime
    route_id: str
    route_type: int
    headsign: str
    stop: str
    id: str
    transit_time_min: int
    trip_id: Optional[str] = None
    alerting: bool = False
    bikes_allowed: bool = False


def dummy_schedule_event(event_id: str):
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

    def __init__(self):
        r = Redis(
            host=environ.get("IMT_REDIS_ENDPOINT"),
            port=os.environ.get("IMT_REDIS_PORT", "6379"),
            password=os.environ.get("IMT_REDIS_PASSWORD"),
        )
        self.redis = r

    @staticmethod
    def str_timestamp(event: ScheduleEvent):
        return str(event.time.timestamp())

    @staticmethod
    def calculate_time_diff(event: ScheduleEvent):
        res = event.time - datetime.now().astimezone(UTC)
        if res < timedelta(seconds=5):
            return timedelta(minutes=5)
        return res

    @staticmethod
    def log_prediction(event: ScheduleEvent):
        logger.info(
            f"action={event.action} time={event.time.astimezone(ZoneInfo("US/Eastern")).strftime("%c")} route_id={event.route_id} route_type={event.route_type} headsign={event.headsign} stop={event.stop} id={event.id}, transit_time_min={event.transit_time_min}, alerting={event.alerting}, bikes_allowed={event.bikes_allowed}"
        )

    async def cleanup(self, pipeline: Pipeline):
        try:
            obsolete_ids = await self.redis.zrange(
                "time",
                start=0,
                end=int(datetime.now().astimezone(UTC).timestamp()),
                withscores=False,
                byscore=True,
            )
            for item in obsolete_ids:
                dec_i = item.decode("utf-8")
                if dec_i:
                    await self.rm(dummy_schedule_event(dec_i), pipeline)
        except ResponseError as err:
            logger.error("unable to cleanup old entries", exc_info=err)

    async def add(self, event: ScheduleEvent, pipeline: Pipeline, action: str):
        # only add events in the future
        if event.time > datetime.now().astimezone(UTC):
            trip_redis_key = f"trip-{event.trip_id}-{event.stop.replace(" ", "_")}"
            existing_event = await self.redis.get(trip_redis_key)
            if existing_event:
                dec_ee = existing_event.decode("utf-8")
                if dec_ee != event.id and dec_ee.startswith("schedule"):
                    logger.info(
                        f"Removing existing schedule entry with id {dec_ee} as it has been replaced with {event.id}, trip_id={event.trip_id}"
                    )
                    await self.rm(dummy_schedule_event(existing_event), pipeline)
                if dec_ee.startswith("prediction") and event.id.startswith("schedule"):
                    # don't override realtime predictions
                    return
            await pipeline.set(
                trip_redis_key,
                event.id,
                ex=(event.time - datetime.now().astimezone(UTC)) + timedelta(hours=1),
            )

            await pipeline.set(
                event.id,
                event.model_dump_json(exclude={"trip_id"}),
                ex=self.calculate_time_diff(event) + timedelta(minutes=1),
            )
            await pipeline.zadd("time", {event.id: int(event.time.timestamp())})
            schedule_events.labels(action, event.route_id, event.stop).inc()
            self.log_prediction(event)

    async def rm(self, event: ScheduleEvent, pipeline: Pipeline):
        try:
            # fetch existing event metadata so we can keep tabs in prometheus
            dec_ee = await self.redis.get(event.id)
            if dec_ee:
                event = ScheduleEvent.model_validate_json(
                    dec_ee.decode("utf-8"), strict=False
                )
            await pipeline.delete(event.id)
            await pipeline.zrem("time", int(event.time.timestamp()))
            schedule_events.labels("remove", event.route_id, event.stop).inc()
            self.log_prediction(event)
        except ResponseError as err:
            logger.error("unable to get key from redis", exc_info=err)
        except ValidationError as err:
            logger.error("unable to validate ScheduleEvent model", exc_info=err)

    @staticmethod
    def __determine_color(event: ScheduleEvent):
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

    async def fetch_mqtt_events(self):
        ret: list[ScheduleEvent] = list()
        try:
            now = datetime.now().astimezone(UTC)
            max_time = now + timedelta(hours=12)
            events = await self.redis.zrange(
                "time",
                start=int(now.timestamp()),
                end=int(max_time.timestamp()),
                byscore=True,
                withscores=False,
                num=26,
                offset=0,
            )
            for event in events:
                res = await self.redis.get(event.decode("utf-8"))
                if res:
                    v_event = ScheduleEvent.model_validate_json(
                        res.decode("utf-8"), strict=False
                    )
                    ret.append(v_event)
        except ResponseError as err:
            logger.error("unable to run redis command", exc_info=err)
        except ValidationError as err:
            logger.error("unable to validate schema", exc_info=err)

        return ret

    @staticmethod
    def get_route_icon(event: ScheduleEvent):
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

    async def send_mqtt(self):
        if os.getenv("IMT_ENABLE_MQTT", "true") == "true":
            msgs = list()
            events = await self.fetch_mqtt_events()
            for i, event in enumerate(events):
                topic = f"imt/departure_time{i}"
                payload = self.prediction_display(event)
                msgs.append({"topic": topic, "payload": payload})

                topic = f"imt/destination_and_stop{i}"
                payload = f"{self.get_route_icon(event)} [{event.route_id}] {event.headsign}: {event.stop}"
                if event.id.startswith("prediction"):
                    payload += " 📶"
                if event.alerting:
                    payload += " ⚠️"
                if event.bikes_allowed:
                    payload += " 🚲"
                msgs.append({"topic": topic, "payload": payload})
            if len(msgs) > 0:
                try:
                    publish.multiple(
                        msgs,
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
    def prediction_display(event: ScheduleEvent):
        prediction_indicator = ""

        rounded_time = round(
            (event.time.timestamp() - datetime.now().astimezone(UTC).timestamp())
        )
        if rounded_time > 0:
            return f"🕒 {humanize.naturaldelta(timedelta(seconds=rounded_time))} · 🚲/🚶 {event.transit_time_min} min"
        if rounded_time == 0:
            return f"{prediction_indicator} BRD"
        if rounded_time < 0:
            return f"{prediction_indicator} DEP"

    async def process_schedule_event(self, event: ScheduleEvent, pipeline: Pipeline):
        match event.action:
            case "reset":
                await self.add(event, pipeline, "reset")
            case "add":
                await self.add(event, pipeline, "add")
            case "update":
                await self.add(event, pipeline, "update")
            case "remove":
                await self.rm(event, pipeline)


async def execute(tracker: Tracker, queue: Queue[ScheduleEvent]):
    pipeline = tracker.redis.pipeline()
    while queue.qsize() != 0:
        try:
            schedule_event = queue.get()
            await tracker.process_schedule_event(schedule_event, pipeline)
        except QueueEmpty:
            break
    await tracker.cleanup(pipeline)
    try:
        await pipeline.execute()
        await tracker.redis.zremrangebyscore(
            "time", "-inf", str(datetime.now().timestamp())
        )
    except ResponseError as err:
        logger.error("Unable to communicate with Redis", exc_info=err)
    await tracker.send_mqtt()


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
)
def process_queue(queue: Queue[ScheduleEvent]):
    tracker = Tracker()
    with Runner() as runner:
        while True:
            runner.run(execute(tracker, queue))
            time.sleep(10)
