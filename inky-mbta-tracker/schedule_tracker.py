import contextlib
import logging
import os
import time
from asyncio import QueueEmpty
from datetime import datetime, timedelta
from os import environ
from queue import Queue

import humanize
import redis
from paho.mqtt import MQTTException, publish
from prometheus import api_events
from pydantic import BaseModel
from redis import Redis, ResponseError
from redis.client import Pipeline
from rich.console import Console
from rich.live import Live
from rich.style import Style
from rich.table import Table
from sortedcontainers import SortedDict

logger = logging.getLogger("schedule_tracker")


# actions:
# add
# update
# remove
class ScheduleEvent(BaseModel):
    action: str
    time: datetime
    route_id: str
    route_type: int
    headsign: str
    stop: str
    id: str
    transit_time_min: int


class Tracker:
    all_events: SortedDict[str, ScheduleEvent]
    redis: Redis

    def __init__(self):
        self.all_events = SortedDict()
        r = redis.Redis(
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
        ts = event.time.timestamp()
        diff = ts - datetime.now().timestamp()
        if diff > 0:
            return round(diff)
        else:
            return 200

    def find_timestamp(self, prediction_id: str):
        for _, item in self.all_events.items():
            if item.id == prediction_id:
                return self.str_timestamp(item)
        return None

    def cleanup(self, pipeline: Pipeline):
        for _, v in self.all_events.items():
            if v.time.timestamp() < datetime.now().timestamp():
                self.rm(v, pipeline)
            else:
                break

    def add(self, event: ScheduleEvent, pipeline: Pipeline, action: str):
        # only add events in the future
        if event.time.timestamp() > datetime.now().timestamp():
            self.all_events[self.str_timestamp(event)] = event
            pipeline.set(
                event.id, event.model_dump_json(), ex=self.calculate_time_diff(event)
            )
            pipeline.zadd("time", {event.id: self.str_timestamp(event)})
            api_events.labels(action, event.route_id, event.stop).inc()

    def update(self, event: ScheduleEvent, pipeline: Pipeline):
        existing_timestamp = self.find_timestamp(event.id)
        if existing_timestamp and existing_timestamp != str(event.time.timestamp()):
            # remove old predictions
            self.all_events.pop(existing_timestamp)
        self.add(event, pipeline, "update")

    def rm(self, event: ScheduleEvent, pipeline: Pipeline):
        timestamp = self.find_timestamp(event.id)
        if timestamp:
            with contextlib.suppress(KeyError):
                self.all_events.pop(timestamp)
            pipeline.zrem("time", self.str_timestamp(event))
            api_events.labels("remove", event.route_id, event.stop).inc()

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

    def send_mqtt(self):
        if os.getenv("IMT_ENABLE_MQTT", "true") == "true":
            msgs = list()
            for i, event in enumerate(self.all_events.items()):
                if len(msgs) > 12:
                    break
                topic = f"imt/departure_time{i}"
                payload = self.prediction_display(event[1])
                msgs.append({"topic": topic, "payload": payload})

                topic = f"imt/destination_and_stop{i}"
                payload = f"|{event[1].route_id}| to: {event[1].headsign}, from: {event[1].stop}"
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

        rounded_time = round((event.time.timestamp() - datetime.now().timestamp()))
        if rounded_time > 0:
            return humanize.naturaldelta(timedelta(seconds=rounded_time))
        if rounded_time == 0:
            return f"{prediction_indicator} BRD"
        if rounded_time < 0:
            return f"{prediction_indicator} DEP"

    def generate_table(self):
        table = Table(title="Departures")
        table.add_column("Stop")
        table.add_column("Route")
        table.add_column("Headsign")
        table.add_column("Departure Min", justify="center")
        table.add_column("Departure Time", justify="center")

        table.field_names = [
            "Stop",
            "Route",
            "Headsign",
            "Departure Min",
            "Departure Time",
        ]
        for _, event in self.all_events.items():
            table.add_row(
                event.stop,
                event.route_id,
                event.headsign,
                self.prediction_display(event),
                event.time.strftime("%X"),
                style=Style(color=self.__determine_color(event), bgcolor="white"),
            )
            if len(table.rows) > int(os.getenv("IMT_ROWS", 15)):
                break

        return table

    def process_schedule_event(self, event: ScheduleEvent, pipeline: Pipeline):
        match event.action:
            case "reset":
                self.add(event, pipeline, "reset")
            case "add":
                self.add(event, pipeline, "add")
            case "update":
                self.update(event, pipeline)
            case "remove":
                # get the actual event based on the ID here
                full_event = self.all_events.get(self.find_timestamp(event.id))
                self.rm(full_event, pipeline)


def run(tracker: Tracker, queue: Queue[ScheduleEvent]):
    time.sleep(7)
    pipeline = tracker.redis.pipeline()
    while queue.qsize() != 0:
        try:
            schedule_event = queue.get()
            tracker.process_schedule_event(schedule_event, pipeline)
        except QueueEmpty:
            break
    tracker.cleanup(pipeline)
    try:
        pipeline.execute()
    except ResponseError as err:
        logger.error("Unable to communicate with Redis", exc_info=err)
    tracker.send_mqtt()


def process_queue(queue: Queue[ScheduleEvent]):
    tracker = Tracker()
    if os.environ.get("IMT_CONSOLE", "false") == "true":
        console = Console()
        with Live(
            console=console,
            renderable=tracker.generate_table(),
            refresh_per_second=1,
            screen=True,
            transient=True,
        ) as live:
            while True:
                run(tracker, queue)
                live.update(tracker.generate_table())
    else:
        while True:
            run(tracker, queue)
