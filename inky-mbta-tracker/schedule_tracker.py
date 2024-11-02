import contextlib
import logging
import os
import threading
import time
from asyncio import QueueEmpty
from datetime import datetime, timedelta
from queue import Queue

from pydantic import BaseModel
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
# quit
class ScheduleEvent(BaseModel):
    action: str
    time: datetime
    route_id: str
    route_type: int
    headsign: str
    stop: str
    id: str


class Tracker:
    all_events: SortedDict[str, ScheduleEvent]

    def __init__(self):
        self.all_events = SortedDict()

    @staticmethod
    def __calculate_timestamp(event: ScheduleEvent):
        return str(event.time.timestamp())

    def __find_timestamp(self, prediction_id: str):
        for _, item in self.all_events.items():
            if item.id == prediction_id:
                return self.__calculate_timestamp(item)
        return None

    def __add(self, event: ScheduleEvent):
        self.all_events[self.__calculate_timestamp(event)] = event

    def __update(self, event: ScheduleEvent):
        existing_timestamp = self.__find_timestamp(event.id)
        if existing_timestamp and existing_timestamp != str(event.time.timestamp()):
            # remove old predictions
            self.all_events.pop(existing_timestamp)
        self.__add(event)

    def __rm(self, event: ScheduleEvent):
        timestamp = self.__find_timestamp(event.id)
        if timestamp:
            with contextlib.suppress(KeyError):
                self.all_events.pop(timestamp)

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

    @staticmethod
    def prediction_display(event: ScheduleEvent):
        prediction_indicator = ""

        rounded_time = round((event.time.timestamp() - datetime.now().timestamp()) / 60)
        if rounded_time > 0:
            return f"{prediction_indicator} {rounded_time} min"
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

    def prune_entries(self):
        for event in self.all_events.items():
            if float(event[0]) < (datetime.now() - timedelta(seconds=30)).timestamp():
                self.__rm(event[1])
                continue
            else:
                break

    def process_schedule_event(self, event: ScheduleEvent):
        match event.action:
            case "reset":
                self.__add(event)
            case "add":
                self.__add(event)
            case "update":
                self.__update(event)
            case "remove":
                self.__rm(event)


def process_queue(queue: Queue[ScheduleEvent], exit_event: threading.Event):
    tracker = Tracker()
    console = Console()
    with Live(
        console=console,
        renderable=tracker.generate_table(),
        refresh_per_second=1,
        screen=True,
        transient=True,
    ) as live:
        while True:
            time.sleep(15)
            while queue.qsize() != 0:
                try:
                    schedule_event = queue.get()
                    tracker.process_schedule_event(schedule_event)
                except QueueEmpty:
                    break
            tracker.prune_entries()
            live.update(tracker.generate_table())
            if exit_event.is_set():
                logger.info("bye bye")
                live.stop()
                break