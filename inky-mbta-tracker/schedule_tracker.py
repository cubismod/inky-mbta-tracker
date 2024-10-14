import contextlib
import logging
import time
from asyncio import QueueEmpty
from datetime import datetime, timedelta, tzinfo
from queue import Queue

from prettytable import PrettyTable
from pydantic import BaseModel
from sortedcontainers import SortedDict

logger = logging.getLogger('schedule_tracker')

class ScheduleEvent(BaseModel):
    action: str
    time: datetime
    route_id: str
    route_type: int
    headsign: str
    prediction: bool
    stop: str
    id: str


class Tracker:
    all_events: SortedDict[str, ScheduleEvent]
    displayed_events: list[ScheduleEvent]

    def __init__(self):
        self.all_events = SortedDict()
        self.displayed_events = list()

    @staticmethod
    def __calculate_timestamp(event: ScheduleEvent):
        return str(event.time.timestamp())

    def __find_timestamp(self, schedule_id: str):
        for item in self.all_events.items():
            if item[1].id == schedule_id:
                return self.__calculate_timestamp(item[1])
        return None

    def __add(self, event: ScheduleEvent):
        self.all_events[self.__calculate_timestamp(event)] = event

    def __update(self, event: ScheduleEvent):
        timestamp = self.__find_timestamp(event.id)
        if timestamp:
            self.all_events[timestamp] = event
        else:
            self.__add(event)

    def __rm(self, event: ScheduleEvent):
        timestamp = self.__find_timestamp(event.id)
        if timestamp:
            with contextlib.suppress(KeyError):
                self.all_events.pop(timestamp)

    @staticmethod
    def prediction_display(event: ScheduleEvent):
        prediction_indicator = ''
        if event.prediction:
            prediction_indicator = '📶'

        rounded_time = round((event.time.timestamp() - datetime.now().timestamp()) / 60)
        if rounded_time > 0:
            return f"{prediction_indicator}{rounded_time} min"
        if rounded_time <= 0:
            return f"{prediction_indicator}BRD"


    def display_cli(self):
        table = PrettyTable()
        table.field_names = ["Stop", "Route", "Headsign", "Departure Min", "Departure Time"]
        for event in self.displayed_events:
            table.add_row([event.stop, event.route_id, event.headsign, self.prediction_display(event), event.time.strftime("%X")])

        print(table)

    def generate_display(self):
        self.displayed_events.clear()
        for event in self.all_events.items():
            if float(event[0]) < (datetime.now() - timedelta(minutes=2)).timestamp():
                self.__rm(event[1])
                continue
            if len(self.displayed_events) > 8:
                break
            self.displayed_events.append(event[1])

        self.display_cli()

    def process_schedule_event(self, event: ScheduleEvent):
        match event.action:
            case 'reset':
                self.__add(event)
            case 'update':
                self.__update(event)
            case 'remove':
                self.__rm(event)

def process_queue(queue: Queue[ScheduleEvent]):
    tracker = Tracker()
    while True:
        logger.info("waiting on queue")
        time.sleep(20)
        while queue.qsize() != 0:
            try:
                schedule_event = queue.get()
                tracker.process_schedule_event(schedule_event)
            except QueueEmpty:
                break
        tracker.generate_display()

