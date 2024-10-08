import logging
from datetime import datetime

from pydantic import BaseModel


class ScheduleEvent(BaseModel):
    action: str
    time: datetime
    route_id: str
    headsign: str


class Tracker:
    all_events: dict[str, ScheduleEvent]
    DisplayedEvents: list[ScheduleEvent]

    def rm(self, schedule_id: str):
        try:
            self.all_events.pop(schedule_id)
        except KeyError as err:
            logging.error(err)
