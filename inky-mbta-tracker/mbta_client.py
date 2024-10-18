# client that keeps track of events on the stops specified
import json
import logging
import os
import re
import time
from datetime import datetime
from queue import Queue

import httpx
import sseclient
from expiring_dict import ExpiringDict
from tenacity import retry, wait_exponential, retry_if_exception_type, wait_random_exponential, before_log

from mbta_responses import (
    Route,
    RouteResource,
    ScheduleAttributes,
    ScheduleResource,
    TripResource,
    Trips,
    Stop,
    TypeAndID,
)
from pydantic import ValidationError, TypeAdapter
from schedule_tracker import ScheduleEvent

auth_token = os.environ.get("AUTH_TOKEN")
mbta_v3 = "https://api-v3.mbta.com"
thirty_hours = 108000
logger = logging.getLogger(__name__)
logging.getLogger('httpx').setLevel(logging.WARN)


def with_httpx(url, headers):
    """Get a streaming response for the given event feed using httpx."""
    import httpx

    with httpx.stream("GET", url, headers=headers, timeout=90) as s:
        # Note: 'yield from' is Python >= 3.3. Use for/yield instead if you
        # are using an earlier version.
        yield from s.iter_bytes()


class Watcher:
    stop_id: str
    route: str | None
    direction: int | None
    trips: ExpiringDict[str, TripResource]
    predictions: ExpiringDict[str, ScheduleResource]
    routes: dict[str, RouteResource]
    stop: Stop

    def __init__(self, stop_id: str, route: str | None, direction: int | None):
        self.stop_id = stop_id
        self.route = route
        self.direction = direction
        self.trips = ExpiringDict(thirty_hours)
        self.predictions = ExpiringDict(thirty_hours)
        self.routes = dict()

    @staticmethod
    def determine_time(attributes: ScheduleAttributes) -> datetime:
        if attributes.arrival_time:
            return datetime.fromisoformat(attributes.arrival_time)
        elif attributes.departure_time:
            return datetime.fromisoformat(attributes.departure_time)

    def get_headsign(self, trip_id: str):
        if trip_id in self.trips:
            return self.trips[trip_id].attributes.headsign
        return ""

    def parse_schedule_response(
        self, data: str, event_type: str, queue: Queue[ScheduleEvent]
    ):
        # https://www.mbta.com/developers/v3-api/streaming
        try:
            match event_type:
                case 'reset':
                    ta = TypeAdapter(list[ScheduleResource])
                    schedule = ta.validate_json(data, strict=False)
                    for item in schedule:
                        self.save_trip(item)
                        self.save_route(item)
                        if item.type == 'prediction':
                            self.predictions[item.id] = item
                        else:
                            self.queue_schedule_event(item, 'reset', queue)
                case 'add':
                    schedule = ScheduleResource.model_validate_json(data, strict=False)
                    self.save_trip(schedule)
                    self.save_route(schedule)
                    if schedule.type == 'prediction':
                        self.predictions[schedule.id] = schedule
                    else:
                        self.queue_schedule_event(schedule, 'add', queue)
                case 'update':
                    schedule = ScheduleResource.model_validate_json(data, strict=False)
                    if schedule.type == 'prediction':
                        self.predictions[schedule.id] = schedule
                    else:
                        self.queue_schedule_event(schedule, 'update', queue)
                case 'remove':
                    schedule = TypeAndID.model_validate_json(data, strict=False)
                    # directly interact with the queue here to use a dummy object
                    event = ScheduleEvent(action='remove', headsign='nowhere', prediction=False, route_id='Green Line A Branch', route_type=1, id=schedule.data.id, stop='Boston 2', time=datetime.now())
                    queue.put(event)

        except ValidationError as err:
            logger.error(f"Unable to parse schedule, {err}")
        except KeyError as err:
            logger.error(f"Could not find prediction, {err}")

    def queue_schedule_event(self, item: ScheduleResource, event_type: str, queue: Queue[ScheduleEvent]):
        schedule_time = self.determine_time(item.attributes)
        route_id = item.relationships.route.data.id
        headsign = self.get_headsign(item.relationships.trip.data.id)
        route_type = self.routes[route_id].attributes.type
        prediction = False

        if item.relationships.prediction.data is not None:
            prediction = self.predictions[item.relationships.prediction.data.id]
            schedule_time = self.determine_time(prediction.attributes)
            prediction = True
        event = ScheduleEvent(
            action=event_type,
            time=schedule_time,
            route_id=route_id,
            route_type=route_type,
            headsign=headsign,
            prediction=prediction,
            id=item.id,
            stop=self.stop.data.attributes.name
        )
        queue.put(
            event
        )
        logger.info(f"PUTing new schedule entry: {event}")

    @retry(wait=wait_exponential(multiplier=1, min=1, max=10), retry=retry_if_exception_type(httpx.RequestError))
    def save_trip(self, schedule: ScheduleResource):
        trip_id = schedule.relationships.trip.data.id
        if trip_id not in self.trips:
            try:
                response = httpx.get(
                    f"{mbta_v3}/trips?filter[id]={trip_id}&api_key={auth_token}"
                )

                trip = Trips.model_validate_json(response.text, strict=False)
                for tr in trip.data:
                    self.trips[trip_id] = tr

            except ValidationError as err:
                logger.error(f"Unable to parse trip, {err}")

    @retry(wait=wait_exponential(multiplier=1, min=1, max=10), retry=retry_if_exception_type(httpx.RequestError))
    def save_route(self, schedule: ScheduleResource):
        route_id = schedule.relationships.route.data.id
        if route_id not in self.routes:
            try:
                response = httpx.get(
                    f"{mbta_v3}/routes?filter[id]={route_id}&api_key={auth_token}"
                )

                route = Route.model_validate_json(response.text, strict=False)
                for rd in route.data:
                    self.routes[route_id] = rd
            except ValidationError as err:
                logger.error(f"Unable to parse route, {err}")

    @retry(wait=wait_exponential(multiplier=1, min=1, max=10), retry=retry_if_exception_type(httpx.RequestError))
    def save_stop(self):
        try:
            response = httpx.get(
                f"{mbta_v3}/stops/{self.stop_id}?api_key={auth_token}"
            )

            self.stop = Stop.model_validate_json(response.text, strict=False)
        except ValidationError as err:
            logger.error(f"Unable to parse stop, {err}")

@retry(wait=wait_random_exponential(multiplier=1, max=200), before=before_log(logger, logging.WARN))
def watch_server_side_events(watcher: Watcher, endpoint: str, headers: dict[str, str], queue: Queue[ScheduleEvent]):
    response = with_httpx(endpoint, headers)
    client = sseclient.SSEClient(response)
    for event in client.events():
        watcher.parse_schedule_response(event.data, event.event, queue)
        time.sleep(20)

def watch_station(
    stop_id: str, route: str | None, direction: int | None, queue: Queue[ScheduleEvent]
):
    now = datetime.now()
    endpoint = f"{mbta_v3}/schedules?include=prediction&filter[min_time]={now.strftime("%H:%M")}&filter[stop]={stop_id}&sort=time&api_key={auth_token}"
    if route != "":
        endpoint += f"&filter[route]={route}"
    if direction != "":
        endpoint += f"&filter[direction_id]={direction}"
    headers = {"accept": "text/event-stream"}
    watcher = Watcher(stop_id, route, direction)
    watcher.save_stop()
    watch_server_side_events(watcher, endpoint, headers, queue)
