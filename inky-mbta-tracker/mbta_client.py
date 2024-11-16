# client that keeps track of events on the stops specified
import logging
import os
import random
import time
from datetime import datetime
from queue import Queue

import httpx
import sseclient
from expiring_dict import ExpiringDict
from mbta_responses import (
    PredictionAttributes,
    PredictionResource,
    Route,
    RouteResource,
    ScheduleResource,
    Schedules,
    Stop,
    TripResource,
    Trips,
    TypeAndID,
)
from pydantic import TypeAdapter, ValidationError
from schedule_tracker import ScheduleEvent
from tenacity import (
    before_log,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    wait_exponential,
    wait_random_exponential,
)

auth_token = os.environ.get("AUTH_TOKEN")
mbta_v3 = "https://api-v3.mbta.com"
thirty_hours = 108000
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARN)


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
    routes: dict[str, RouteResource]
    stop: Stop
    schedule_only: bool

    def __init__(
        self,
        stop_id: str,
        route: str | None,
        direction: int | None,
        schedule_only=False,
    ):
        self.stop_id = stop_id
        self.route = route
        self.stop = self.save_stop()
        self.direction = direction
        self.trips = ExpiringDict(thirty_hours)
        self.routes = dict()
        logger.info(
            f"Init mbta_client for stop={stop_id}, route={route}, direction={direction}"
        )
        self.schedule_only = schedule_only

    @staticmethod
    def determine_time(attributes: PredictionAttributes) -> datetime | None:
        if attributes.arrival_time:
            return datetime.fromisoformat(attributes.arrival_time)
        elif attributes.departure_time:
            return datetime.fromisoformat(attributes.departure_time)
        else:
            return None

    def get_headsign(self, trip_id: str):
        if trip_id in self.trips:
            return self.trips[trip_id].attributes.headsign
        return ""

    def parse_schedule_response(
        self,
        data: str,
        event_type: str,
        queue: Queue[ScheduleEvent],
        transit_time_min: int,
    ):
        # https://www.mbta.com/developers/v3-api/streaming
        try:
            match event_type:
                case "reset":
                    ta = TypeAdapter(list[PredictionResource])
                    prediction = ta.validate_json(data, strict=False)
                    for item in prediction:
                        self.save_trip(item)
                        self.save_route(item)
                        self.queue_schedule_event(
                            item, "reset", queue, transit_time_min
                        )
                case "add":
                    prediction = PredictionResource.model_validate_json(
                        data, strict=False
                    )
                    self.save_trip(prediction)
                    self.save_route(prediction)
                    self.queue_schedule_event(
                        prediction, "add", queue, transit_time_min
                    )
                case "update":
                    prediction = PredictionResource.model_validate_json(
                        data, strict=False
                    )
                    self.queue_schedule_event(
                        prediction, "update", queue, transit_time_min
                    )
                case "remove":
                    schedule = TypeAndID.model_validate_json(data, strict=False)
                    # directly interact with the queue here to use a dummy object
                    event = ScheduleEvent(
                        action="remove",
                        headsign="nowhere",
                        route_id="Green Line A Branch",
                        route_type=1,
                        id=schedule.id,
                        stop="Boston 2",
                        time=datetime.now(),
                        transit_time_min=transit_time_min,
                    )
                    queue.put(event)
                    self.log_prediction(event)

        except ValidationError as err:
            logger.error("Unable to parse schedule", exc_info=err)
        except KeyError as err:
            logger.error("Could not find prediction", exc_info=err)

    @staticmethod
    def log_prediction(event: ScheduleEvent):
        logger.info(
            f"action={event.action} time={event.time.strftime("%c")} route_id={event.route_id} route_type={event.route_type} headsign={event.headsign} stop={event.stop} id={event.id}, transit_time_min={event.transit_time_min}"
        )

    def queue_schedule_event(
        self,
        item: PredictionResource | ScheduleResource,
        event_type: str,
        queue: Queue[ScheduleEvent],
        transit_time_min: int,
    ):
        schedule_time = self.determine_time(item.attributes)
        if not schedule_time:
            logger.warning(f"no time associated with event {item.id}, skipping")
            return
        self.save_route(item)
        route_id = item.relationships.route.data.id
        headsign = self.get_headsign(item.relationships.trip.data.id)
        route_type = self.routes[route_id].attributes.type

        event = ScheduleEvent(
            action=event_type,
            time=schedule_time,
            route_id=route_id,
            route_type=route_type,
            headsign=headsign,
            id=item.id,
            stop=self.stop.data.attributes.name,
            transit_time_min=transit_time_min,
        )
        queue.put(event)
        self.log_prediction(event)

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(httpx.RequestError),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    def save_trip(self, prediction: PredictionResource | ScheduleResource):
        trip_id = prediction.relationships.trip.data.id
        if trip_id and trip_id not in self.trips:
            try:
                response = httpx.get(
                    f"{mbta_v3}/trips?filter[id]={trip_id}&api_key={auth_token}"
                )

                trip = Trips.model_validate_json(response.text, strict=False)
                for tr in trip.data:
                    self.trips[trip_id] = tr

            except ValidationError as err:
                logger.error(f"Unable to parse trip, {err}")

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(httpx.RequestError),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    def save_route(self, prediction: PredictionResource | ScheduleResource):
        route_id = prediction.relationships.route.data.id
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

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(httpx.RequestError),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    def save_schedule(self, transit_time_min: int, queue: Queue[ScheduleEvent]):
        try:
            endpoint = f"{mbta_v3}/schedules?filter[stop]={self.stop_id}&sort=time&api_key={auth_token}&filter[date]={datetime.now().date().isoformat()}"
            if self.route != "":
                endpoint += f"&filter[route]={self.route}"
            if self.direction != "":
                endpoint += f"&filter[direction_id]={self.direction}"

            response = httpx.get(endpoint)
            schedules = Schedules.model_validate_json(
                strict=False, json_data=response.text
            )

            for item in schedules.data:
                self.save_trip(item)
                self.save_route(item)
                self.queue_schedule_event(item, "reset", queue, transit_time_min)
        except ValidationError as err:
            logger.error("Unable to parse schedule", exc_info=err)

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(httpx.RequestError),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    def save_stop(self):
        try:
            response = httpx.get(f"{mbta_v3}/stops/{self.stop_id}?api_key={auth_token}")
            logger.info("saved stop")
            return Stop.model_validate_json(response.text, strict=False)
        except ValidationError as err:
            logger.error("Unable to parse stop", exc_info=err)


@retry(
    wait=wait_random_exponential(multiplier=1),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
)
def watch_server_side_events(
    watcher: Watcher,
    endpoint: str,
    headers: dict[str, str],
    queue: Queue[ScheduleEvent],
    transit_time_min: int,
):
    response = with_httpx(endpoint, headers)
    client = sseclient.SSEClient(response)
    for event in client.events():
        watcher.parse_schedule_response(
            event.data, event.event, queue, transit_time_min
        )
        time.sleep(random.randint(1, 30))


def watch_static_schedule(
    stop_id: str,
    route: str | None,
    direction: int | None,
    queue: Queue[ScheduleEvent],
    transit_time_min: int,
):
    while True:
        watcher = Watcher(stop_id, route, direction, schedule_only=True)

        watcher.save_schedule(transit_time_min, queue)
        time.sleep(10800)  # 3 hours


def watch_station(
    stop_id: str,
    route: str | None,
    direction: int | None,
    queue: Queue[ScheduleEvent],
    transit_time_min: int,
):
    endpoint = f"{mbta_v3}/predictions?filter[stop]={stop_id}&api_key={auth_token}"
    if route != "":
        endpoint += f"&filter[route]={route}"
    if direction != "":
        endpoint += f"&filter[direction_id]={direction}"
    headers = {"accept": "text/event-stream"}
    watcher = Watcher(stop_id, route, direction)
    watch_server_side_events(watcher, endpoint, headers, queue, transit_time_min)
