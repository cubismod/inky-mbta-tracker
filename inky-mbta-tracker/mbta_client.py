# client that keeps track of events on the stops specified
import logging
import os
import random
from asyncio import CancelledError, Runner, sleep
from datetime import UTC, datetime, timedelta
from enum import Enum
from queue import Queue
from typing import Optional

import aiohttp
from aiohttp import ClientSession
from aiosseclient import aiosseclient
from expiring_dict import ExpiringDict
from mbta_responses import (
    Alerts,
    Facilities,
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
    Vehicle,
)
from prometheus import mbta_api_requests, tracker_executions
from pydantic import TypeAdapter, ValidationError
from schedule_tracker import ScheduleEvent, VehicleRedisSchema, dummy_schedule_event
from tenacity import (
    before_log,
    before_sleep_log,
    retry,
    retry_if_not_exception_type,
    wait_exponential,
    wait_random_exponential,
)
from zoneinfo import ZoneInfo

auth_token = os.environ.get("AUTH_TOKEN")
mbta_v3 = "https://api-v3.mbta.com"
logger = logging.getLogger(__name__)


class EventType(Enum):
    OTHER = -1
    PREDICTIONS = 0
    SCHEDULES = 1
    VEHICLES = 2


class Watcher:
    # by default is predictions, can also be vehicles for a live
    # vehicle watcher
    watcher_type: EventType
    stop_id: Optional[str]
    route: Optional[str]
    direction: Optional[str]
    trips: ExpiringDict[str, TripResource]
    # key is trip, value is if there is an active alert
    alerts: ExpiringDict[str, bool]
    routes: dict[str, RouteResource]
    stop: Optional[Stop]
    schedule_only: bool
    facilities: Optional[Facilities]
    expiration_time: Optional[datetime]

    def __init__(
        self,
        stop_id: Optional[str] = None,
        route: Optional[str] = None,
        direction: Optional[int] = None,
        expiration_time: Optional[datetime] = None,
        route_type: Optional[str] = None,
        schedule_only=False,
        watcher_type=EventType.PREDICTIONS,
    ):
        thirty_hours = 108000
        thirty_min = 1800

        self.stop_id = stop_id
        self.route = route
        self.direction = direction
        self.trips = ExpiringDict(thirty_hours)
        self.alerts = ExpiringDict(thirty_min)
        self.routes = dict()
        self.expiration_time = expiration_time
        self.watcher_type = watcher_type
        logger.info(
            f"Init mbta_client for stop={stop_id}, route={route}, direction={direction}, route_type={route_type}"
        )
        self.schedule_only = schedule_only

    @staticmethod
    def determine_time(attributes: PredictionAttributes) -> datetime | None:
        if attributes.arrival_time:
            return datetime.fromisoformat(attributes.arrival_time).astimezone(UTC)
        elif attributes.departure_time:
            return datetime.fromisoformat(attributes.departure_time).astimezone(UTC)
        else:
            return None

    def get_headsign(self, trip_id: str):
        if trip_id in self.trips:
            hs = self.trips[trip_id].attributes.headsign
            if self.trips[trip_id].attributes.revenue_status == "NON_REVENUE":
                return f"[NR] ${hs}"
            return hs
        return ""

    async def parse_live_api_response(
        self,
        data: str,
        event_type: str,
        queue: Queue[ScheduleEvent | VehicleRedisSchema],
        transit_time_min: Optional[int],
        session: ClientSession,
    ):
        # https://www.mbta.com/developers/v3-api/streaming
        try:
            match event_type:
                case "reset":
                    if (
                        self.watcher_type == EventType.PREDICTIONS
                        or self.watcher_type == EventType.SCHEDULES
                    ):
                        ta = TypeAdapter(list[PredictionResource])
                        prediction = ta.validate_json(data, strict=False)
                        for item in prediction:
                            await self.save_trip(item, session)
                            await self.queue_event(
                                item,
                                "reset",
                                queue,
                                transit_time_min=transit_time_min,
                                session=session,
                            )
                        if len(prediction) == 0:
                            await self.save_schedule(
                                transit_time_min, queue, session, timedelta(hours=4)
                            )
                    else:
                        ta = TypeAdapter(list[Vehicle])
                        vehicles = ta.validate_json(data, strict=False)
                        for v in vehicles:
                            await self.queue_event(
                                v, event_type, queue, session=session
                            )

                case "add":
                    if (
                        self.watcher_type == EventType.PREDICTIONS
                        or self.watcher_type == EventType.SCHEDULES
                    ):
                        prediction = PredictionResource.model_validate_json(
                            data, strict=False
                        )
                        await self.save_trip(prediction, session)
                        await self.queue_event(
                            prediction,
                            "add",
                            queue,
                            transit_time_min=transit_time_min,
                            session=session,
                        )
                    else:
                        vehicle = Vehicle.model_validate_json(data, strict=False)
                        await self.queue_event(vehicle, event_type, queue, session)
                case "update":
                    if (
                        self.watcher_type == EventType.PREDICTIONS
                        or self.watcher_type == EventType.SCHEDULES
                    ):
                        prediction = PredictionResource.model_validate_json(
                            data, strict=False
                        )
                        await self.queue_event(
                            prediction,
                            "update",
                            queue,
                            transit_time_min=transit_time_min,
                            session=session,
                        )
                    else:
                        vehicle = Vehicle.model_validate_json(data, strict=False)
                        await self.queue_event(vehicle, event_type, queue, session)
                case "remove":
                    type_and_id = TypeAndID.model_validate_json(data, strict=False)
                    # directly interact with the queue here to use a dummy object
                    if (
                        self.watcher_type == EventType.PREDICTIONS
                        or self.watcher_type == EventType.SCHEDULES
                    ):
                        queue.put(dummy_schedule_event(type_and_id.id))
                    else:
                        queue.put(
                            VehicleRedisSchema(
                                longitude=0,
                                latitude=0,
                                direction_id=0,
                                current_status="",
                                id=type_and_id.id,
                                action="remove",
                                route=self.route,
                            )
                        )

        except ValidationError as err:
            logger.error("Unable to parse schedule", exc_info=err)
        except KeyError as err:
            logger.error("Could not find prediction", exc_info=err)

    async def get_alerting_state(self, trip_id: str, session: ClientSession):
        alerting = self.alerts.get(trip_id)
        if alerting is None:
            return await self.save_alert(trip_id, session)
        return alerting

    # checks if secure bike storage is available at the facility
    def check_secure_bike_storage(self):
        enclosed = False
        secured = False
        for facility in self.facilities.data:
            for prop in facility.attributes.properties:
                if prop.name == "enclosed" and prop.value != 0:
                    enclosed = True
                if prop.name == "secured" and prop.value != 0:
                    secured = True
        return enclosed and secured

    def bikes_allowed(self, trip: TripResource):
        match trip.attributes.bikes_allowed:
            case 0:
                return self.check_secure_bike_storage()
            case 1:
                return True
            case 2:
                return self.check_secure_bike_storage()
            case _:
                return self.check_secure_bike_storage()

    # abbreviate common words to fit more on screen
    @staticmethod
    def abbreviate(inp: str):
        inp = inp.replace("Massachusetts", "Mass")
        inp = inp.replace("Street", "St")
        inp = inp.replace("Avenue", "Ave")
        inp = inp.replace("Square", "Sq")
        inp = inp.replace("Road", "Rd")
        inp = inp.replace("Government", "Gov't")
        inp = inp.replace("Parkway", "Pkwy")
        return inp

    async def queue_event(
        self,
        item: PredictionResource | ScheduleResource | Vehicle,
        event_type: str,
        queue: Queue[ScheduleEvent | VehicleRedisSchema],
        session: ClientSession,
        transit_time_min: Optional[int] = None,
    ):
        if (
            self.watcher_type == EventType.PREDICTIONS
            or self.watcher_type == EventType.SCHEDULES
        ):
            schedule_time = self.determine_time(item.attributes)
            if schedule_time and schedule_time > datetime.now().astimezone(UTC):
                await self.save_route(item, session)
                route_id = item.relationships.route.data.id
                headsign = self.get_headsign(item.relationships.trip.data.id)

                # drop events that have the same stop & headsign as that train cannot be
                # immediately boarded in most cases so there is no sense in showing it as a departure
                if headsign == self.stop.data.attributes.name:
                    logger.info(
                        f"Dropping invalid schedule event {headsign}/{headsign}"
                    )
                    return
                if route_id.startswith("Green"):
                    branch = route_id[-1:]
                    headsign = f"{branch} - {headsign}"
                if route_id.startswith("CR"):
                    line = route_id.split("-")
                    headsign = f"{headsign} - {line[1]} Ln"
                route_type = self.routes[route_id].attributes.type
                alerting = False
                trip = ""
                bikes_allowed = False
                if item.relationships.trip.data:
                    trip = item.relationships.trip.data.id
                    trip_info = self.trips[trip]
                    if trip_info:
                        bikes_allowed = self.bikes_allowed(trip_info)
                    alerting = await self.get_alerting_state(trip, session)

                event = ScheduleEvent(
                    action=event_type,
                    time=schedule_time,
                    route_id=route_id,
                    route_type=route_type,
                    headsign=self.abbreviate(headsign),
                    id=item.id,
                    stop=self.abbreviate(self.stop.data.attributes.name),
                    transit_time_min=transit_time_min,
                    trip_id=trip,
                    alerting=alerting,
                    bikes_allowed=bikes_allowed,
                )
                queue.put(event)
        else:
            event = VehicleRedisSchema(
                action=event_type,
                id=item.id,
                current_status=item.attributes.current_status,
                direction_id=item.attributes.direction_id,
                latitude=item.attributes.latitude,
                longitude=item.attributes.longitude,
                speed=item.attributes.speed,
                route=item.relationships.route.data.id,
            )
            queue.put(event)

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    async def save_trip(
        self, prediction: PredictionResource | ScheduleResource, session: ClientSession
    ):
        trip_id = prediction.relationships.trip.data.id
        if trip_id and trip_id not in self.trips:
            async with session.get(
                f"trips?filter[id]={trip_id}&api_key={auth_token}"
            ) as response:
                try:
                    body = await response.text()
                    mbta_api_requests.labels("trips").inc()

                    trip = Trips.model_validate_json(body, strict=False)
                    for tr in trip.data:
                        self.trips[trip_id] = tr
                except ValidationError as err:
                    logger.error(f"Unable to parse trip, {err}")

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    async def save_route(
        self, prediction: PredictionResource | ScheduleResource, session: ClientSession
    ):
        route_id = prediction.relationships.route.data.id
        if route_id not in self.routes:
            async with session.get(
                f"routes?filter[id]={route_id}&api_key={auth_token}"
            ) as response:
                try:
                    body = await response.text()
                    mbta_api_requests.labels("routes").inc()
                    route = Route.model_validate_json(body, strict=False)
                    for rd in route.data:
                        self.routes[route_id] = rd
                except ValidationError as err:
                    logger.error(f"Unable to parse route, {err}")

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    async def save_alert(self, trip_id: str, session: ClientSession):
        endpoint = f"alerts?filter[trip]={trip_id}&api_key={auth_token}&filter[lifecycle]=NEW,ONGOING,ONGOING_UPCOMING&filter[datetime]=NOW&filter[severity]=3,4,5,6,7,8,9,10"
        async with session.get(endpoint) as response:
            try:
                body = await response.text()
                mbta_api_requests.labels("alerts").inc()
                alerts = Alerts.model_validate_json(strict=False, json_data=body)
                if len(alerts.data) > 0:
                    self.alerts[trip_id] = True
                    return True
                else:
                    self.alerts[trip_id] = False
                    return False
            except ValidationError as err:
                logger.error("Unable to parse alert", exc_info=err)

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    async def save_schedule(
        self,
        transit_time_min: int,
        queue: Queue[ScheduleEvent | VehicleRedisSchema],
        session: ClientSession,
        time_limit: Optional[timedelta] = None,
    ):
        endpoint = f"schedules?filter[stop]={self.stop_id}&sort=time&api_key={auth_token}&filter[date]={datetime.now().date().isoformat()}"
        if self.route != "":
            endpoint += f"&filter[route]={self.route}"
        if self.direction != "":
            endpoint += f"&filter[direction_id]={self.direction}"
        if time_limit:
            diff = datetime.now().astimezone(ZoneInfo("US/Eastern")) + time_limit
            if diff.hour >= 2:
                # Time after which schedule should not be returned.
                # To filter times after midnight use more than 24 hours.
                # For example, min_time=24:00 will return schedule information for the next calendar day, since that service is considered part of the current service day.
                # Additionally, min_time=00:00&max_time=02:00 will not return anything. The time format is HH:MM.
                # https://api-v3.mbta.com/docs/swagger/index.html#/Schedule/ApiWeb_ScheduleController_index
                endpoint += f"&filter[max_time]={diff.strftime('%H:%M')}"
        async with session.get(endpoint) as response:
            try:
                body = await response.text()
                mbta_api_requests.labels("schedules").inc()
                schedules = Schedules.model_validate_json(strict=False, json_data=body)

                for item in schedules.data:
                    await self.save_trip(item, session)
                    await self.save_route(item, session)
                    await self.queue_event(
                        item,
                        "reset",
                        queue,
                        transit_time_min=transit_time_min,
                        session=session,
                    )
            except ValidationError as err:
                logger.error("Unable to parse schedule", exc_info=err)

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    async def save_stop(self, session: ClientSession):
        if self.stop_id:
            async with session.get(
                f"/stops/{self.stop_id}?api_key={auth_token}"
            ) as response:
                try:
                    body = await response.text()
                    mbta_api_requests.labels("stops").inc()
                    stop = Stop.model_validate_json(body, strict=False)
                    logger.info(f"saved stop {stop.data.attributes.name}")
                    self.stop = stop
                except ValidationError as err:
                    logger.error("Unable to parse stop", exc_info=err)
            # retrieve bike facility info
            async with session.get(
                f"/facilities/?filter[stop]={self.stop_id}&filter[type]=BIKE_STORAGE"
            ) as response:
                try:
                    body = await response.text()
                    mbta_api_requests.labels("facilities").inc()
                    facilities = Facilities.model_validate_json(body, strict=False)
                    self.facilities = facilities
                except ValidationError as err:
                    logger.error("Unable to parse facility", exc_info=err)


@retry(
    wait=wait_random_exponential(multiplier=1),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def watch_server_side_events(
    watcher: Watcher,
    endpoint: str,
    headers: dict[str, str],
    queue: Queue[ScheduleEvent | VehicleRedisSchema],
    session: ClientSession,
    transit_time_min: Optional[int] = None,
):
    async for event in aiosseclient(endpoint, headers=headers):
        if datetime.now().astimezone(UTC) > watcher.expiration_time:
            logger.info(
                f"Restarting thread {watcher.watcher_type} - {watcher.stop_id}/{watcher.route_type}"
            )
            return
        await watcher.parse_live_api_response(
            event.data, event.event, queue, transit_time_min, session
        )
        await sleep(random.randint(1, 30))


async def watch_static_schedule(
    stop_id: str,
    route: str | None,
    direction: int | None,
    queue: Queue[ScheduleEvent | VehicleRedisSchema],
    transit_time_min: int,
):
    while True:
        watcher = Watcher(
            stop_id=stop_id,
            route=route,
            direction=direction,
            schedule_only=True,
            watcher_type=EventType.SCHEDULES,
        )
        async with aiohttp.ClientSession(mbta_v3) as session:
            await watcher.save_stop(session)
            if watcher.stop:
                tracker_executions.labels(watcher.stop.data.attributes.name).inc()
            await watcher.save_schedule(transit_time_min, queue, session)
        await sleep(10800)  # 3 hours


async def watch_vehicles(
    queue: Queue[ScheduleEvent | VehicleRedisSchema],
    expiration_time: datetime,
    route_id: str,
):
    endpoint = f"{mbta_v3}/vehicles?filter[revenue]=REVENUE&fields[vehicle]=direction_id,latitude,longitude,speed,current_status&filter[route]={route_id}&api_key={auth_token}"
    mbta_api_requests.labels("vehicles").inc()
    headers = {"accept": "text/event-stream"}
    watcher = Watcher(
        route=route_id,
        watcher_type=EventType.VEHICLES,
        expiration_time=expiration_time,
    )
    async with aiohttp.ClientSession(mbta_v3) as session:
        tracker_executions.labels("vehicles").inc()
        await watch_server_side_events(
            watcher, endpoint, headers, queue, session=session
        )


async def watch_station(
    stop_id: str,
    route: str | None,
    direction: str | None,
    queue: Queue[ScheduleEvent | VehicleRedisSchema],
    transit_time_min: int,
    expiration_time: datetime,
):
    endpoint = f"{mbta_v3}/predictions?filter[stop]={stop_id}&api_key={auth_token}"
    mbta_api_requests.labels("predictions").inc()
    if route != "":
        endpoint += f"&filter[route]={route}"
    if direction != "":
        endpoint += f"&filter[direction_id]={direction}"
    headers = {"accept": "text/event-stream"}
    watcher = Watcher(
        stop_id, route, direction, expiration_time, watcher_type=EventType.PREDICTIONS
    )
    async with aiohttp.ClientSession(mbta_v3) as session:
        await watcher.save_stop(session)
        tracker_executions.labels(watcher.stop.data.attributes.name).inc()
        await watch_server_side_events(
            watcher,
            endpoint,
            headers,
            queue,
            transit_time_min=transit_time_min,
            session=session,
        )


def thread_runner(
    target: EventType,
    queue: Queue[ScheduleEvent | VehicleRedisSchema],
    transit_time_min: int = 0,
    stop_id: Optional[str] = None,
    route: Optional[str] = None,
    direction: Optional[str] = None,
    expiration_time: Optional[datetime] = None,
    route_id: Optional[str] = None,
):
    with Runner() as runner:
        match target:
            case EventType.SCHEDULES:
                runner.run(
                    watch_static_schedule(
                        stop_id,
                        route,
                        direction,
                        queue,
                        transit_time_min,
                    )
                )
            case EventType.PREDICTIONS:
                runner.run(
                    watch_station(
                        stop_id,
                        route,
                        direction,
                        queue,
                        transit_time_min,
                        expiration_time,
                    )
                )
            case EventType.VEHICLES:
                runner.run(
                    watch_vehicles(
                        queue,
                        expiration_time,
                        route_id,
                    )
                )
