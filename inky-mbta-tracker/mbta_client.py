# client that keeps track of events on the stops specified
import logging
import os
import random
from asyncio import CancelledError, sleep
from datetime import UTC, datetime
from queue import Queue
from typing import Optional

import aiohttp
from aiohttp import ClientSession
from aiosseclient import aiosseclient
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
from prometheus import mbta_api_requests
from pydantic import TypeAdapter, ValidationError
from schedule_tracker import ScheduleEvent
from tenacity import (
    before_log,
    before_sleep_log,
    retry,
    retry_if_not_exception_type,
    wait_exponential,
    wait_random_exponential,
)

auth_token = os.environ.get("AUTH_TOKEN")
mbta_v3 = "https://api-v3.mbta.com"
thirty_hours = 108000
logger = logging.getLogger(__name__)


class Watcher:
    stop_id: str
    route: str | None
    direction: int | None
    trips: ExpiringDict[str, TripResource]
    routes: dict[str, RouteResource]
    stop: Optional[Stop]
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
            return datetime.fromisoformat(attributes.arrival_time).astimezone(UTC)
        elif attributes.departure_time:
            return datetime.fromisoformat(attributes.departure_time).astimezone(UTC)
        else:
            return None

    def get_headsign(self, trip_id: str):
        if trip_id in self.trips:
            return self.trips[trip_id].attributes.headsign
        return ""

    async def parse_schedule_response(
        self,
        data: str,
        event_type: str,
        queue: Queue[ScheduleEvent],
        transit_time_min: int,
        session: ClientSession,
    ):
        # https://www.mbta.com/developers/v3-api/streaming
        try:
            match event_type:
                case "reset":
                    ta = TypeAdapter(list[PredictionResource])
                    prediction = ta.validate_json(data, strict=False)
                    for item in prediction:
                        await self.save_trip(item, session)
                        await self.queue_schedule_event(
                            item, "reset", queue, transit_time_min, session
                        )
                    if len(prediction) == 0:
                        await self.save_schedule(transit_time_min, queue, session)
                case "add":
                    prediction = PredictionResource.model_validate_json(
                        data, strict=False
                    )
                    await self.save_trip(prediction, session)
                    await self.queue_schedule_event(
                        prediction, "add", queue, transit_time_min, session
                    )
                case "update":
                    prediction = PredictionResource.model_validate_json(
                        data, strict=False
                    )
                    await self.queue_schedule_event(
                        prediction, "update", queue, transit_time_min, session
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
                        time=datetime.now(UTC),
                        transit_time_min=transit_time_min,
                    )
                    queue.put(event)

        except ValidationError as err:
            logger.error("Unable to parse schedule", exc_info=err)
        except KeyError as err:
            logger.error("Could not find prediction", exc_info=err)

    async def queue_schedule_event(
        self,
        item: PredictionResource | ScheduleResource,
        event_type: str,
        queue: Queue[ScheduleEvent],
        transit_time_min: int,
        session: ClientSession,
    ):
        schedule_time = self.determine_time(item.attributes)
        if schedule_time > datetime.now().astimezone(UTC):
            if not schedule_time:
                logger.warning(f"no time associated with event {item.id}, skipping")
                return
            await self.save_route(item, session)
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
    async def save_schedule(
        self, transit_time_min: int, queue: Queue[ScheduleEvent], session: ClientSession
    ):
        endpoint = f"schedules?filter[stop]={self.stop_id}&sort=time&api_key={auth_token}&filter[date]={datetime.now().date().isoformat()}"
        if self.route != "":
            endpoint += f"&filter[route]={self.route}"
        if self.direction != "":
            endpoint += f"&filter[direction_id]={self.direction}"
        async with session.get(endpoint) as response:
            try:
                body = await response.text()
                mbta_api_requests.labels("schedules").inc()
                schedules = Schedules.model_validate_json(strict=False, json_data=body)

                for item in schedules.data:
                    await self.save_trip(item, session)
                    await self.save_route(item, session)
                    await self.queue_schedule_event(
                        item, "reset", queue, transit_time_min, session
                    )
            except ValidationError as err:
                logger.error("Unable to parse schedule", exc_info=err)

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    async def save_stop(self, session: ClientSession):
        async with session.get(
            f"/stops/{self.stop_id}?api_key={auth_token}"
        ) as response:
            try:
                body = await response.text()
                mbta_api_requests.labels("stops").inc()
                stop = Stop.model_validate_json(body, strict=False)
                logger.info(f"saved stop {stop.data.attributes.name}")
                self.stop = stop
                return stop
            except ValidationError as err:
                logger.error("Unable to parse stop", exc_info=err)


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
    queue: Queue[ScheduleEvent],
    transit_time_min: int,
    session: ClientSession,
):
    async for event in aiosseclient(endpoint, headers=headers):
        await watcher.parse_schedule_response(
            event.data, event.event, queue, transit_time_min, session
        )
        await sleep(random.randint(1, 30))


async def watch_static_schedule(
    stop_id: str,
    route: str | None,
    direction: int | None,
    queue: Queue[ScheduleEvent],
    transit_time_min: int,
):
    while True:
        watcher = Watcher(stop_id, route, direction, schedule_only=True)
        async with aiohttp.ClientSession(mbta_v3) as session:
            await watcher.save_stop(session)
            await watcher.save_schedule(transit_time_min, queue, session)
            await sleep(10800)  # 3 hours


async def watch_station(
    stop_id: str,
    route: str | None,
    direction: str | None,
    queue: Queue[ScheduleEvent],
    transit_time_min: int,
):
    endpoint = f"{mbta_v3}/predictions?filter[stop]={stop_id}&api_key={auth_token}"
    mbta_api_requests.labels("predictions").inc()
    if route != "":
        endpoint += f"&filter[route]={route}"
    if direction != "":
        endpoint += f"&filter[direction_id]={direction}"
    headers = {"accept": "text/event-stream"}
    watcher = Watcher(stop_id, route, direction)
    async with aiohttp.ClientSession(mbta_v3) as session:
        await watcher.save_stop(session)
        await watch_server_side_events(
            watcher, endpoint, headers, queue, transit_time_min, session
        )
