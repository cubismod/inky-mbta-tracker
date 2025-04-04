import logging
import os
from asyncio import CancelledError, Runner, sleep
from collections import Counter
from datetime import UTC, datetime, timedelta
from enum import Enum
from queue import Queue
from random import randint
from typing import Optional

import aiohttp
from aiohttp import ClientSession
from aiosseclient import aiosseclient
from mbta_responses import (
    AlertResource,
    Alerts,
    Facilities,
    PredictionAttributes,
    PredictionResource,
    Route,
    RouteResource,
    ScheduleAttributes,
    ScheduleResource,
    Schedules,
    Shapes,
    Stop,
    StopAndFacilities,
    TripResource,
    Trips,
    TypeAndID,
    Vehicle,
)
from polyline import decode
from prometheus import mbta_api_requests, tracker_executions
from pydantic import BaseModel, TypeAdapter, ValidationError
from redis.asyncio.client import Redis
from redis_cache import check_cache, write_cache
from schedule_tracker import ScheduleEvent, VehicleRedisSchema, dummy_schedule_event
from tenacity import (
    before_log,
    before_sleep_log,
    retry,
    retry_if_not_exception_type,
    wait_random_exponential,
)
from times_in_seconds import DAY, FOUR_WEEKS, TWO_MONTHS
from zoneinfo import ZoneInfo

MBTA_AUTH = os.environ.get("AUTH_TOKEN")
MBTA_V3_ENDPOINT = "https://api-v3.mbta.com"
logger = logging.getLogger(__name__)
SHAPE_POLYLINES = set[str]()


class EventType(Enum):
    OTHER = -1
    PREDICTIONS = 0
    SCHEDULES = 1
    VEHICLES = 2


class LightStop(BaseModel):
    stop_id: str
    long: Optional[float] = None
    lat: Optional[float] = None


def parse_shape_data(shapes: Shapes) -> list[list[tuple]]:
    ret = list[list[tuple]]()
    for shape in shapes.data:
        if (
            shape.attributes.polyline not in SHAPE_POLYLINES
            and "canonical" in shape.id
            or shape.id.replace("_", "").isdecimal()
        ):
            ret.append([i for i in decode(shape.attributes.polyline, geojson=True)])
            SHAPE_POLYLINES.add(shape.attributes.polyline)
    return ret


# gets line (orange, blue, red, green, etc) geometry using MBTA API
# redis expires in 24 hours
@retry(
    wait=wait_random_exponential(multiplier=2, min=10),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
)
async def get_shapes(
    r_client: Redis, routes: list[str]
) -> dict[str, list[list[tuple]]]:
    ret = dict[str, list[list[tuple]]]()
    async with aiohttp.ClientSession(MBTA_V3_ENDPOINT) as session:
        for route in routes:
            key = f"shape-{route}"
            cached = await check_cache(r_client, key)
            body = ""
            if cached:
                body = cached
            else:
                async with session.get(
                    f"/shapes?filter[route]={route}&api_key={MBTA_AUTH}"
                ) as response:
                    body = await response.text()
                    mbta_api_requests.labels("shapes").inc()
                    # 4 weeks
                    await write_cache(r_client, key, body, 2419200)
            shapes = Shapes.model_validate_json(body, strict=False)
            ret[route] = parse_shape_data(shapes)
        return ret


def silver_line_lookup(route_id: str) -> str:
    match route_id:
        case "741":
            return "SL1"
        case "742":
            return "SL2"
        case "743":
            return "SL3"
        case "746":
            return "SLW"
        case "749":
            return "SL5"
        case "751":
            return "SL4"
        case _:
            return route_id


# retrieves a rail/bus stop from Redis & returns the stop ID with optional coordinates
async def light_get_stop(r_client: Redis, stop_id: str) -> Optional[LightStop]:
    key = f"light-stop-{stop_id}"
    cached = await check_cache(r_client, key)
    if cached:
        try:
            cached_model = LightStop.model_validate_json(cached)
            return cached_model
        except ValidationError as err:
            logger.error("unable to validate json", exc_info=err)
    ls = None
    async with aiohttp.ClientSession(MBTA_V3_ENDPOINT) as session:
        watcher = Watcher(stop_id=stop_id, watcher_type=EventType.OTHER)
        # avoid rate-limiting by spacing out requests
        await sleep(1)
        stop = await watcher.get_stop(session, stop_id)
        if stop and stop[0]:
            if stop[0].data.attributes.description:
                stop_id = stop[0].data.attributes.description
            elif stop[0].data.attributes.name:
                stop_id = stop[0].data.attributes.name
            ls = LightStop(
                stop_id=stop_id,
                long=stop[0].data.attributes.longitude,
                lat=stop[0].data.attributes.latitude,
            )
        if ls:
            await write_cache(
                r_client, key, ls.model_dump_json(), randint(FOUR_WEEKS, TWO_MONTHS)
            )
    return ls


async def light_get_alerts(route_id: str) -> Optional[list[AlertResource]]:
    async with aiohttp.ClientSession(MBTA_V3_ENDPOINT) as session:
        watcher = Watcher(route=route_id, watcher_type=EventType.VEHICLES)
        alerts = await watcher.get_alerts(session, route_id=route_id)
        if alerts:
            return alerts
    return None


class Watcher:
    # by default is predictions, can also be vehicles for a live
    # vehicle watcher
    watcher_type: EventType
    stop_id: Optional[str]
    route: Optional[str]
    direction_filter: Optional[int]
    routes: dict[str, RouteResource]
    stop: Optional[Stop]
    schedule_only: bool
    facilities: Optional[Facilities]
    expiration_time: Optional[datetime]
    r_client: Redis

    def __init__(
        self,
        stop_id: Optional[str] = None,
        route: Optional[str] = None,
        direction_filter: Optional[int] = None,
        expiration_time: Optional[datetime] = None,
        route_type: Optional[str] = None,
        schedule_only: bool = False,
        watcher_type: EventType = EventType.PREDICTIONS,
    ):
        self.stop_id = stop_id
        self.route = route
        self.direction_filter = direction_filter
        self.routes = dict()
        self.expiration_time = expiration_time
        self.watcher_type = watcher_type
        logger.info(
            f"Init mbta_client for type={watcher_type} stop={stop_id}, route={route}, direction={direction_filter}, route_type={route_type}"
        )
        self.schedule_only = schedule_only
        self.r_client = Redis(
            host=os.environ.get("IMT_REDIS_ENDPOINT", ""),
            port=int(os.environ.get("IMT_REDIS_PORT", "6379")),
            password=os.environ.get("IMT_REDIS_PASSWORD", ""),
        )

    @staticmethod
    def determine_time(
        attributes: PredictionAttributes | ScheduleAttributes,
    ) -> datetime | None:
        if attributes.arrival_time:
            return datetime.fromisoformat(attributes.arrival_time).astimezone(UTC)
        elif attributes.departure_time:
            return datetime.fromisoformat(attributes.departure_time).astimezone(UTC)
        else:
            return None

    async def get_headsign(self, trip_id: str, session: ClientSession) -> str:
        hs = ""
        trip = await self.get_trip(trip_id, session)
        if trip and len(trip.data) > 0:
            hs = trip.data[0].attributes.headsign
            if trip.data[0].attributes.revenue_status == "NON_REVENUE":
                return f"[NR] ${hs}"
        return hs

    async def parse_live_api_response(
        self,
        data: str,
        event_type: str,
        queue: Queue[ScheduleEvent | VehicleRedisSchema],
        transit_time_min: int,
        session: ClientSession,
    ) -> None:
        # https://www.mbta.com/developers/v3-api/streaming
        try:
            match event_type:
                case "reset":
                    if (
                        self.watcher_type == EventType.PREDICTIONS
                        or self.watcher_type == EventType.SCHEDULES
                    ):
                        ta = TypeAdapter(list[PredictionResource])
                        prediction_resource = ta.validate_json(data, strict=False)
                        for item in prediction_resource:
                            await self.queue_event(
                                item,
                                "reset",
                                queue,
                                transit_time_min=transit_time_min,
                                session=session,
                            )
                        if len(prediction_resource) == 0:
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
                        await self.queue_event(
                            PredictionResource.model_validate_json(data, strict=False),
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
                        await self.queue_event(
                            PredictionResource.model_validate_json(data, strict=False),
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
                                update_time=datetime.now().astimezone(UTC),
                            )
                        )

        except ValidationError as err:
            logger.error("Unable to parse schedule", exc_info=err)
        except KeyError as err:
            logger.error("Could not find prediction", exc_info=err)

    async def get_alerting_state(self, trip_id: str, session: ClientSession) -> bool:
        alerts = await self.get_alerts(trip_id=trip_id, session=session)
        if alerts and len(alerts) > 0:
            return True
        else:
            return False

    # checks if secure bike storage is available at the facility
    # no parameters as this is associated with the stop set in the config
    # of this watcher
    def check_secure_bike_storage(self) -> bool:
        enclosed = False
        secured = False
        if self.facilities and self.facilities.data:
            for facility in self.facilities.data:
                for prop in facility.attributes.properties:
                    if prop.name == "enclosed" and prop.value != 0:
                        enclosed = True
                    if prop.name == "secured" and prop.value != 0:
                        secured = True
        return enclosed and secured

    def bikes_allowed(self, trip: TripResource) -> bool:
        match trip.attributes.bikes_allowed:
            case 1:
                return True
            case _:
                return self.check_secure_bike_storage()

    @staticmethod
    def meters_per_second_to_mph(speed: Optional[float]) -> Optional[float]:
        if speed:
            return round(speed * 2.23693629, 2)
        return None

    @staticmethod
    def occupancy_status_human_readable(occupancy: str) -> str:
        return occupancy.replace("_", " ").capitalize()

    @staticmethod
    def calculate_carriage_occupancy(vehicle: Vehicle) -> str:
        if vehicle.attributes.carriages:
            statuses = list[str]()
            [
                statuses.append(carriage.occupancy_status)  # type: ignore
                for carriage in vehicle.attributes.carriages
                if carriage.occupancy_status
                and carriage.occupancy_status != "NO_DATA_AVAILABLE"
            ]
            if len(statuses) > 0:
                count = Counter(statuses)
                return count.most_common(1)[0][0]
        return ""

    # abbreviate common words to fit more on screen
    @staticmethod
    def abbreviate(inp: str) -> str:
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
    ) -> None:
        if isinstance(item, PredictionResource) or isinstance(item, ScheduleResource):
            schedule_time = self.determine_time(item.attributes)
            if schedule_time and schedule_time > datetime.now().astimezone(UTC):
                await self.save_route(item, session)
                if (
                    item.relationships.route
                    and item.relationships.route.data
                    and item.relationships.trip.data
                ):
                    route_id = item.relationships.route.data.id
                    headsign = await self.get_headsign(
                        item.relationships.trip.data.id, session
                    )

                    # drop events that have the same stop & headsign as that train cannot be
                    # immediately boarded in most cases so there is no sense in showing it as a departure
                    if self.stop and headsign == self.stop.data.attributes.name:
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
                    bikes_allowed = False
                    trip_id = ""
                    if item.relationships.trip.data:
                        trip_id = item.relationships.trip.data.id
                        trip = await self.get_trip(trip_id, session)
                        if trip and len(trip.data) > 0:
                            bikes_allowed = self.bikes_allowed(trip.data[0])
                            alerting = await self.get_alerting_state(trip_id, session)
                    stop_name = ""
                    if self.stop:
                        stop_name = self.stop.data.attributes.name

                    event = ScheduleEvent(
                        action=event_type,
                        time=schedule_time,
                        route_id=route_id,
                        route_type=route_type,
                        headsign=self.abbreviate(headsign),
                        id=item.id,
                        stop=self.abbreviate(stop_name),
                        transit_time_min=transit_time_min,
                        trip_id=trip_id,
                        alerting=alerting,
                        bikes_allowed=bikes_allowed,
                    )
                    queue.put(event)
        else:
            occupancy = item.attributes.occupancy_status
            if not occupancy:
                occupancy = self.calculate_carriage_occupancy(item)
            if occupancy:
                occupancy = self.occupancy_status_human_readable(occupancy)
            route = ""
            trip_id = item.id
            if item.relationships.route.data:
                route = item.relationships.route.data.id
            if (
                "CR" in route
                and item.relationships.trip
                and item.relationships.trip.data
            ):
                # save the trip name as this is what the T uses to refer to specific trains on commuter rail
                trip_info = await self.get_trip(
                    item.relationships.trip.data.id, session
                )
                if (
                    trip_info
                    and len(trip_info.data) > 0
                    and trip_info.data[0].attributes.name != ""
                ):
                    trip_id = trip_info.data[0].attributes.name
            event = VehicleRedisSchema(  # type: ignore
                action=event_type,
                id=trip_id,
                current_status=item.attributes.current_status,
                direction_id=item.attributes.direction_id,
                latitude=item.attributes.latitude,
                longitude=item.attributes.longitude,
                speed=self.meters_per_second_to_mph(item.attributes.speed),
                route=route,
                update_time=datetime.now().astimezone(UTC),
                occupancy_status=occupancy,
            )
            if item.relationships.stop and item.relationships.stop.data:
                event.stop = item.relationships.stop.data.id
            queue.put(event)

    @retry(
        wait=wait_random_exponential(multiplier=1, min=1),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    async def get_trip(self, trip_id: str, session: ClientSession) -> Optional[Trips]:
        key = f"tripc-{trip_id}"
        cached = await check_cache(self.r_client, key)
        try:
            if cached:
                trip = Trips.model_validate_json(cached, strict=False)
                return trip
            else:
                async with session.get(
                    f"trips?filter[id]={trip_id}&api_key={MBTA_AUTH}"
                ) as response:
                    body = await response.text()
                    mbta_api_requests.labels("trips").inc()

                    trip = Trips.model_validate_json(body, strict=False)
                    await write_cache(self.r_client, key, trip.model_dump_json(), DAY)
                    return trip
        except ValidationError as err:
            logger.error(f"Unable to parse trip, {err}")
        return None

    # saves a route to the dict of routes rather than redis
    @retry(
        wait=wait_random_exponential(multiplier=1, min=1),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    async def save_route(
        self, prediction: PredictionResource | ScheduleResource, session: ClientSession
    ) -> None:
        if prediction.relationships.route and prediction.relationships.route.data:
            route_id = prediction.relationships.route.data.id
            if route_id not in self.routes:
                async with session.get(
                    f"routes?filter[id]={route_id}&api_key={MBTA_AUTH}"
                ) as response:
                    try:
                        body = await response.text()
                        mbta_api_requests.labels("routes").inc()
                        route = Route.model_validate_json(body, strict=False)
                        for rd in route.data:
                            logger.info(f"route {rd.id} saved")
                            self.routes[route_id] = rd
                    except ValidationError as err:
                        logger.error(f"Unable to parse route, {err}")

    @retry(
        wait=wait_random_exponential(multiplier=1, min=1),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    async def get_alerts(
        self,
        session: ClientSession,
        trip_id: Optional[str] = None,
        route_id: Optional[str] = None,
    ) -> Optional[list[AlertResource]]:
        endpoint = "alerts"
        if trip_id:
            endpoint += f"?filter[trip]={trip_id}"
        elif route_id:
            endpoint += f"?filter[route]={route_id}"
        else:
            logging.error("you need to specify a trip_id or route_id to fetch alerts")
            return None
        endpoint += f"&api_key={MBTA_AUTH}&filter[lifecycle]=NEW,ONGOING,ONGOING_UPCOMING&filter[datetime]=NOW&filter[severity]=3,4,5,6,7,8,9,10"

        async with session.get(endpoint) as response:
            try:
                body = await response.text()
                mbta_api_requests.labels("alerts").inc()
                alerts = Alerts.model_validate_json(strict=False, json_data=body)
                return alerts.data
            except ValidationError as err:
                logger.error("Unable to parse alert", exc_info=err)
        return None

    @retry(
        wait=wait_random_exponential(multiplier=1, min=1),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    async def save_schedule(
        self,
        transit_time_min: int,
        queue: Queue[ScheduleEvent | VehicleRedisSchema],
        session: ClientSession,
        time_limit: Optional[timedelta] = None,
    ) -> None:
        endpoint = f"schedules?filter[stop]={self.stop_id}&sort=time&api_key={MBTA_AUTH}&filter[date]={datetime.now().date().isoformat()}"
        if self.route != "":
            endpoint += f"&filter[route]={self.route}"
        if self.direction_filter != "":
            endpoint += f"&filter[direction_id]={self.direction_filter}"
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

    async def save_own_stop(self, session: ClientSession) -> None:
        if self.stop_id:
            stop_and_facilities = await self.get_stop(session, self.stop_id)
            self.stop = stop_and_facilities[0]
            self.facilities = stop_and_facilities[1]

    # 3 weeks of caching in redis as maybe a stop will change? idk
    @retry(
        wait=wait_random_exponential(multiplier=2, min=10),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    )
    async def get_stop(
        self, session: ClientSession, stop_id: str
    ) -> tuple[Optional[Stop], Optional[Facilities]]:
        key = f"stop-{stop_id}"
        stop = None
        facilities = None
        cached = await check_cache(self.r_client, key)
        if cached:
            try:
                s_and_f = StopAndFacilities.model_validate_json(cached)
                return s_and_f.stop, s_and_f.facilities
            except ValidationError as err:
                logger.error("validation err", exc_info=err)
        else:
            async with session.get(f"/stops/{stop_id}?api_key={MBTA_AUTH}") as response:
                try:
                    body = await response.text()
                    mbta_api_requests.labels("stops").inc()
                    stop = Stop.model_validate_json(body, strict=False)
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
                except ValidationError as err:
                    logger.error("Unable to parse facility", exc_info=err)
            await write_cache(
                self.r_client,
                key,
                StopAndFacilities(stop=stop, facilities=facilities).model_dump_json(),
                randint(FOUR_WEEKS, TWO_MONTHS),
            )
        return stop, facilities


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
    transit_time_min: int,
) -> None:
    client = aiosseclient(endpoint, headers=headers)
    try:
        async for event in client:
            if (
                watcher.expiration_time
                and datetime.now().astimezone(UTC) > watcher.expiration_time
            ):
                await client.aclose()
                logger.info(
                    f"Restarting thread {watcher.watcher_type} - {watcher.stop_id}/{watcher.route}"
                )
                return
            await watcher.parse_live_api_response(
                event.data, event.event, queue, transit_time_min, session
            )
    except GeneratorExit:
        return


@retry(
    wait=wait_random_exponential(multiplier=1),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def watch_static_schedule(
    stop_id: str,
    route: str | None,
    direction: int | None,
    queue: Queue[ScheduleEvent | VehicleRedisSchema],
    transit_time_min: int,
) -> None:
    while True:
        watcher = Watcher(
            stop_id=stop_id,
            route=route,
            direction_filter=direction,
            schedule_only=True,
            watcher_type=EventType.SCHEDULES,
        )
        async with aiohttp.ClientSession(MBTA_V3_ENDPOINT) as session:
            await watcher.save_own_stop(session)
            await watcher.save_schedule(transit_time_min, queue, session)
        await sleep(10800)  # 3 hours


@retry(
    wait=wait_random_exponential(multiplier=1),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def watch_vehicles(
    queue: Queue[ScheduleEvent | VehicleRedisSchema],
    expiration_time: Optional[datetime],
    route_id: str,
) -> None:
    endpoint = f"{MBTA_V3_ENDPOINT}/vehicles?fields[vehicle]=direction_id,latitude,longitude,speed,current_status,occupancy_status,carriages&filter[route]={route_id}&api_key={MBTA_AUTH}"
    mbta_api_requests.labels("vehicles").inc()
    headers = {"accept": "text/event-stream"}
    watcher = Watcher(
        route=route_id,
        watcher_type=EventType.VEHICLES,
        expiration_time=expiration_time,
    )
    async with aiohttp.ClientSession(MBTA_V3_ENDPOINT) as session:
        tracker_executions.labels("vehicles").inc()
        await watch_server_side_events(
            watcher, endpoint, headers, queue, session=session, transit_time_min=0
        )


async def watch_station(
    stop_id: str,
    route: str | None,
    direction_filter: Optional[int],
    queue: Queue[ScheduleEvent | VehicleRedisSchema],
    transit_time_min: int,
    expiration_time: Optional[datetime],
) -> None:
    endpoint = (
        f"{MBTA_V3_ENDPOINT}/predictions?filter[stop]={stop_id}&api_key={MBTA_AUTH}"
    )
    mbta_api_requests.labels("predictions").inc()
    if route != "":
        endpoint += f"&filter[route]={route}"
    if direction_filter != "":
        endpoint += f"&filter[direction_id]={direction_filter}"
    headers = {"accept": "text/event-stream"}
    watcher = Watcher(
        stop_id,
        route,
        direction_filter,
        expiration_time,
        watcher_type=EventType.PREDICTIONS,
    )
    async with aiohttp.ClientSession(MBTA_V3_ENDPOINT) as session:
        await watcher.save_own_stop(session)
        if watcher.stop:
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
    direction_filter: Optional[int] = None,
    expiration_time: Optional[datetime] = None,
) -> None:
    with Runner() as runner:
        match target:
            case EventType.SCHEDULES:
                if stop_id:
                    runner.run(
                        watch_static_schedule(
                            stop_id,
                            route,
                            direction_filter,
                            queue,
                            transit_time_min,
                        )
                    )
            case EventType.PREDICTIONS:
                runner.run(
                    watch_station(
                        stop_id or "place-sstat",
                        route,
                        direction_filter,
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
                        route or "Red",
                    )
                )
