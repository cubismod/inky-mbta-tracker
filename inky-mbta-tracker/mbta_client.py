import logging
import os
from asyncio import CancelledError
from collections import Counter
from datetime import UTC, datetime, timedelta
from random import randint
from types import TracebackType
from typing import TYPE_CHECKING, Optional
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientPayloadError
from aiosseclient import aiosseclient
from anyio import create_task_group, sleep
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectSendStream
from consts import DAY, FOUR_WEEKS, HOUR, MBTA_V3_ENDPOINT, TWO_MONTHS, YEAR
from exceptions import RateLimitExceeded
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
from ollama_imt import OllamaClientIMT
from polyline import decode
from prometheus import (
    mbta_api_requests,
    query_server_side_events,
    server_side_events,
    tracker_executions,
)
from pydantic import BaseModel, TypeAdapter, ValidationError
from redis.asyncio.client import Redis
from redis_cache import check_cache, write_cache
from schedule_tracker import ScheduleEvent, VehicleRedisSchema, dummy_schedule_event
from shared_types.shared_types import (
    LineRoute,
    RouteShapes,
    TaskType,
    TrackAssignment,
    TrackAssignmentType,
)
from tenacity import (
    before_log,
    before_sleep_log,
    retry,
    retry_if_not_exception_type,
    wait_exponential_jitter,
)

if TYPE_CHECKING:
    from track_predictor.track_predictor import TrackPredictor

MBTA_AUTH = os.environ.get("AUTH_TOKEN")
logger = logging.getLogger(__name__)
SHAPE_POLYLINES = set[str]()


class LightStop(BaseModel):
    stop_id: str
    long: Optional[float] = None
    lat: Optional[float] = None
    platform_prediction: Optional[str] = None


def parse_shape_data(shapes: Shapes) -> LineRoute:
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
    wait=wait_exponential_jitter(initial=2, jitter=5, max=60),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def get_shapes(
    r_client: Redis,
    routes: list[str],
    session: ClientSession,
    tg: Optional[TaskGroup] = None,
) -> RouteShapes:
    ret = RouteShapes(lines={})
    for route in routes:
        key = f"shape:{route}"
        cached = await check_cache(r_client, key)
        body = ""
        if cached:
            body = cached
        else:
            async with session.get(
                f"/shapes?filter[route]={route}&api_key={MBTA_AUTH}"
            ) as response:
                if response.status == 429:
                    raise RateLimitExceeded()
                body = await response.text()
                mbta_api_requests.labels("shapes").inc()
                # 4 weeks
                if tg:
                    tg.start_soon(write_cache, r_client, key, body, 2419200)
                else:
                    await write_cache(r_client, key, body, 2419200)
        shapes = Shapes.model_validate_json(body, strict=False)
        ret.lines[route] = parse_shape_data(shapes)
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
async def light_get_stop(
    r_client: Redis,
    stop_id: str,
    session: ClientSession,
    tg: Optional[TaskGroup] = None,
) -> Optional[LightStop]:
    key = f"stop:{stop_id}:light"
    cached = await check_cache(r_client, key)
    if cached:
        try:
            cached_model = LightStop.model_validate_json(cached)
            return cached_model
        except ValidationError as err:
            logger.error("unable to validate json", exc_info=err)
    ls = None
    async with MBTAApi(
        r_client, stop_id=stop_id, watcher_type=TaskType.LIGHT_STOP
    ) as watcher:
        # avoid rate-limiting by spacing out requests
        await sleep(randint(1, 3))
        stop = await watcher.get_stop(session, stop_id, tg)
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
            if tg:
                tg.start_soon(
                    write_cache,
                    r_client,
                    key,
                    ls.model_dump_json(),
                    randint(FOUR_WEEKS, TWO_MONTHS),
                )
            else:
                await write_cache(
                    r_client, key, ls.model_dump_json(), randint(FOUR_WEEKS, TWO_MONTHS)
                )
    return ls


async def light_get_alerts(
    route_id: str, session: ClientSession, r_client: Redis
) -> Optional[list[AlertResource]]:
    async with MBTAApi(
        r_client, route=route_id, watcher_type=TaskType.VEHICLES
    ) as watcher:
        alerts = await watcher.get_alerts(session, route_id=route_id)
        if alerts:
            return alerts
    return None


class MBTAApi:
    """
    MBTA API client

    Implements a limited set of functionality from the MBTA v3 API.
    Focused primarily around real-time predictions of vehicles and schedules however this
    can also be used as a general API client. Utilizes Redis for caching.
    """

    watcher_type: TaskType
    stop_id: Optional[str]
    route: Optional[str]
    direction_filter: Optional[int]
    routes: dict[str, RouteResource]
    stop: Optional[Stop] = None
    schedule_only: bool = False
    facilities: Optional[Facilities]
    expiration_time: Optional[datetime]
    r_client: Redis
    track_predictor: "TrackPredictor"
    show_on_display: bool = True
    route_substring_filter: Optional[str] = None

    def __init__(
        self,
        r_client: Redis,
        stop_id: Optional[str] = None,
        route: Optional[str] = None,
        direction_filter: Optional[int] = None,
        expiration_time: Optional[datetime] = None,
        schedule_only: bool = False,
        watcher_type: TaskType = TaskType.SCHEDULE_PREDICTIONS,
        show_on_display: bool = True,
        route_substring_filter: Optional[str] = None,
    ):
        self.stop_id = stop_id
        self.route = route
        self.direction_filter = direction_filter
        self.routes = dict()
        self.expiration_time = expiration_time
        self.watcher_type = watcher_type
        self.r_client = r_client
        from track_predictor.track_predictor import TrackPredictor

        self.track_predictor = TrackPredictor(r_client)
        self.show_on_display = show_on_display
        self.route_substring_filter = route_substring_filter

        if (
            stop_id
            or route
            or direction_filter
            or expiration_time
            or schedule_only
            or route_substring_filter
        ):
            logger.debug(
                f"init MBTAApi {self.watcher_type} with {stop_id=} {route=} {direction_filter=} {expiration_time=} {schedule_only=} {route_substring_filter=}"
            )
        else:
            logger.debug(f"init MBTAApi {self.watcher_type}")

        self.schedule_only = schedule_only

    async def __aenter__(self) -> "MBTAApi":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[BaseException],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ):
        logging.debug(f"Closing MBTAApi {self.watcher_type} {exc_type}")
        if exc_value:
            logger.error(
                f"Error in MBTAApi {exc_type}\n{traceback}", exc_info=exc_value
            )
            return True
        return False

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

    def gen_unique_id(self):
        return f"{self.watcher_type}{self.stop_id or ''}{self.route or ''}".lower()

    def get_service_status(self) -> bool:
        """
        Determines the service status of a route based on the current time and route type.
        Uses https://cdn.mbta.com/sites/default/files/media/route_pdfs/SUB-S4-P4-C.pdf as a reference
        """
        ny_tz = ZoneInfo("America/New_York")
        now = datetime.now(ny_tz)
        weekday = now.isoweekday()
        service_start = "05:30"
        service_end = "23:30"
        if self.route:
            if "Red" in self.route:
                service_start = "05:15"
                service_end = "01:30"
                if weekday == 5 or weekday == 6:
                    service_end = "02:30"
                if weekday == 7:
                    service_start = "05:45"
            if "Orange" in self.route:
                service_start = "05:15"
                service_end = "00:30"
                if weekday == 5 or weekday == 6:
                    service_end = "01:29"
                if weekday == 7:
                    service_start = "06:00"
            if "Green" in self.route:
                service_start = "04:45"
                service_end = "00:57"
                if weekday == 5 or weekday == 6:
                    service_end = "01:53"
                if weekday == 7:
                    service_start = "05:15"
                    service_end = "05:55"
            if "Blue" in self.route:
                service_start = "05:08"
                service_end = "00:52"
                if weekday == 5 or weekday == 6:
                    service_end = "01:51"
                if weekday == 7:
                    service_start = "06:00"
                    service_end = "00:52"
            if "SL" in silver_line_lookup(self.route):
                service_start = "04:21"
                service_end = "01:18"
                if weekday == 5 or weekday == 6:
                    service_end = "02:16"
                if weekday == 7:
                    service_start = "05:36"
                    service_end = "01:19"

        service_start_time = (
            datetime.strptime(service_start, "%H:%M").astimezone(ny_tz).time()
        )
        service_end_time = (
            datetime.strptime(service_end, "%H:%M").astimezone(ny_tz).time()
        )

        if now.time() <= service_end_time:
            return True

        if now.time() <= service_end_time or now.time() >= service_start_time:
            logger.debug("in service")
            return True
        return False

    async def _monitor_health(self, tg: TaskGroup) -> None:
        hc_fail_threshold = 2 * HOUR
        ny_tz = ZoneInfo("America/New_York")
        failtime: Optional[datetime] = None
        if self.route:
            if (
                "Red" in self.route
                or "Orange" in self.route
                or "Blue" in self.route
                or "Green" in self.route
                or "Mattapan" in self.route
            ):
                hc_fail_threshold = 5 * 60
        async with aiohttp.ClientSession() as session:
            logger.debug("started hc monitoring")
            while True:
                await sleep(randint(60, 180))
                now = datetime.now(ny_tz)
                if failtime and now >= failtime:
                    logger.info("Refreshing MBTA server side events")
                    tg.cancel_scope.cancel()
                    return
                if self.get_service_status():
                    prom_resp = await query_server_side_events(
                        session,
                        os.getenv("IMT_PROMETHEUS_JOB", "imt"),
                        self.gen_unique_id(),
                    )
                    if prom_resp:
                        results = prom_resp.data.result
                        if results:
                            for result in results:
                                val = result.value[0]
                                if isinstance(val, float) and val <= 0.001:
                                    if not failtime:
                                        failtime = now + timedelta(
                                            seconds=hc_fail_threshold
                                        )
                                        logger.warning(
                                            f"Detected a potentially stuck SSE worker, will restart at {failtime.isoformat()}"
                                        )
                                else:
                                    failtime = None

    async def get_headsign(
        self, trip_id: str, session: ClientSession, tg: TaskGroup
    ) -> str:
        hs = ""
        trip = await self.get_trip(trip_id, session, tg)
        if trip and len(trip.data) > 0:
            hs = trip.data[0].attributes.headsign
            if trip.data[0].attributes.revenue_status == "NON_REVENUE":
                return f"[NR] ${hs}"
        return hs

    async def parse_live_api_response(
        self,
        data: str,
        event_type: str,
        send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
        transit_time_min: int,
        session: ClientSession,
        tg: TaskGroup,
    ) -> None:
        # https://www.mbta.com/developers/v3-api/streaming
        try:
            match event_type:
                case "reset":
                    if (
                        self.watcher_type == TaskType.SCHEDULE_PREDICTIONS
                        or self.watcher_type == TaskType.SCHEDULES
                    ):
                        ta = TypeAdapter(list[PredictionResource])
                        prediction_resource = ta.validate_json(data, strict=False)
                        for item in prediction_resource:
                            tg.start_soon(
                                self.queue_event,
                                item,
                                "reset",
                                send_stream,
                                session,
                                tg,
                                transit_time_min,
                            )
                        if len(prediction_resource) == 0:
                            tg.start_soon(
                                self.save_schedule,
                                transit_time_min,
                                send_stream,
                                session,
                                tg,
                                timedelta(hours=4),
                            )
                    else:
                        ta = TypeAdapter(list[Vehicle])
                        vehicles = ta.validate_json(data, strict=False)
                        for v in vehicles:
                            tg.start_soon(
                                self.queue_event,
                                v,
                                event_type,
                                send_stream,
                                session,
                                tg,
                            )

                case "add":
                    if (
                        self.watcher_type == TaskType.SCHEDULE_PREDICTIONS
                        or self.watcher_type == TaskType.SCHEDULES
                    ):
                        tg.start_soon(
                            self.queue_event,
                            PredictionResource.model_validate_json(data, strict=False),
                            "add",
                            send_stream,
                            session,
                            tg,
                            transit_time_min,
                        )
                    else:
                        vehicle = Vehicle.model_validate_json(data, strict=False)
                        tg.start_soon(
                            self.queue_event,
                            vehicle,
                            event_type,
                            send_stream,
                            session,
                            tg,
                        )
                case "update":
                    if (
                        self.watcher_type == TaskType.SCHEDULE_PREDICTIONS
                        or self.watcher_type == TaskType.SCHEDULES
                    ):
                        tg.start_soon(
                            self.queue_event,
                            PredictionResource.model_validate_json(data, strict=False),
                            "update",
                            send_stream,
                            session,
                            tg,
                            transit_time_min,
                        )
                    else:
                        vehicle = Vehicle.model_validate_json(data, strict=False)
                        tg.start_soon(
                            self.queue_event,
                            vehicle,
                            event_type,
                            send_stream,
                            session,
                            tg,
                        )
                case "remove":
                    type_and_id = TypeAndID.model_validate_json(data, strict=False)
                    # directly interact with the queue here to use a dummy object
                    if (
                        self.watcher_type == TaskType.SCHEDULE_PREDICTIONS
                        or self.watcher_type == TaskType.SCHEDULES
                    ):
                        await send_stream.send(dummy_schedule_event(type_and_id.id))
                    elif self.route:
                        await send_stream.send(
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
    def get_carriages(vehicle: Vehicle) -> tuple[list[str], str]:
        carriages = list[str]()
        if vehicle.attributes.carriages:
            statuses = list[str]()
            for carriage in vehicle.attributes.carriages:
                if carriage.label:
                    carriages.append(carriage.label)
                if (
                    carriage.occupancy_status
                    and carriage.occupancy_status != "NO_DATA_AVAILABLE"
                ):
                    statuses.append(carriage.occupancy_status)
            if len(statuses) > 0:
                count = Counter(statuses)
                return carriages, count.most_common(1)[0][0]
        return carriages, ""

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
        send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
        session: ClientSession,
        tg: TaskGroup,
        transit_time_min: Optional[int] = None,
    ) -> None:
        if isinstance(item, PredictionResource) or isinstance(item, ScheduleResource):
            schedule_time = self.determine_time(item.attributes)
            if (
                self.route_substring_filter
                and item.relationships
                and item.relationships.route
                and item.relationships.route.data
                and self.route_substring_filter not in item.relationships.route.data.id
            ):
                return
            if schedule_time and schedule_time > datetime.now().astimezone(UTC):
                await self.save_route(item, session)
                if (
                    item.relationships.route
                    and item.relationships.route.data
                    and item.relationships.trip.data
                ):
                    route_id = item.relationships.route.data.id
                    headsign = await self.get_headsign(
                        item.relationships.trip.data.id, session, tg
                    )

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
                        trip = await self.get_trip(trip_id, session, tg)
                        if trip and len(trip.data) > 0:
                            bikes_allowed = self.bikes_allowed(trip.data[0])
                            alerting = await self.get_alerting_state(trip_id, session)
                    stop_name = ""
                    if self.stop:
                        stop_name = self.stop.data.attributes.name

                    # Track prediction integration
                    track_number = None
                    track_confidence = None

                    # Get detailed stop information for track data
                    if item.relationships.stop and item.relationships.stop.data:
                        stop_data = await self.get_stop(
                            session, item.relationships.stop.data.id, tg
                        )
                        if stop_data[0]:  # stop_data is a tuple (Stop, Facilities)
                            stop_info = stop_data[0]
                            track_number = stop_info.data.attributes.platform_code

                            station_id, has_track_predictions = determine_station_id(
                                self.stop_id or item.relationships.stop.data.id
                            )

                            # For commuter rail, store historical assignment and generate prediction
                            if (
                                route_id.startswith("CR")
                                and track_number
                                and has_track_predictions
                            ):
                                # Store historical track assignment
                                try:
                                    assignment = TrackAssignment(
                                        station_id=station_id,
                                        route_id=route_id,
                                        trip_id=trip_id,
                                        headsign=headsign,
                                        direction_id=item.attributes.direction_id,
                                        assignment_type=TrackAssignmentType.HISTORICAL,
                                        track_number=track_number,
                                        scheduled_time=schedule_time,
                                        actual_time=schedule_time,  # For predictions, use scheduled time
                                        recorded_time=datetime.now(UTC),
                                        day_of_week=schedule_time.weekday(),
                                        hour=schedule_time.hour,
                                        minute=schedule_time.minute,
                                    )

                                    tg.start_soon(
                                        self.track_predictor.store_historical_assignment,
                                        assignment,
                                        tg,
                                    )

                                    # Validate previous predictions
                                    tg.start_soon(
                                        self.track_predictor.validate_prediction,
                                        assignment.station_id,
                                        route_id,
                                        trip_id,
                                        schedule_time,
                                        track_number,
                                        tg,
                                    )

                                except (ConnectionError, TimeoutError) as e:
                                    logger.error(
                                        f"Failed to store historical assignment due to Redis connection issue: {e}",
                                        exc_info=True,
                                    )
                                except ValidationError as e:
                                    logger.error(
                                        f"Failed to store historical assignment due to validation error: {e}",
                                        exc_info=True,
                                    )

                            # Generate prediction for future trips (only for commuter rail)
                            elif (
                                route_id.startswith("CR")
                                and not track_number
                                and has_track_predictions
                            ):
                                try:
                                    if self.track_predictor:
                                        tg.start_soon(
                                            self.track_predictor.predict_track,
                                            station_id,
                                            route_id,
                                            trip_id,
                                            headsign,
                                            item.attributes.direction_id,
                                            schedule_time,
                                            tg,
                                        )
                                except (ConnectionError, TimeoutError) as e:
                                    logger.error(
                                        f"Failed to generate track prediction due to connection issue: {e}",
                                        exc_info=True,
                                    )
                                except ValidationError as e:
                                    logger.error(
                                        f"Failed to generate track prediction due to validation error: {e}",
                                        exc_info=True,
                                    )
                    if self.stop and headsign == self.stop.data.attributes.name:
                        return
                    event = ScheduleEvent(
                        action=event_type,
                        time=schedule_time,
                        route_id=route_id,
                        route_type=route_type,
                        headsign=self.abbreviate(headsign),
                        id=item.id.replace("-", ":"),
                        stop=self.abbreviate(stop_name),
                        transit_time_min=transit_time_min or 1,
                        trip_id=trip_id,
                        alerting=alerting,
                        bikes_allowed=bikes_allowed,
                        track_number=track_number,
                        track_confidence=track_confidence,
                        show_on_display=self.show_on_display,
                    )
                    await send_stream.send(event)
        else:
            occupancy = item.attributes.occupancy_status
            carriage_ids = list[str]()
            if not occupancy:
                carriage_ids, occupancy = self.get_carriages(item)
            if occupancy:
                occupancy = self.occupancy_status_human_readable(occupancy)
            route = ""
            trip_id = item.id
            trip_info = None
            if (
                item.relationships
                and item.relationships.route
                and item.relationships.route.data
            ):
                route = item.relationships.route.data.id
            if (
                item.relationships
                and item.relationships.trip
                and item.relationships.trip.data
            ):
                # save the trip name as this is what the T uses to refer to specific trains on commuter rail
                trip_info = await self.get_trip(
                    item.relationships.trip.data.id, session, tg
                )
                if (
                    trip_info
                    and "CR" in route
                    and len(trip_info.data) > 0
                    and trip_info.data[0].attributes.name != ""
                ):
                    trip_id = trip_info.data[0].attributes.name
            headsign = None
            if trip_info and len(trip_info.data) > 0:
                headsign = trip_info.data[0].attributes.headsign
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
                headsign=headsign,
            )
            if (
                item.relationships
                and item.relationships.stop
                and item.relationships.stop.data
            ):
                event.stop = item.relationships.stop.data.id
            if len(carriage_ids) > 0 and isinstance(event, VehicleRedisSchema):
                event.carriages = carriage_ids
            redis_vehicle_id = f"vehicle:{trip_id}"
            tg.start_soon(
                write_cache,
                self.r_client,
                redis_vehicle_id,
                event.model_dump_json(),
                10,
            )
            await send_stream.send(event)

    @retry(
        wait=wait_exponential_jitter(initial=2, jitter=5, max=60),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
        retry=retry_if_not_exception_type(CancelledError),
    )
    async def get_trip(
        self, trip_id: str, session: ClientSession, tg: TaskGroup
    ) -> Optional[Trips]:
        key = f"trip:{trip_id}:full"
        cached = await check_cache(self.r_client, key)
        try:
            if cached:
                trip = Trips.model_validate_json(cached, strict=False)
                return trip
            else:
                async with session.get(
                    f"trips?filter[id]={trip_id}&api_key={MBTA_AUTH}"
                ) as response:
                    if response.status == 429:
                        raise RateLimitExceeded()
                    body = await response.text()
                    mbta_api_requests.labels("trips").inc()

                    trip = Trips.model_validate_json(body, strict=False)
                    tg.start_soon(
                        write_cache, self.r_client, key, trip.model_dump_json(), DAY
                    )
                    return trip
        except ValidationError as err:
            logger.error("Unable to parse trip", exc_info=err)
        return None

    # saves a route to the dict of routes rather than redis
    @retry(
        wait=wait_exponential_jitter(initial=2, jitter=5, max=60),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
        retry=retry_if_not_exception_type(CancelledError),
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
                        if response.status == 429:
                            raise RateLimitExceeded()
                        body = await response.text()
                        mbta_api_requests.labels("routes").inc()
                        route = Route.model_validate_json(body, strict=False)
                        for rd in route.data:
                            logger.info(f"route {rd.id} saved")
                            self.routes[route_id] = rd
                    except ValidationError as err:
                        logger.error("Unable to parse route", exc_info=err)

    @retry(
        wait=wait_exponential_jitter(initial=2, jitter=5, max=60),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
        retry=retry_if_not_exception_type(CancelledError),
    )
    async def get_alerts(
        self,
        session: ClientSession,
        trip_id: Optional[str] = None,
        route_id: Optional[str] = None,
    ) -> Optional[list[AlertResource]]:
        endpoint = "/alerts"
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
                if response.status == 429:
                    raise RateLimitExceeded()
                body = await response.text()
                mbta_api_requests.labels("alerts").inc()
                alerts = Alerts.model_validate_json(strict=False, json_data=body)
                for alert in alerts.data:
                    # append an AI summary to the alert if available, otherwise queue one
                    # to be appended with the next alerts refresh
                    async with OllamaClientIMT(r_client=self.r_client) as ollama:
                        resp = await ollama.fetch_cached_summary(alert)
                        if resp:
                            alert.ai_summary = resp
                return alerts.data
            except ValidationError as err:
                logger.error("Unable to parse alert", exc_info=err)
                logger.debug(f"Alert body: {body}")
        return None

    @retry(
        wait=wait_exponential_jitter(initial=2, jitter=5, max=60),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
        retry=retry_if_not_exception_type(CancelledError),
    )
    async def save_schedule(
        self,
        transit_time_min: int,
        send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
        session: ClientSession,
        tg: TaskGroup,
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
                if response.status == 429:
                    raise RateLimitExceeded()
                body = await response.text()
                mbta_api_requests.labels("schedules").inc()
                schedules = Schedules.model_validate_json(strict=False, json_data=body)

                for item in schedules.data:
                    tg.start_soon(self.save_route, item, session)
                    tg.start_soon(
                        self.queue_event,
                        item,
                        "reset",
                        send_stream,
                        session,
                        tg,
                        transit_time_min,
                    )
            except ValidationError as err:
                logger.error("Unable to parse schedule", exc_info=err)

    async def save_own_stop(self, session: ClientSession, tg: TaskGroup) -> None:
        if self.stop_id:
            stop_and_facilities = await self.get_stop(session, self.stop_id, tg)
            self.stop = stop_and_facilities[0]
            self.facilities = stop_and_facilities[1]

    # 3 weeks of caching in redis as maybe a stop will change? idk
    @retry(
        wait=wait_exponential_jitter(initial=2, jitter=5, max=60),
        before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
        retry=retry_if_not_exception_type(CancelledError),
    )
    async def get_stop(
        self, session: ClientSession, stop_id: str, tg: Optional[TaskGroup] = None
    ) -> tuple[Optional[Stop], Optional[Facilities]]:
        key = f"stop:{stop_id}:full"
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
                    if response.status == 429:
                        raise RateLimitExceeded()
                    if response.status == 404:
                        return None, None
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
                    if response.status == 429:
                        raise RateLimitExceeded()
                    body = await response.text()
                    mbta_api_requests.labels("facilities").inc()
                    facilities = Facilities.model_validate_json(body, strict=False)
                except ValidationError as err:
                    logger.error("Unable to parse facility", exc_info=err)
            if stop:
                if tg:
                    tg.start_soon(
                        write_cache,
                        self.r_client,
                        key,
                        StopAndFacilities(
                            stop=stop, facilities=facilities
                        ).model_dump_json(),
                        randint(TWO_MONTHS, YEAR),
                    )
                else:
                    await write_cache(
                        self.r_client,
                        key,
                        StopAndFacilities(
                            stop=stop, facilities=facilities
                        ).model_dump_json(),
                        randint(TWO_MONTHS, YEAR),
                    )
        return stop, facilities

    async def _precache_track_predictions(
        self,
        tg: TaskGroup,
        routes: Optional[list[str]] = None,
        target_stations: Optional[list[str]] = None,
    ) -> int:
        """
        Precache track predictions for upcoming trips using the track predictor.

        Args:
            routes: List of route IDs to precache (defaults to all CR routes)
            target_stations: List of station IDs to precache for (defaults to supported stations)

        Returns:
            Number of predictions cached
        """
        if self.track_predictor:
            return await self.track_predictor.precache(
                routes=routes, target_stations=target_stations, tg=tg
            )
        else:
            logger.warning("Track predictor not available for precaching")
            return 0


async def precache_track_predictions_runner(
    r_client: Redis,
    tg: TaskGroup,
    routes: Optional[list[str]] = None,
    target_stations: Optional[list[str]] = None,
    interval_hours: int = 8,
) -> None:
    """
    Run track prediction precaching at regular intervals.

    Args:
        routes: List of route IDs to precache (defaults to all CR routes)
        target_stations: List of station IDs to precache for (defaults to supported stations)
        interval_hours: Hours between precaching runs (default: 2)
    """
    logger.info(
        f"Starting track prediction precaching runner (interval: {interval_hours}h)"
    )

    while True:
        try:
            async with MBTAApi(
                r_client, watcher_type=TaskType.TRACK_PREDICTIONS
            ) as api:
                await api._precache_track_predictions(
                    tg=tg,
                    routes=routes,
                    target_stations=target_stations,
                )

        except CancelledError:
            logger.info("Track prediction precache runner cancelled")
            break
        except Exception as e:
            logger.error("Error in track prediction precaching runner", exc_info=e)

        # Wait for the specified interval
        await sleep(interval_hours * HOUR)


@retry(
    wait=wait_exponential_jitter(initial=2, jitter=5, max=60),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def watch_mbta_server_side_events(
    watcher: MBTAApi,
    endpoint: str,
    headers: dict[str, str],
    send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
    session: ClientSession,
    transit_time_min: int,
) -> None:
    while True:
        async with create_task_group() as tg:
            client = aiosseclient(endpoint, headers=headers)
            if os.getenv("IMT_PROMETHEUS_ENDPOINT"):
                tg.start_soon(watcher._monitor_health, tg)
            try:
                async for event in client:
                    server_side_events.labels(watcher.gen_unique_id()).inc()
                    tg.start_soon(
                        watcher.parse_live_api_response,
                        event.data,
                        event.event,
                        send_stream,
                        transit_time_min,
                        session,
                        tg,
                    )
            except ClientPayloadError as err:
                logging.error("Error processing response", exc_info=err)
            except GeneratorExit as e:
                logger.error(
                    "GeneratorExit in watch_mbta_server_side_events", exc_info=e
                )
                return


@retry(
    wait=wait_exponential_jitter(initial=2, jitter=5, max=60),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def watch_static_schedule(
    r_client: Redis,
    stop_id: str,
    route: str | None,
    direction: int | None,
    send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
    transit_time_min: int,
    show_on_display: bool,
    tg: TaskGroup,
    route_substring_filter: Optional[str] = None,
) -> None:
    if route_substring_filter:
        logger.info(
            f"Watching station {stop_id} for route substring filter {route_substring_filter}"
        )
    refresh_time = datetime.now().astimezone(UTC) - timedelta(minutes=10)
    while True:
        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            if datetime.now().astimezone(UTC) > refresh_time:
                async with MBTAApi(
                    r_client,
                    stop_id=stop_id,
                    route=route,
                    direction_filter=direction,
                    schedule_only=True,
                    watcher_type=TaskType.SCHEDULES,
                    show_on_display=show_on_display,
                    route_substring_filter=route_substring_filter,
                ) as watcher:
                    await watcher.save_own_stop(session, tg)
                    await watcher.save_schedule(
                        transit_time_min, send_stream, session, tg
                    )
                    refresh_time = datetime.now().astimezone(UTC) + timedelta(
                        hours=randint(2, 6)
                    )
            await sleep(10)


@retry(
    wait=wait_exponential_jitter(initial=2, jitter=5, max=60),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def watch_vehicles(
    r_client: Redis,
    send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
    expiration_time: Optional[datetime],
    route_id: str,
) -> None:
    endpoint = f"{MBTA_V3_ENDPOINT}/vehicles?fields[vehicle]=direction_id,latitude,longitude,speed,current_status,occupancy_status,carriages&filter[route]={route_id}&api_key={MBTA_AUTH}"
    mbta_api_requests.labels("vehicles").inc()
    headers = {"accept": "text/event-stream"}
    async with MBTAApi(
        r_client,
        route=route_id,
        watcher_type=TaskType.VEHICLES,
        expiration_time=expiration_time,
    ) as watcher:
        async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
            tracker_executions.labels("vehicles").inc()
            await watch_mbta_server_side_events(
                watcher,
                endpoint,
                headers,
                send_stream,
                session=session,
                transit_time_min=0,
            )


async def watch_station(
    r_client: Redis,
    stop_id: str,
    route: str | None,
    direction_filter: Optional[int],
    send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
    transit_time_min: int,
    expiration_time: Optional[datetime],
    show_on_display: bool,
    tg: TaskGroup,
    route_substring_filter: Optional[str] = None,
) -> None:
    if route_substring_filter:
        logger.info(
            f"Watching station {stop_id} for route substring filter {route_substring_filter}"
        )
    endpoint = (
        f"{MBTA_V3_ENDPOINT}/predictions?filter[stop]={stop_id}&api_key={MBTA_AUTH}"
    )

    mbta_api_requests.labels("predictions").inc()
    if route != "":
        endpoint += f"&filter[route]={route}"
    if direction_filter != "":
        endpoint += f"&filter[direction_id]={direction_filter}"
    headers = {"accept": "text/event-stream"}

    async with aiohttp.ClientSession(MBTA_V3_ENDPOINT) as session:
        async with MBTAApi(
            r_client,
            stop_id,
            route,
            direction_filter,
            expiration_time,
            watcher_type=TaskType.SCHEDULE_PREDICTIONS,
            show_on_display=show_on_display,
            route_substring_filter=route_substring_filter,
        ) as watcher:
            tg.start_soon(watcher.save_own_stop, session, tg)
            if watcher.stop:
                tracker_executions.labels(watcher.stop.data.attributes.name).inc()
            await watch_mbta_server_side_events(
                watcher,
                endpoint,
                headers,
                send_stream,
                session,
                transit_time_min,
            )


# takes a stop_id from the vehicle API and returns the station_id and if it is one of the stations that has track predictions
def determine_station_id(stop_id: str) -> tuple[str, bool]:
    if "North Station" in stop_id or "BNT" in stop_id or "place-north" in stop_id:
        return "place-north", True
    if "South Station" in stop_id or "NEC-2287" in stop_id or "place-sstat" in stop_id:
        return "place-sstat", True
    if "Back Bay" in stop_id or "NEC-1851" in stop_id or "place-bbsta" in stop_id:
        return "place-bbsta", True
    if "Ruggles" in stop_id or "NEC-2265" in stop_id or "place-rugg" in stop_id:
        return "place-rugg", True
    if "Providence" in stop_id or "NEC-1851" in stop_id:
        return "place-NEC-1851", True
    return stop_id, False
