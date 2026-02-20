import logging
import os
from asyncio import CancelledError
from collections import Counter
from datetime import UTC, datetime, timedelta
from random import randint
from types import TracebackType
from typing import Optional
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import ClientSession
from anyio import sleep
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectSendStream
from config import Config
from consts import DAY, HOUR, MINUTE, TWO_MONTHS, WEEK, YEAR
from exceptions import RateLimitExceeded
from mbta_client_extended import silver_line_lookup
from mbta_responses import (
    AlertResource,
    Facilities,
    PredictionAttributes,
    PredictionResource,
    Route,
    RouteResource,
    ScheduleAttributes,
    ScheduleResource,
    Schedules,
    Stop,
    StopAndFacilities,
    TripResource,
    Trips,
    TypeAndID,
    Vehicle,
)
from opentelemetry.trace import Span
from otel_config import get_tracer, is_otel_enabled
from otel_utils import (
    add_transaction_ids_to_span,
    should_trace_operation,
)
from prometheus import (
    mbta_api_requests,
    query_server_side_events,
)
from pydantic import TypeAdapter, ValidationError
from redis.asyncio.client import Redis
from redis.exceptions import RedisError
from redis_cache import check_cache, write_cache
from schedule_tracker import ScheduleEvent, VehicleRedisSchema, dummy_schedule_event
from shared_types.shared_types import TaskType
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_not_exception_type,
    wait_exponential_jitter,
)
from webhook.discord_webhook import delete_webhook, process_alert_event

MBTA_AUTH = os.environ.get("AUTH_TOKEN")
logger = logging.getLogger(__name__)
SHAPE_POLYLINES = set[str]()


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
        # Suppress noisy traces on cooperative cancellation
        if isinstance(exc_value, CancelledError):
            logger.info(f"{self.watcher_type} cancelled; exiting cleanly")
            return True
        if exc_value:
            # Log unexpected errors with stack for investigation
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
        if self.watcher_type == TaskType.ALERTS:
            await sleep(6 * HOUR)
            tg.cancel_scope.cancel()
            return
        if self.route:
            if (
                "Red" in self.route
                or "Orange" in self.route
                or "Blue" in self.route
                or "Green" in self.route
                or "Mattapan" in self.route
                or self.route.startswith("7")
            ):
                hc_fail_threshold = 60
            if "CR" in self.route or self.route == "746":
                hc_fail_threshold = HOUR + (randint(0, 120) * 60)
        async with aiohttp.ClientSession() as session:
            logger.debug("started hc monitoring")
            await sleep(10 * MINUTE)
            while True:
                await sleep(randint(20, 90))
                now = datetime.now(ny_tz)
                if failtime and now >= failtime:
                    logger.info(
                        "Refreshing MBTA server side events due to health check failure/scheduled restart."
                    )
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
                                if len(result.value) > 0:
                                    val = float(result.value[1])
                                    if val <= 0.001:
                                        if not failtime:
                                            failtime = now + timedelta(
                                                seconds=hc_fail_threshold
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
        send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema] | None,
        transit_time_min: int,
        session: ClientSession,
        tg: TaskGroup,
        config: Config,
    ) -> None:
        # https://www.mbta.com/developers/v3-api/streaming
        try:
            # Handle Alerts stream separately
            if self.watcher_type == TaskType.ALERTS:
                if event_type == "reset":
                    ta = TypeAdapter(list[AlertResource])
                    alerts = ta.validate_json(data, strict=False)
                    # If filtering by a single route, reset that set first
                    if self.route:
                        await self.r_client.delete(f"alerts:route:{self.route}")
                    for a in alerts:
                        await process_alert_event(a, self.r_client, config, tg)
                        await write_cache(
                            self.r_client,
                            f"alert:{a.id}",
                            a.model_dump_json(),
                            2 * HOUR,
                        )
                        # Ensure membership in sets
                        if self.route:
                            await self.r_client.sadd(f"alerts:route:{self.route}", a.id)  # type: ignore
                        # Also map to any routes in informed_entity
                        if a.attributes and a.attributes.informed_entity:
                            for ent in a.attributes.informed_entity:
                                if ent.route:
                                    await self.r_client.sadd(
                                        f"alerts:route:{ent.route}", a.id
                                    )  # type: ignore[misc]
                                # Map to trip as well if present
                                if ent.trip:
                                    await self.r_client.sadd(
                                        f"alerts:trip:{ent.trip}", a.id
                                    )  # type: ignore[misc]
                                    await self.r_client.expire(
                                        f"alerts:trip:{ent.trip}", WEEK
                                    )
                    return
                elif event_type in ("add", "update"):
                    a = AlertResource.model_validate_json(data, strict=False)
                    await write_cache(
                        self.r_client, f"alert:{a.id}", a.model_dump_json(), 2 * HOUR
                    )
                    if self.route:
                        await self.r_client.sadd(f"alerts:route:{self.route}", a.id)  # type: ignore[misc]
                    await process_alert_event(a, self.r_client, config, tg)
                    if a.attributes and a.attributes.informed_entity:
                        for ent in a.attributes.informed_entity:
                            if ent.route:
                                await self.r_client.sadd(
                                    f"alerts:route:{ent.route}", a.id
                                )  # type: ignore[misc]
                            if ent.trip:
                                await self.r_client.sadd(
                                    f"alerts:trip:{ent.trip}", a.id
                                )  # type: ignore[misc]
                                await self.r_client.expire(
                                    f"alerts:trip:{ent.trip}", WEEK
                                )
                    return
                elif event_type == "remove":
                    type_and_id = TypeAndID.model_validate_json(data, strict=False)
                    # Remove from sets based on stored alert data, then delete key
                    try:
                        cached = await check_cache(
                            self.r_client, f"alert:{type_and_id.id}"
                        )
                        if cached:
                            try:
                                a = AlertResource.model_validate_json(
                                    cached, strict=False
                                )
                                await delete_webhook(type_and_id.id, self.r_client)
                                if a.attributes and a.attributes.informed_entity:
                                    for ent in a.attributes.informed_entity:
                                        if ent.route:
                                            await self.r_client.srem(
                                                f"alerts:route:{ent.route}",
                                                type_and_id.id,
                                            )  # type: ignore[misc]
                                        if ent.trip:
                                            await self.r_client.srem(
                                                f"alerts:trip:{ent.trip}",
                                                type_and_id.id,
                                            )  # type: ignore[misc]
                                            await self.r_client.expire(
                                                f"alerts:trip:{ent.trip}", WEEK
                                            )
                            except ValidationError:
                                pass
                    finally:
                        await self.r_client.delete(f"alert:{type_and_id.id}")  # type: ignore[misc]
                    return
                # For unknown event types, ignore
                return
            match event_type:
                case "reset":
                    if (
                        self.watcher_type == TaskType.SCHEDULE_PREDICTIONS
                        or self.watcher_type == TaskType.SCHEDULES
                    ):
                        ta = TypeAdapter(list[PredictionResource])
                        prediction_resource = ta.validate_json(data, strict=False)
                        if send_stream is not None:
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
                            if send_stream is not None:
                                tg.start_soon(
                                    self.save_schedule,
                                    transit_time_min,
                                    send_stream,
                                    session,
                                    tg,
                                    timedelta(hours=4),
                                )
                    else:
                        if send_stream is not None:
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
                        if send_stream is not None:
                            tg.start_soon(
                                self.queue_event,
                                PredictionResource.model_validate_json(
                                    data, strict=False
                                ),
                                "add",
                                send_stream,
                                session,
                                tg,
                                transit_time_min,
                            )
                    else:
                        if send_stream is not None:
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
                        if send_stream is not None:
                            tg.start_soon(
                                self.queue_event,
                                PredictionResource.model_validate_json(
                                    data, strict=False
                                ),
                                "update",
                                send_stream,
                                session,
                                tg,
                                transit_time_min,
                            )
                    else:
                        if send_stream is not None:
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
                        if send_stream is not None:
                            await send_stream.send(dummy_schedule_event(type_and_id.id))
                    elif self.route:
                        if send_stream is not None:
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
        wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
        retry=retry_if_not_exception_type(CancelledError),
    )
    async def get_trip(
        self, trip_id: str, session: ClientSession, tg: TaskGroup
    ) -> Optional[Trips]:
        tracer = get_tracer(__name__) if is_otel_enabled() else None
        if tracer and should_trace_operation("high_volume"):
            with tracer.start_as_current_span(
                "mbta_client.get_trip", attributes={"trip_id": trip_id}
            ) as span:
                # Add transaction IDs to the span
                add_transaction_ids_to_span(span)
                return await self._get_trip_impl(trip_id, session, tg, span)
        else:
            return await self._get_trip_impl(trip_id, session, tg, None)

    async def _get_trip_impl(
        self, trip_id: str, session: ClientSession, tg: TaskGroup, span: Optional[Span]
    ) -> Optional[Trips]:
        if session.closed:
            logger.debug("get_trip called with a closed session; skipping fetch")
            if span:
                span.set_attribute("session.closed", True)
            return None
        key = f"trip:{trip_id}:full"
        cached = await check_cache(self.r_client, key)
        try:
            if cached:
                if span:
                    span.set_attribute("cache.hit", True)
                trip = Trips.model_validate_json(cached, strict=False)
                return trip
            else:
                if span:
                    span.set_attribute("cache.hit", False)
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
            if span:
                span.set_attribute("error", True)
                span.set_attribute("error.type", "validation_error")
        return None

    # saves a route to the dict of routes rather than redis
    @retry(
        wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
        retry=retry_if_not_exception_type(CancelledError),
    )
    async def save_route(
        self, prediction: PredictionResource | ScheduleResource, session: ClientSession
    ) -> None:
        if session.closed:
            logger.debug("save_route called with a closed session; skipping fetch")
            return
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
        wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
        retry=retry_if_not_exception_type(CancelledError),
    )
    async def get_alerts(
        self,
        session: ClientSession,
        trip_id: Optional[str] = None,
        route_id: Optional[str] = None,
    ) -> Optional[list[AlertResource]]:
        tracer = get_tracer(__name__) if is_otel_enabled() else None
        if tracer and should_trace_operation("high_volume"):
            attrs = {}
            if trip_id:
                attrs["trip_id"] = trip_id
            if route_id:
                attrs["route_id"] = route_id
            with tracer.start_as_current_span(
                "mbta_client.get_alerts", attributes=attrs
            ) as span:
                return await self._get_alerts_impl(session, trip_id, route_id, span)
        else:
            return await self._get_alerts_impl(session, trip_id, route_id, None)

    async def _get_alerts_impl(
        self,
        session: ClientSession,
        trip_id: Optional[str],
        route_id: Optional[str],
        span: Optional[Span],
    ) -> Optional[list[AlertResource]]:
        # Read alerts from Redis sets populated by SSE watchers
        key: Optional[str] = None
        if trip_id:
            key = f"alerts:trip:{trip_id}"
        elif route_id:
            key = f"alerts:route:{route_id}"
        else:
            logging.error("you need to specify a trip_id or route_id to fetch alerts")
            if span:
                span.set_attribute("error", True)
                span.set_attribute("error.type", "missing_parameters")
            return None

        try:
            ids = await self.r_client.smembers(key)  # type: ignore[misc]
            if not ids:
                if span:
                    span.set_attribute("alerts.count", 0)
                return []
            # Fetch alert objects in a pipeline
            pl = self.r_client.pipeline()
            for raw in ids:
                alert_id = (
                    raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
                )
                pl.get(f"alert:{alert_id}")
            results = await pl.execute()
            ret: list[AlertResource] = []
            for raw in results:
                if not raw:
                    continue
                try:
                    alert = AlertResource.model_validate_json(raw, strict=False)
                    ret.append(alert)
                except ValidationError:
                    continue
            if span:
                span.set_attribute("alerts.count", len(ret))
            return ret
        except RedisError as e:
            logger.error("Redis error while loading alerts", exc_info=e)
            if span:
                span.set_attribute("error", True)
                span.set_attribute("error.type", "redis_error")
            return None
        except (ConnectionError, TimeoutError) as e:
            logger.error("Connection error while loading alerts", exc_info=e)
            if span:
                span.set_attribute("error", True)
                span.set_attribute("error.type", "connection_error")
            return None

    @retry(
        wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
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
        if session.closed:
            logger.debug("save_schedule called with a closed session; skipping fetch")
            return
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
        wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
        retry=retry_if_not_exception_type(CancelledError),
    )
    async def get_stop(
        self, session: ClientSession, stop_id: str, tg: Optional[TaskGroup] = None
    ) -> tuple[Optional[Stop], Optional[Facilities]]:
        tracer = get_tracer(__name__) if is_otel_enabled() else None
        if tracer and should_trace_operation("high_volume"):
            with tracer.start_as_current_span(
                "mbta_client.get_stop", attributes={"stop_id": stop_id}
            ) as span:
                return await self._get_stop_impl(session, stop_id, tg, span)
        else:
            return await self._get_stop_impl(session, stop_id, tg, None)

    async def _get_stop_impl(
        self,
        session: ClientSession,
        stop_id: str,
        tg: Optional[TaskGroup],
        span: Optional[Span],
    ) -> tuple[Optional[Stop], Optional[Facilities]]:
        if session.closed:
            logger.debug("get_stop called with a closed session; skipping fetch")
            if span:
                span.set_attribute("session.closed", True)
            return None, None
        key = f"stop:{stop_id}:full"
        stop = None
        facilities = None
        cached = await check_cache(self.r_client, key)
        if cached:
            if span:
                span.set_attribute("cache.hit", True)
            try:
                s_and_f = StopAndFacilities.model_validate_json(cached)
                return s_and_f.stop, s_and_f.facilities
            except ValidationError as err:
                logger.error("validation err", exc_info=err)
                if span:
                    span.set_attribute("error", True)
                    span.set_attribute("error.type", "validation_error")
        else:
            if span:
                span.set_attribute("cache.hit", False)
            async with session.get(f"/stops/{stop_id}?api_key={MBTA_AUTH}") as response:
                try:
                    if response.status == 429:
                        raise RateLimitExceeded()
                    if response.status == 404:
                        if span:
                            span.set_attribute("error", True)
                            span.set_attribute("error.type", "not_found")
                        return None, None
                    body = await response.text()
                    mbta_api_requests.labels("stops").inc()
                    stop = Stop.model_validate_json(body, strict=False)
                except ValidationError as err:
                    logger.error("Unable to parse stop", exc_info=err)
                    if span:
                        span.set_attribute("error", True)
                        span.set_attribute("error.type", "validation_error")
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
