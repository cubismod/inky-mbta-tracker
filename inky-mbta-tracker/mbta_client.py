import hashlib
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
from anyio import create_task_group, sleep
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectSendStream
from async_lru import alru_cache
from config import Config
from consts import (
    ALERTS_SET_KEY,
    DAY,
    HOUR,
    LIVE_NEGATIVE_CACHE_KEY,
    MINUTE,
    TWO_MONTHS,
    WEEK,
    YEAR,
)
from exceptions import RateLimitExceeded, WatcherRefreshRequested
from mbta_client_extended import silver_line_lookup
from mbta_rate_limiter import rate_limited_get
from mbta_responses import (
    AlertResource,
    Facilities,
    PredictionAttributes,
    PredictionResource,
    RouteResource,
    ScheduleAttributes,
    ScheduleResource,
    Schedules,
    Stop,
    StopAndFacilities,
    TripResource,
    Trips,
    TypeAndID,
    VehicleResource,
)
from opentelemetry.trace import Span
from otel_config import get_tracer, is_otel_enabled
from otel_utils import (
    add_entity_id_attribute,
    add_span_attributes,
    add_transaction_ids_to_span,
    set_span_error,
    should_trace_operation,
)
from prometheus import alerts_counter, mbta_api_requests
from pydantic import TypeAdapter, ValidationError
from redis.asyncio.client import Redis
from redis.exceptions import RedisError
from redis_cache import get_cache, write_cache
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


def occupancy_status_human_readable(occupancy: str) -> str:
    return occupancy.replace("_", " ").capitalize()


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
                "init MBTAApi %s with stop_id=%r route=%r direction_filter=%r "
                "expiration_time=%r schedule_only=%r route_substring_filter=%r",
                self.watcher_type,
                stop_id,
                route,
                direction_filter,
                expiration_time,
                schedule_only,
                route_substring_filter,
            )
        else:
            logger.debug("init MBTAApi %s", self.watcher_type)

        self.schedule_only = schedule_only

    async def __aenter__(self) -> "MBTAApi":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[BaseException],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ):
        logging.debug("Closing MBTAApi %s %s", self.watcher_type, exc_type)
        # Suppress noisy traces on cooperative cancellation
        if isinstance(exc_value, CancelledError):
            logger.info("%s cancelled; exiting cleanly", self.watcher_type)
            return True
        if exc_value:
            # Log unexpected errors with stack for investigation
            logger.error(
                "Error in MBTAApi %s\n%s", exc_type, traceback, exc_info=exc_value
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
        hc_fail_threshold = 5 * MINUTE
        ny_tz = ZoneInfo("America/New_York")
        failtime: Optional[datetime] = None
        max_runtime_expiration = None
        if self.watcher_type == TaskType.VEHICLES:
            # i do not trust the MBTA SSE API so they will regularly restart to ensure vehicle events stay consistent
            # includes a random offset to prevent an API thundering herd
            max_runtime_expiration = (
                datetime.now(UTC)
                + timedelta(hours=1)
                + timedelta(minutes=randint(0, 45))
            )
            if self.expiration_time:
                max_runtime_expiration = min(
                    max_runtime_expiration, self.expiration_time
                )
        if self.watcher_type == TaskType.ALERTS:
            hc_fail_threshold = 30 * MINUTE
            if self.route:
                hc_fail_threshold = 20 * MINUTE
            r = Redis(
                host=os.environ.get("IMT_REDIS_ENDPOINT") or "",
                port=int(os.environ.get("IMT_REDIS_PORT", "6379") or ""),
                password=os.environ.get("IMT_REDIS_PASSWORD") or "",
            )
            logger.debug("started alerts hc monitoring")
            await sleep(10 * MINUTE)
            while True:
                await sleep(randint(20, 90))
                now = datetime.now(ny_tz)
                if failtime and now >= failtime:
                    logger.info(
                        f"Refreshing alerts watcher (route={self.route}) due to health check failure/scheduled restart."
                    )
                    raise WatcherRefreshRequested
                heartbeat_key = "heartbeat:events:alerts"
                if self.route:
                    heartbeat_key = f"heartbeat:events:alerts:{self.route}"
                try:
                    heartbeat_value = await r.get(heartbeat_key)
                    if heartbeat_value:
                        heartbeat_time = datetime.fromisoformat(
                            heartbeat_value.decode("utf-8")
                        )
                        now_utc = datetime.now(UTC)
                        age_seconds = (now_utc - heartbeat_time).total_seconds()
                        if age_seconds > hc_fail_threshold:
                            if not failtime:
                                failtime = now + timedelta(seconds=hc_fail_threshold)
                    else:
                        if not failtime:
                            failtime = now + timedelta(seconds=hc_fail_threshold)
                except (ValueError, TypeError) as err:
                    logger.debug("Error reading alerts heartbeat: %s", err)
                    if not failtime:
                        failtime = now + timedelta(seconds=hc_fail_threshold)
        if self.route:
            if "CR" in self.route or self.route == "746":
                hc_fail_threshold = 90 * MINUTE
        r = Redis(
            host=os.environ.get("IMT_REDIS_ENDPOINT") or "",
            port=int(os.environ.get("IMT_REDIS_PORT", "6379") or ""),
            password=os.environ.get("IMT_REDIS_PASSWORD") or "",
        )
        logger.debug("started hc monitoring")
        await sleep(10 * MINUTE)
        while True:
            await sleep(randint(20, 90))
            now = datetime.now(ny_tz)
            if (
                self.watcher_type == TaskType.VEHICLES
                and max_runtime_expiration
                and datetime.now(UTC) >= max_runtime_expiration
            ):
                logger.info(
                    "Refreshing vehicle watcher (route=%s) due to scheduled restart.",
                    self.route,
                )
                raise WatcherRefreshRequested
            if failtime and now >= failtime:
                logger.info(
                    "Refreshing %s watcher (route=%s, stop=%s) due to health check failure/scheduled restart.",
                    self.watcher_type,
                    self.route,
                    self.stop_id,
                )
                raise WatcherRefreshRequested
            if self.get_service_status():
                heartbeat_key = f"heartbeat:events:{self.route}"
                try:
                    heartbeat_value = await r.get(heartbeat_key)
                    if heartbeat_value:
                        heartbeat_time = datetime.fromisoformat(
                            heartbeat_value.decode("utf-8")
                        )
                        now_utc = datetime.now(UTC)
                        age_seconds = (now_utc - heartbeat_time).total_seconds()
                        if age_seconds > hc_fail_threshold:
                            if not failtime:
                                failtime = now + timedelta(seconds=hc_fail_threshold)
                    else:
                        if not failtime:
                            failtime = now + timedelta(seconds=hc_fail_threshold)
                except (ValueError, TypeError) as err:
                    logger.debug("Error reading heartbeat: %s", err)
                    if not failtime:
                        failtime = now + timedelta(seconds=hc_fail_threshold)

    @alru_cache(maxsize=256)
    async def get_headsign(
        self,
        session: ClientSession,
        tg: TaskGroup,
        trip_id: Optional[str] = None,
        route_id: Optional[str] = None,
        direction_id: Optional[int] = None,
    ) -> str:
        hs = ""
        if trip_id:
            trip = await self.get_trip(trip_id, tg, session)
            if trip and len(trip.data) > 0:
                hs = trip.data[0].attributes.headsign
                if trip.data[0].attributes.revenue_status == "NON_REVENUE":
                    return f"[NR] ${hs}"
        if hs == "" and route_id and direction_id is not None:
            route = await self.get_route(route_id, self.r_client, session)
            if route:
                hs = (
                    route.attributes.direction_destinations[direction_id]
                    if route.attributes.direction_destinations
                    and len(route.attributes.direction_destinations) > direction_id
                    else ""
                )
        if hs == "":
            logger.warning(
                f"Could not determine headsign for trip {trip_id}, route/direction: {route_id}/{direction_id}"
            )
        return hs

    @staticmethod
    def _live_cache_key(data: str, event_type: str) -> str:
        return f"{event_type}:{hashlib.sha512(data.encode('utf-8')).hexdigest()}"

    async def _save_live_negative_cache(self, cache_key: str) -> None:
        ex_time = 15
        if self.watcher_type == TaskType.ALERTS:
            ex_time = HOUR
        await self.r_client.hsetex(LIVE_NEGATIVE_CACHE_KEY, cache_key, "", ex=ex_time)  # type: ignore[misc]

    async def _skip_live_negative_cache(self, cache_key: str) -> bool:
        return await self.r_client.hexists(LIVE_NEGATIVE_CACHE_KEY, cache_key)  # type: ignore[misc]

    def _parses_prediction_events(self) -> bool:
        return self.watcher_type in (
            TaskType.SCHEDULE_PREDICTIONS,
            TaskType.SCHEDULES,
        )

    def _parse_live_reset_items(
        self, data: str
    ) -> list[PredictionResource] | list[VehicleResource]:
        if self._parses_prediction_events():
            return TypeAdapter(list[PredictionResource]).validate_json(
                data, strict=False
            )
        return TypeAdapter(list[VehicleResource]).validate_json(data, strict=False)

    def _parse_live_item(self, data: str) -> PredictionResource | VehicleResource:
        if self._parses_prediction_events():
            return PredictionResource.model_validate_json(data, strict=False)
        return VehicleResource.model_validate_json(data, strict=False)

    async def _queue_item_event(
        self,
        item: PredictionResource | ScheduleResource | VehicleResource,
        event_type: str,
        send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
        session: ClientSession,
        transit_time_min: Optional[int] = None,
    ) -> None:
        async with create_task_group() as item_tg:
            await self.queue_event(
                item,
                event_type,
                send_stream,
                session,
                item_tg,
                transit_time_min,
            )

    async def _send_remove_event(
        self,
        type_and_id: TypeAndID,
        send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
    ) -> None:
        if self._parses_prediction_events():
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
                    bearing=0,
                )
            )

    async def _save_alert_memberships(self, alert: AlertResource) -> None:
        if self.route:
            await self.r_client.sadd(f"alerts:route:{self.route}", alert.id)  # type: ignore[misc]
        for ent in alert.attributes.informed_entity:
            if ent.route:
                await self.r_client.sadd(f"alerts:route:{ent.route}", alert.id)  # type: ignore[misc]
            if ent.trip:
                await self.r_client.sadd(f"alerts:trip:{ent.trip}", alert.id)  # type: ignore[misc]
                await self.r_client.expire(f"alerts:trip:{ent.trip}", WEEK)

    async def _process_alert_item(
        self,
        alert: AlertResource,
        event_type: str,
        config: Config,
    ) -> None:
        async with create_task_group() as item_tg:
            await write_cache(
                self.r_client,
                f"alert:{alert.id}",
                alert.model_dump_json(),
                2 * HOUR,
            )
            if event_type in ("reset", "add"):
                alerts_counter.labels(
                    route=self.route,
                    severity=alert.attributes.severity,
                    effect=alert.attributes.effect,
                ).inc()
            if event_type in ("add") and self.route:
                await self.r_client.hincrby(ALERTS_SET_KEY, self.route)  # type: ignore[misc]
            if (
                event_type == "reset"
                and self.route
                and datetime.now().astimezone(UTC)
                - datetime.fromisoformat(alert.attributes.updated_at).astimezone(UTC)
                < timedelta(minutes=4)
            ):
                await self.r_client.hincrby(ALERTS_SET_KEY, self.route)  # type: ignore[misc]
            await process_alert_event(alert, self.r_client, config, item_tg)
            await self._save_alert_memberships(alert)
            logger.debug("Alert: %s | %s", self.route, alert.attributes.header)

    async def _process_alert_remove(self, type_and_id: TypeAndID) -> None:
        try:
            cached = await get_cache(self.r_client, f"alert:{type_and_id.id}")
            if cached:
                try:
                    alert = AlertResource.model_validate_json(cached, strict=False)
                    await delete_webhook(type_and_id.id, self.r_client)
                    for ent in alert.attributes.informed_entity:
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
                            await self.r_client.expire(f"alerts:trip:{ent.trip}", WEEK)
                except ValidationError:
                    pass
        finally:
            await self.r_client.delete(f"alert:{type_and_id.id}")  # type: ignore[misc]

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
        if data != "[]":
            try:
                cache_key = self._live_cache_key(data, event_type)
                if await self._skip_live_negative_cache(cache_key):
                    logger.debug("Skipping live negative cache for data: %s", data)
                    return
                # Handle Alerts stream separately
                if self.watcher_type == TaskType.ALERTS:
                    if event_type == "reset":
                        alerts = TypeAdapter(list[AlertResource]).validate_json(
                            data, strict=False
                        )
                        # If filtering by a single route, reset that set first
                        if self.route:
                            await self.r_client.delete(f"alerts:route:{self.route}")
                        for alert in alerts:
                            tg.start_soon(
                                self._process_alert_item,
                                alert,
                                event_type,
                                config,
                            )
                        return
                    elif event_type in ("add", "update"):
                        alert = AlertResource.model_validate_json(data, strict=False)
                        tg.start_soon(
                            self._process_alert_item,
                            alert,
                            event_type,
                            config,
                        )
                        return
                    elif event_type == "remove":
                        type_and_id = TypeAndID.model_validate_json(data, strict=False)
                        # Remove from sets based on stored alert data, then delete key
                        tg.start_soon(self._process_alert_remove, type_and_id)
                        return
                    # For unknown event types, ignore
                    return
                match event_type:
                    case "reset":
                        items = self._parse_live_reset_items(data)
                        if send_stream is not None:
                            for item in items:
                                tg.start_soon(
                                    self._queue_item_event,
                                    item,
                                    event_type,
                                    send_stream,
                                    session,
                                    transit_time_min,
                                )
                            if self._parses_prediction_events() and len(items) == 0:
                                tg.start_soon(
                                    self.save_schedule,
                                    transit_time_min,
                                    send_stream,
                                    session,
                                    tg,
                                    timedelta(hours=4),
                                )

                    case "add" | "update":
                        item = self._parse_live_item(data)
                        if send_stream is not None:
                            tg.start_soon(
                                self._queue_item_event,
                                item,
                                event_type,
                                send_stream,
                                session,
                                transit_time_min,
                            )
                    case "remove":
                        type_and_id = TypeAndID.model_validate_json(data, strict=False)
                        # directly interact with the queue here to use a dummy object
                        if send_stream is not None:
                            tg.start_soon(
                                self._send_remove_event, type_and_id, send_stream
                            )
                tg.start_soon(self._save_live_negative_cache, cache_key)
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
        if speed is not None:
            return speed * 2.23693629
        return None

    @staticmethod
    def get_carriages(vehicle: VehicleResource) -> tuple[list[str], str]:
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
        item: PredictionResource | ScheduleResource | VehicleResource,
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
                if (
                    item.relationships.route
                    and item.relationships.route.data
                    and item.relationships.trip.data
                    and item.attributes.direction_id
                ):
                    route_id = item.relationships.route.data.id
                    headsign = await self.get_headsign(
                        session,
                        tg,
                        item.relationships.trip.data.id,
                        item.relationships.route.data.id,
                        item.attributes.direction_id,
                    )

                    if route_id.startswith("Green"):
                        branch = route_id[-1:]
                        headsign = f"{branch} - {headsign}"
                    if route_id.startswith("CR"):
                        line = route_id.split("-")
                        headsign = f"{headsign} - {line[1]} Ln"
                    route_data = self.routes.get(route_id)
                    if route_data is None:
                        route_data = await self.get_route(route_id, self.r_client, session)
                    if route_data is None:
                        return
                    route_type = route_data.attributes.type
                    alerting = False
                    bikes_allowed = False
                    trip_id = ""
                    if item.relationships.trip.data:
                        trip_id = item.relationships.trip.data.id
                        trip = await self.get_trip(trip_id, tg, session)
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
                occupancy = occupancy_status_human_readable(occupancy)
            route = ""
            vehicle_id = item.id
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
                # CR trains are referred to by the trip's short name; keep it on a
                # dedicated display field so the storage/dedup key stays the
                # vehicle id, matching the GTFS-rt VehiclePositions feed.
                trip_info = await self.get_trip(
                    item.relationships.trip.data.id, tg, session
                )
            trip_resource_id: str | None = None
            if (
                item.relationships
                and item.relationships.trip
                and item.relationships.trip.data
            ):
                trip_resource_id = item.relationships.trip.data.id
            short_name: str | None = None
            if (
                trip_info
                and "CR" in route
                and len(trip_info.data) > 0
                and trip_info.data[0].attributes.name != ""
            ):
                short_name = trip_info.data[0].attributes.name
            headsign = await self.get_headsign(
                session, tg, trip_resource_id, route, item.attributes.direction_id
            )
            event = VehicleRedisSchema(  # type: ignore
                action=event_type,
                id=vehicle_id,
                current_status=item.attributes.current_status,
                direction_id=item.attributes.direction_id,
                latitude=item.attributes.latitude,
                longitude=item.attributes.longitude,
                speed=self.meters_per_second_to_mph(item.attributes.speed),
                bearing=item.attributes.bearing,
                route=route,
                update_time=datetime.now().astimezone(UTC),
                occupancy_status=occupancy,
                headsign=headsign,
                trip_id=trip_resource_id,
                short_name=short_name,
            )
            if (
                item.relationships
                and item.relationships.stop
                and item.relationships.stop.data
            ):
                event.stop = item.relationships.stop.data.id
            if len(carriage_ids) > 0 and isinstance(event, VehicleRedisSchema):
                event.carriages = carriage_ids
            redis_vehicle_id = f"vehicle:{vehicle_id}"
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
    @alru_cache(maxsize=256)
    async def get_trip(
        self, trip_id: str, tg: TaskGroup, session: ClientSession
    ) -> Optional[Trips]:
        tracer = get_tracer(__name__) if is_otel_enabled() else None
        if tracer and should_trace_operation("high_volume"):
            with tracer.start_as_current_span("mbta_client.get_trip") as span:
                # Add transaction IDs to the span
                add_transaction_ids_to_span(span)
                add_entity_id_attribute(span, "trip.id", trip_id, entity_type="trip")
                return await self._get_trip_impl(trip_id, session, tg, span)
        else:
            return await self._get_trip_impl(trip_id, session, tg, None)

    async def _get_trip_impl(
        self, trip_id: str, session: ClientSession, tg: TaskGroup, span: Optional[Span]
    ) -> Optional[Trips]:
        key = f"trip:{trip_id}:full"
        trip_negative_cache = f"trip:{trip_id}:negative"
        cached = await get_cache(self.r_client, key)
        cached_negative = await get_cache(self.r_client, trip_negative_cache)
        try:
            if cached:
                if span:
                    add_span_attributes(span, {"cache.hit": True})
                trip = Trips.model_validate_json(cached, strict=False)
                add_span_attributes(span, {"trip.result": "cache_hit"})
                return trip
            elif cached_negative:
                if span:
                    add_span_attributes(span, {"cache.hit": True})
                add_span_attributes(span, {"trip.result": "cache_hit"})
                return None
            else:
                if span:
                    add_span_attributes(span, {"cache.hit": False})
                params = [("api_key", MBTA_AUTH), ("filter[id]", trip_id)]
                async with rate_limited_get(
                    session,
                    self.r_client,
                    "trips",
                    params=params,
                ) as response:
                    add_span_attributes(
                        span,
                        {
                            "http.status_code": response.status,
                            "mbta.endpoint": "trips",
                        },
                    )
                    if response.status == 429:
                        raise RateLimitExceeded()
                    if response.status != 200:
                        logger.warning(
                            f"Unexpected status code: {response.status} on trip {id}"
                        )
                        await write_cache(
                            self.r_client, trip_negative_cache, "negative", WEEK
                        )
                        return None
                    body = await response.text()
                    mbta_api_requests.labels("trips").inc()

                    trip = Trips.model_validate_json(body, strict=False)
                    tg.start_soon(
                        write_cache, self.r_client, key, trip.model_dump_json(), DAY
                    )
                    add_span_attributes(span, {"trip.result": "fetched"})
                    return trip
        except ValidationError as err:
            logger.error("Unable to parse trip", exc_info=err)
            set_span_error(span, err)
            add_span_attributes(span, {"error.type": "validation_error"})
        return None

    @retry(
        wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
        retry=retry_if_not_exception_type(CancelledError),
    )
    @alru_cache(maxsize=128)
    async def get_route(
        self, id: str, r_client: Redis, session: aiohttp.ClientSession
    ) -> Optional[RouteResource]:
        key = f"route:{id}"
        negative_key = f"route:{id}:negative"
        cached_negative = await get_cache(r_client, negative_key)
        if cached_negative:
            return None

        cached = await r_client.get(key)
        if cached:
            try:
                self.routes[id] = RouteResource.model_validate_json(
                    cached, strict=False
                )
                return self.routes[id]
            except ValidationError:
                logger.error("Unable to parse cached route", exc_info=True)
        else:
            async with rate_limited_get(
                session,
                self.r_client,
                f"routes/{id}",
                params=[("api_key", MBTA_AUTH)],
            ) as response:
                try:
                    if response.status == 429:
                        raise RateLimitExceeded()
                    if response.status != 200:
                        logger.warning(
                            f"Unexpected status code: {response.status} on route {id}"
                        )
                        await write_cache(r_client, negative_key, "negative", WEEK)
                        return
                    body = await response.json()
                    mbta_api_requests.labels("routes").inc()
                    route = RouteResource.model_validate(body["data"], strict=False)
                    self.routes[id] = route
                    await write_cache(r_client, key, route.model_dump_json(), WEEK)
                    return route
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
            with tracer.start_as_current_span(
                "mbta_client.get_alerts",
                attributes={"lookup.type": "trip" if trip_id else "route"},
            ) as span:
                add_transaction_ids_to_span(span)
                add_entity_id_attribute(span, "trip.id", trip_id, entity_type="trip")
                if route_id:
                    add_span_attributes(span, {"route.id": route_id})
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
            add_span_attributes(
                span,
                {
                    "error": True,
                    "error.type": "missing_parameters",
                    "alerts.count": 0,
                },
            )
            return None

        try:
            ids = await self.r_client.smembers(key)  # type: ignore[misc]
            add_span_attributes(span, {"alerts.ids.count": len(ids)})
            if not ids:
                add_span_attributes(span, {"alerts.count": 0})
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
            add_span_attributes(span, {"alerts.count": len(ret)})
            return ret
        except RedisError as e:
            logger.error("Redis error while loading alerts", exc_info=e)
            set_span_error(span, e)
            add_span_attributes(span, {"error.type": "redis_error"})
            return None
        except (ConnectionError, TimeoutError) as e:
            logger.error("Connection error while loading alerts", exc_info=e)
            set_span_error(span, e)
            add_span_attributes(span, {"error.type": "connection_error"})
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

        endpoint = "schedules"
        params = [
            ("api_key", MBTA_AUTH),
            ("filter[stop]", self.stop_id),
            ("sort", "time"),
            ("filter[date]", datetime.now().date().isoformat()),
        ]
        if self.route != "":
            params.append(("filter[route]", self.route))
        if self.direction_filter != "":
            params.append(("filter[direction_id]", self.direction_filter))
        if time_limit:
            diff = datetime.now().astimezone(ZoneInfo("America/New_York")) + time_limit
            if diff.hour >= 2:
                # Time after which schedule should not be returned.
                # To filter times after midnight use more than 24 hours.
                # For example, min_time=24:00 will return schedule information for the next calendar day, since that service is considered part of the current service day.
                # Additionally, min_time=00:00&max_time=02:00 will not return anything. The time format is HH:MM.
                # https://api-v3.mbta.com/docs/swagger/index.html#/Schedule/ApiWeb_ScheduleController_index
                params.append(("filter[max_time]", diff.strftime("%H:%M")))
        async with rate_limited_get(
            session, self.r_client, endpoint, params=params
        ) as response:
            try:
                if response.status == 429:
                    raise RateLimitExceeded()
                body = await response.text()
                mbta_api_requests.labels("schedules").inc()
                schedules = Schedules.model_validate_json(strict=False, json_data=body)

                for item in schedules.data:
                    if item.relationships.route and item.relationships.route.data:
                        tg.start_soon(
                            self.get_route,
                            item.relationships.route.data.id,
                            self.r_client,
                            session,
                        )
                    tg.start_soon(
                        self._queue_item_event,
                        item,
                        "reset",
                        send_stream,
                        session,
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
        wait=wait_exponential_jitter(initial=2, jitter=50, max=120),
        before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
        retry=retry_if_not_exception_type(CancelledError),
    )
    @alru_cache(maxsize=256)
    async def get_stop(
        self,
        session: ClientSession,
        stop_id: str,
        tg: Optional[TaskGroup] = None,
        include_facilities: bool = True,
    ) -> tuple[Optional[Stop], Optional[Facilities]]:
        tracer = get_tracer(__name__) if is_otel_enabled() else None
        if tracer and should_trace_operation("high_volume"):
            with tracer.start_as_current_span("mbta_client.get_stop") as span:
                add_transaction_ids_to_span(span)
                add_entity_id_attribute(span, "stop.id", stop_id, entity_type="stop")
                return await self._get_stop_impl(
                    session, stop_id, tg, span, include_facilities
                )
        else:
            return await self._get_stop_impl(
                session, stop_id, tg, None, include_facilities
            )

    async def _get_stop_impl(
        self,
        session: ClientSession,
        stop_id: str,
        tg: Optional[TaskGroup],
        span: Optional[Span],
        include_facilities: bool,
    ) -> tuple[Optional[Stop], Optional[Facilities]]:
        if session.closed:
            logger.debug("get_stop called with a closed session; skipping fetch")
            if span:
                span.set_attribute("session.closed", True)
            return None, None
        key = stop_key(stop_id)
        stop = None
        facilities = None
        cached = await get_cache(self.r_client, key)
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

            endpoint = f"/stops/{stop_id}"
            params = [("api_key", MBTA_AUTH)]
            async with rate_limited_get(
                session,
                self.r_client,
                endpoint,
                params=params,
            ) as response:
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
            if include_facilities:
                endpoint = "/facilities"
                params = [
                    ("api_key", MBTA_AUTH),
                    ("filter[stop]", stop_id),
                    ("filter[type]", "BIKE_STORAGE"),
                ]
                async with rate_limited_get(
                    session,
                    self.r_client,
                    endpoint,
                    params=params,
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


def stop_key(stop_id: str):
    return f"stop:{stop_id}:full"
