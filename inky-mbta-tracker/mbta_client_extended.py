import logging
import os
import random
from asyncio import CancelledError
from datetime import UTC, datetime, timedelta
from random import randint
from typing import TYPE_CHECKING, Any, List, Optional

import aiohttp
import anyio
from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientPayloadError
from aiosseclient import aiosseclient
from anyio import create_task_group, sleep
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectSendStream
from consts import FOUR_WEEKS, HOUR, MBTA_V3_ENDPOINT, TWO_MONTHS
from exceptions import RateLimitExceeded
from mbta_responses import (
    AlertResource,
    Schedules,
    Shapes,
)
from polyline import decode
from prometheus import (
    mbta_api_requests,
    server_side_events,
    tracker_executions,
)
from pydantic import ValidationError
from redis.asyncio.client import Redis
from redis.exceptions import RedisError
from redis_cache import check_cache, write_cache
from schedule_tracker import ScheduleEvent, VehicleRedisSchema
from shared_types.shared_types import (
    DepartureInfo,
    LightStop,
    LineRoute,
    RouteShapes,
    TaskType,
)
from tenacity import (
    before_log,
    before_sleep_log,
    retry,
    retry_if_not_exception_type,
    wait_exponential_jitter,
)

# Avoid circular import at runtime; only import for typing checks
if TYPE_CHECKING:
    from mbta_client import MBTAApi

# Module-level constants used by the moved functions
MBTA_AUTH = os.environ.get("AUTH_TOKEN")
logger = logging.getLogger(__name__)


def parse_shape_data(shapes: Shapes) -> LineRoute:
    # Import the mbta_client module and reference attributes at runtime. This
    # avoids a static from-import which confuses pyright in the presence of
    # circular imports while still allowing tests to patch attributes on
    # the `mbta_client` module (e.g., `mbta_client.decode`).
    import mbta_client

    ret = list[list[tuple]]()
    for shape in shapes.data:
        if (
            shape.attributes.polyline not in mbta_client.SHAPE_POLYLINES
            and "canonical" in shape.id
            or shape.id.replace("_", "").isdecimal()
        ):
            ret.append([i for i in decode(shape.attributes.polyline, geojson=True)])
            mbta_client.SHAPE_POLYLINES.add(shape.attributes.polyline)
    return ret


# gets line (orange, blue, red, green, etc) geometry using MBTA API
# redis expires in 24 hours
@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def get_shapes(
    r_client: Redis,
    routes: list[str],
    session: ClientSession,
    tg: Optional[TaskGroup] = None,
) -> RouteShapes:
    # Avoid retries using a closed session; return empty result
    if session.closed:
        logger.debug("get_shapes called with a closed session; skipping fetch")
        return RouteShapes(lines={})
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
    # Import the module and access functions from it so static analysis won't
    # complain about unknown import symbols and tests can still patch them.
    import mbta_client

    cached = await mbta_client.check_cache(r_client, key)
    if cached:
        try:
            cached_model = LightStop.model_validate_json(cached)
            return cached_model
        except ValidationError as err:
            logger.error("unable to validate json", exc_info=err)
    ls = None
    # Local import to avoid circular module import at import-time
    import mbta_client

    MBTAApi = mbta_client.MBTAApi

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
    from mbta_client import MBTAApi

    async with MBTAApi(
        r_client, route=route_id, watcher_type=TaskType.VEHICLES
    ) as watcher:
        alerts = await watcher.get_alerts(session, route_id=route_id)
        if alerts:
            return alerts
    return None


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
            from mbta_client import MBTAApi

            async with MBTAApi(
                r_client, watcher_type=TaskType.TRACK_PREDICTIONS
            ) as api:
                try:
                    num_cached = await api._precache_track_predictions(
                        tg=tg,
                        routes=routes,
                        target_stations=target_stations,
                    )
                    logger.info(
                        f"Track prediction precache completed: {num_cached} predictions cached; routes={routes}, stations={target_stations}"
                    )
                except (
                    ValidationError,
                    RedisError,
                    ConnectionError,
                    TimeoutError,
                ) as e:
                    logger.error("Error during precache operation", exc_info=e)
        except (ValidationError, RedisError, ConnectionError, TimeoutError) as e:
            logger.error("Error in track prediction precaching runner", exc_info=e)

        # Wait for the specified interval
        await sleep(interval_hours * HOUR)


async def fetch_upcoming_departures(
    session: Any,
    route_id: str,
    station_ids: List[str],
    target_date: Optional[datetime] = None,
) -> List[DepartureInfo]:
    """
    Fetch upcoming departures for specific stations on a commuter rail route.

    Args:
        session: HTTP session for API calls
        route_id: Commuter rail route ID (e.g., "CR-Worcester")
        station_ids: List of station IDs to fetch departures for
        target_date: Date to fetch departures for (defaults to today)

    Returns:
        List of departure data dictionaries with scheduled times
    """
    max_attempts = int(os.getenv("IMT_PRECACHE_MAX_ATTEMPTS", "5"))
    base_backoff = float(os.getenv("IMT_PRECACHE_BASE_BACKOFF", "1.0"))
    attempt = 0

    while True:
        try:
            if target_date is None:
                target_date = datetime.now(UTC)
            # Proceed with the normal fetch logic below on success
            break
        except CancelledError:
            # Preserve cancellation semantics
            raise
        except (TypeError, ValueError) as e:
            # Only retry on errors likely to be caused by malformed inputs or similar recoverable issues.
            attempt += 1
            if attempt >= max_attempts:
                logger.error(
                    f"Exceeded max attempts ({max_attempts}) in fetch_upcoming_departures for route {route_id}",
                    exc_info=e,
                )
                return []
            # Exponential backoff with small random jitter
            backoff = min(60.0, base_backoff * (2 ** (attempt - 1)))
            jitter = random.random() * 0.5
            sleep_for = backoff + jitter
            logger.debug(
                f"fetch_upcoming_departures attempt {attempt}/{max_attempts} failed for route {route_id}; retrying in {sleep_for:.2f}s",
                exc_info=e,
            )
            await anyio.sleep(sleep_for)
            # loop and retry

    date_str = target_date.date().isoformat()
    auth_token = os.environ.get("AUTH_TOKEN", "")
    upcoming_departures: list[DepartureInfo] = []

    # Fetch schedules for all stations in a single API call
    stations_str = ",".join(station_ids)

    endpoint = f"{MBTA_V3_ENDPOINT}/schedules?filter[stop]={stations_str}&filter[route]={route_id}&filter[date]={date_str}&sort=departure_time&include=trip&api_key={auth_token}"

    try:
        async with session.get(endpoint) as response:
            if response.status == 429:
                raise RateLimitExceeded()

            if response.status != 200:
                logger.error(
                    f"Failed to fetch schedules for {stations_str} on {route_id}: HTTP {response.status}"
                )
                return []

            body = await response.text()
            mbta_api_requests.labels("schedules").inc()

            try:
                schedules_data = Schedules.model_validate_json(body, strict=False)
            except ValidationError as e:
                logger.error(
                    f"Unable to parse schedules for {stations_str} on {route_id}",
                    exc_info=e,
                )
                return []

            # Process each scheduled departure
            for schedule in schedules_data.data:
                if not schedule.attributes.departure_time:
                    continue

                # Get trip information from relationships
                trip_id = ""
                if (
                    hasattr(schedule, "relationships")
                    and hasattr(schedule.relationships, "trip")
                    and schedule.relationships.trip.data
                ):
                    trip_id = schedule.relationships.trip.data.id

                # Get station ID from relationships
                station_id = ""
                if (
                    hasattr(schedule, "relationships")
                    and hasattr(schedule.relationships, "stop")
                    and schedule.relationships.stop.data
                ):
                    station_id = schedule.relationships.stop.data.id

                departure_info: DepartureInfo = {
                    "trip_id": trip_id,
                    "station_id": station_id,
                    "route_id": route_id,
                    "direction_id": schedule.attributes.direction_id,
                    "departure_time": schedule.attributes.departure_time,
                }

                upcoming_departures.append(departure_info)

    except (aiohttp.ClientError, TimeoutError) as e:
        logger.error(
            f"Error fetching schedules for {stations_str} on {route_id}",
            exc_info=e,
        )
        return []

    logger.debug(
        f"Found {len(upcoming_departures)} upcoming departures for route {route_id} across {len(station_ids)} stations"
    )
    return upcoming_departures


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def watch_mbta_server_side_events(
    watcher: "MBTAApi",
    endpoint: str,
    headers: dict[str, str],
    send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema] | None,
    session: ClientSession,
    transit_time_min: int,
) -> None:
    while True:
        try:
            try:
                async with create_task_group() as tg:
                    client = aiosseclient(endpoint, headers=headers)
                    if os.getenv("IMT_PROMETHEUS_ENDPOINT"):
                        tg.start_soon(watcher._monitor_health, tg)
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
            except* ClientPayloadError as eg:
                for err in eg.exceptions:
                    logger.error("SSE client payload error", exc_info=err)
        except CancelledError:
            logger.info("SSE watcher cancelled; stopping stream loop")
            return
        except GeneratorExit as e:
            logger.error("GeneratorExit in watch_mbta_server_side_events", exc_info=e)
            return


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
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
    session: ClientSession | None = None,
) -> None:
    if route_substring_filter:
        logger.info(
            f"Watching station {stop_id} for route substring filter {route_substring_filter}"
        )
    refresh_time = datetime.now().astimezone(UTC) - timedelta(minutes=10)
    assert session is not None, (
        "ClientSession must be provided to watch_static_schedule"
    )
    while True:
        if datetime.now().astimezone(UTC) > refresh_time:
            from mbta_client import MBTAApi

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
                await watcher.save_schedule(transit_time_min, send_stream, session, tg)
                refresh_time = datetime.now().astimezone(UTC) + timedelta(
                    hours=randint(2, 6)
                )
        await sleep(10)


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def watch_vehicles(
    r_client: Redis,
    send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
    expiration_time: Optional[datetime],
    route_id: str,
    session: ClientSession | None = None,
) -> None:
    endpoint = f"{MBTA_V3_ENDPOINT}/vehicles?fields[vehicle]=direction_id,latitude,longitude,speed,current_status,occupancy_status,carriages&filter[route]={route_id}&api_key={MBTA_AUTH}"
    mbta_api_requests.labels("vehicles").inc()
    headers = {"accept": "text/event-stream"}
    assert session is not None, "ClientSession must be provided to watch_vehicles"
    from mbta_client import MBTAApi

    async with MBTAApi(
        r_client,
        route=route_id,
        watcher_type=TaskType.VEHICLES,
        expiration_time=expiration_time,
    ) as watcher:
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
    session: ClientSession | None = None,
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

    assert session is not None, "ClientSession must be provided to watch_station"
    from mbta_client import MBTAApi

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


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def watch_alerts(
    r_client: Redis, route_id: Optional[str], session: ClientSession
) -> None:
    """Watch MBTA Alerts via SSE and persist to Redis.

    - If `route_id` is provided, filters alerts for that route and maintains
      `alerts:route:{route_id}` set membership.
    - Stores individual alerts under `alert:{id}` with a short TTL.
    """
    endpoint = f"{MBTA_V3_ENDPOINT}/alerts?api_key={MBTA_AUTH}&filter[lifecycle]=NEW,ONGOING,ONGOING_UPCOMING&filter[datetime]=NOW&filter[severity]=3,4,5,6,7,8,9,10"
    if route_id:
        endpoint += f"&filter[route]={route_id}"
    headers = {"accept": "text/event-stream"}

    from mbta_client import MBTAApi

    async with MBTAApi(
        r_client,
        route=route_id,
        watcher_type=TaskType.ALERTS,
    ) as watcher:
        tracker_executions.labels("alerts").inc()
        await sleep(randint(1, 15))
        await watch_mbta_server_side_events(
            watcher,
            endpoint,
            headers,
            None,
            session=session,
            transit_time_min=0,
        )


def get_default_routes() -> List[str]:
    """Get the default list of commuter rail routes for precaching."""
    return [
        "CR-Worcester",
        "CR-Framingham",
        "CR-Franklin",
        "CR-Foxboro",
        "CR-Providence",
        "CR-Stoughton",
        "CR-Needham",
        "CR-Fairmount",
        "CR-Fitchburg",
        "CR-Lowell",
        "CR-Haverhill",
        "CR-Newburyport",
        "CR-Rockport",
        "CR-Kingston",
        "CR-Plymouth",
        "CR-Greenbush",
    ]


def get_default_target_stations() -> List[str]:
    """Get the default list of stations for precaching."""
    return [
        "place-sstat",  # South Station
        "place-north",  # North Station
        "place-bbsta",  # Back Bay
        "place-rugg",  # Ruggles
    ]
