import logging
import os
from asyncio import CancelledError
from datetime import UTC, datetime, timedelta
from random import randint
from typing import TYPE_CHECKING, Optional

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientPayloadError
from aiosseclient import aiosseclient
from anyio import create_task_group, sleep
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectSendStream
from config import Config
from consts import FOUR_WEEKS, MBTA_V3_ENDPOINT, TWO_MONTHS
from exceptions import RateLimitExceeded
from mbta_responses import AlertResource, Shapes
from opentelemetry.trace import Span
from otel_config import get_tracer, is_otel_enabled
from otel_utils import (
    add_span_attributes,
)
from polyline import decode
from prometheus import (
    mbta_api_requests,
    server_side_events,
    tracker_executions,
)
from pydantic import ValidationError
from redis.asyncio.client import Redis
from redis_cache import check_cache, write_cache
from schedule_tracker import ScheduleEvent, VehicleRedisSchema
from shared_types.shared_types import LightStop, LineRoute, RouteShapes, TaskType
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
    tracer = get_tracer(__name__) if is_otel_enabled() else None
    if tracer:
        with tracer.start_as_current_span(
            "mbta_client_extended.light_get_stop", attributes={"stop_id": stop_id}
        ) as span:
            return await _light_get_stop_impl(r_client, stop_id, session, tg, span)
    else:
        return await _light_get_stop_impl(r_client, stop_id, session, tg, None)


async def _light_get_stop_impl(
    r_client: Redis,
    stop_id: str,
    session: ClientSession,
    tg: Optional[TaskGroup],
    span: Optional[Span],
) -> Optional[LightStop]:
    key = f"stop:{stop_id}:light"
    # Import the module and access functions from it so static analysis won't
    # complain about unknown import symbols and tests can still patch them.
    import mbta_client

    cached = await mbta_client.check_cache(r_client, key)
    if cached:
        if span:
            span.set_attribute("cache.hit", True)
        try:
            cached_model = LightStop.model_validate_json(cached)
            return cached_model
        except ValidationError as err:
            logger.error("unable to validate json", exc_info=err)
            if span:
                span.set_attribute("error", True)
                span.set_attribute("error.type", "validation_error")
    else:
        if span:
            span.set_attribute("cache.hit", False)
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
    config: Config,
) -> None:
    while True:
        try:
            try:
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
                                config,
                            )
                    except ClientPayloadError as err:
                        logger.warning("SSE client payload error", exc_info=err)
                        continue
            except CancelledError:
                logger.info("SSE watcher cancelled; stopping stream loop")
                return
            except GeneratorExit as e:
                logger.error(
                    "GeneratorExit in watch_mbta_server_side_events", exc_info=e
                )
                return
        except* ClientPayloadError as eg:
            for err in eg.exceptions:
                logger.warning("SSE client payload error", exc_info=err)


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
    config: Config,
    session: ClientSession | None = None,
) -> None:
    # Set route monitor transaction ID for this route monitoring task
    from logging import getLogger

    from otel_utils import set_route_monitor_transaction_id

    route_monitor_txn_id = set_route_monitor_transaction_id(route_id)
    logger = getLogger(__name__)
    logger.info(
        f"Starting route monitoring for {route_id} with transaction ID: {route_monitor_txn_id}"
    )

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
            config=config,
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
    config: Config,
    route_substring_filter: Optional[str] = None,
    session: ClientSession | None = None,
) -> None:
    tracer = get_tracer(__name__) if is_otel_enabled() else None

    if tracer:
        with tracer.start_as_current_span("mbta_sse.watch_station") as span:
            add_span_attributes(
                span,
                {
                    "stop.id": stop_id,
                    "route.id": route or "all",
                    "direction.filter": direction_filter
                    if direction_filter is not None
                    else "all",
                    "task.type": "sse_watcher",
                },
            )
            await _watch_station_impl(
                r_client,
                stop_id,
                route,
                direction_filter,
                send_stream,
                transit_time_min,
                expiration_time,
                show_on_display,
                tg,
                config,
                route_substring_filter,
                session,
            )
    else:
        await _watch_station_impl(
            r_client,
            stop_id,
            route,
            direction_filter,
            send_stream,
            transit_time_min,
            expiration_time,
            show_on_display,
            tg,
            config,
            route_substring_filter,
            session,
        )


async def _watch_station_impl(
    r_client: Redis,
    stop_id: str,
    route: str | None,
    direction_filter: Optional[int],
    send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
    transit_time_min: int,
    expiration_time: Optional[datetime],
    show_on_display: bool,
    tg: TaskGroup,
    config: Config,
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
            watcher, endpoint, headers, send_stream, session, transit_time_min, config
        )


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def watch_alerts(
    r_client: Redis, route_id: Optional[str], session: ClientSession, config: Config
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
            config=config,
        )
