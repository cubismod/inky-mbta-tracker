import logging
import os
from asyncio import CancelledError
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from random import randint
from typing import TYPE_CHECKING, Optional

import aiohttp
from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientPayloadError, ClientResponseError
from aiosseclient import aiosseclient
from anyio import create_task_group, sleep
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectSendStream
from config import Config
from consts import (
    HOUR,
    MBTA_V3_ENDPOINT,
    TWO_MONTHS,
    YEAR,
)
from exceptions import RateLimitExceeded, WatcherRefreshRequested
from mbta_rate_limiter import rate_limited_get
from mbta_responses import AlertResource, Shapes
from opentelemetry.trace import Span
from otel_config import get_tracer, is_otel_enabled
from otel_utils import (
    add_entity_id_attribute,
    add_span_attributes,
    add_transaction_ids_to_span,
    set_span_error,
)
from polyline import decode
from prometheus import (
    mbta_api_requests,
    record_mbta_api_rate_limit_hit,
    server_side_events,
    tracker_executions,
)
from pydantic import ValidationError
from redis.asyncio.client import Redis
from redis_cache import get_cache, write_cache
from redis_lock.asyncio import RedisLock
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
    before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def get_shapes(
    r_client: Redis,
    routes: list[str],
    session: ClientSession,
    tg: Optional[TaskGroup] = None,
) -> RouteShapes:
    tracer = get_tracer(__name__) if is_otel_enabled() else None
    if tracer:
        with tracer.start_as_current_span("mbta_client_extended.get_shapes") as span:
            add_transaction_ids_to_span(span)
            add_span_attributes(
                span,
                {
                    "routes.count": len(routes),
                    "session.closed": session.closed,
                },
            )
            return await _get_shapes_impl(r_client, routes, session, tg, span)
    return await _get_shapes_impl(r_client, routes, session, tg, None)


async def _get_shapes_impl(
    r_client: Redis,
    routes: list[str],
    session: ClientSession,
    tg: Optional[TaskGroup],
    span: Optional[Span],
) -> RouteShapes:
    # Avoid retries using a closed session; return empty result
    if session.closed:
        logger.debug("get_shapes called with a closed session; skipping fetch")
        add_span_attributes(span, {"session.closed": True, "shapes.count": 0})
        return RouteShapes(lines={})
    ret = RouteShapes(lines={})
    cache_hits = 0
    cache_misses = 0
    for route in routes:
        key = f"shape:{route}"
        cached = await get_cache(r_client, key)
        body = ""
        if cached:
            cache_hits += 1
            body = cached
        else:
            cache_misses += 1
            async with rate_limited_get(
                session, r_client, f"/shapes?filter[route]={route}&api_key={MBTA_AUTH}"
            ) as response:
                add_span_attributes(
                    span,
                    {
                        "http.status_code": response.status,
                        "mbta.endpoint": "shapes",
                    },
                )
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
    add_span_attributes(
        span,
        {
            "cache.hits": cache_hits,
            "cache.misses": cache_misses,
            "shapes.count": sum(len(lines) for lines in ret.lines.values()),
        },
    )
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
    tg: TaskGroup,
) -> Optional[LightStop]:
    tracer = get_tracer(__name__) if is_otel_enabled() else None
    if tracer:
        with tracer.start_as_current_span(
            "mbta_client_extended.light_get_stop"
        ) as span:
            add_transaction_ids_to_span(span)
            add_entity_id_attribute(span, "stop.id", stop_id, entity_type="stop")
            return await _light_get_stop_impl(r_client, stop_id, tg, span)
    else:
        return await _light_get_stop_impl(r_client, stop_id, tg, None)


async def _light_get_stop_impl(
    r_client: Redis,
    stop_id: str,
    tg: TaskGroup,
    span: Optional[Span],
) -> Optional[LightStop]:
    key = f"stop:{stop_id}:light"
    import mbta_client

    cached = await mbta_client.get_cache(r_client, key)
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
        # if it's not cached then we fetch it in the background and return None; it will be cached for next time
        if span:
            span.set_attribute("cache.hit", False)
            span.set_attribute("stop.fetch.scheduled", True)

        async def fetch_stop(stop_id: str, tg: TaskGroup):
            import mbta_client

            tracer = get_tracer(__name__) if is_otel_enabled() else None
            with (
                tracer.start_as_current_span("mbta_client_extended.fetch_light_stop")
                if tracer
                else nullcontext() as fetch_span
            ):
                add_transaction_ids_to_span(fetch_span)
                add_entity_id_attribute(
                    fetch_span, "stop.id", stop_id, entity_type="stop"
                )
                await sleep(randint(0, 60))
                if await get_cache(r_client, key):
                    logger.debug(
                        f"Stop data for {stop_id} was cached while waiting; skipping fetch"
                    )
                    add_span_attributes(fetch_span, {"cache.hit": True})
                    return

                logger.debug(f"Fetching stop data for {stop_id} from MBTA API")
                MBTAApi = mbta_client.MBTAApi
                ls = None

                async with RedisLock(
                    r_client,
                    f"stop_fetch:{stop_id}",
                    blocking_timeout=60,
                    expire_timeout=30,
                ):
                    if await get_cache(r_client, key):
                        logger.debug(
                            f"Stop data for {stop_id} was cached while waiting on lock; skipping fetch"
                        )
                        add_span_attributes(fetch_span, {"cache.hit": True})
                        return

                    add_span_attributes(fetch_span, {"cache.hit": False})
                    async with aiohttp.ClientSession(
                        base_url=MBTA_V3_ENDPOINT
                    ) as session:
                        async with MBTAApi(
                            r_client,
                            stop_id=stop_id,
                            watcher_type=TaskType.LIGHT_STOP,
                        ) as watcher:
                            stop = await watcher.get_stop(
                                session, stop_id, tg, include_facilities=False
                            )
                            mbta_stop_id = stop_id
                            if stop and stop[0]:
                                parent_stop_id = None
                                if stop[0].data.attributes.description:
                                    stop_id = stop[0].data.attributes.description
                                elif stop[0].data.attributes.name:
                                    stop_id = stop[0].data.attributes.name

                                if (
                                    stop[0].data.relationships
                                    and stop[0].data.relationships.parent_station.data
                                ):
                                    parent_stop_id = stop[
                                        0
                                    ].data.relationships.parent_station.data.id
                                    tg.start_soon(
                                        light_get_stop,
                                        r_client,
                                        parent_stop_id,
                                        tg,
                                    )
                                ls = LightStop(
                                    stop_id=stop_id,
                                    long=stop[0].data.attributes.longitude,
                                    lat=stop[0].data.attributes.latitude,
                                    mbta_stop_id=mbta_stop_id,
                                    parent_stop_id=parent_stop_id,
                                )
                            if ls:
                                await write_cache(
                                    r_client,
                                    key,
                                    ls.model_dump_json(),
                                    randint(TWO_MONTHS, YEAR),
                                )
                                add_span_attributes(
                                    fetch_span, {"stop.fetch.result": "cached"}
                                )
                            else:
                                add_span_attributes(
                                    fetch_span, {"stop.fetch.result": "not_found"}
                                )

        tg.start_soon(fetch_stop, stop_id, tg)
        return None


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
    before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
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
        tracer = get_tracer(__name__) if is_otel_enabled() else None
        stream_span_cm = (
            tracer.start_as_current_span("mbta_sse.stream_connection")
            if tracer
            else nullcontext()
        )
        with stream_span_cm as span:
            add_transaction_ids_to_span(span)
            add_span_attributes(
                span,
                {
                    "mbta.sse.watcher": watcher.gen_unique_id(),
                    "mbta.sse.has_send_stream": send_stream is not None,
                    "mbta.sse.transit_time_min": transit_time_min,
                },
            )
            event_count = 0
            reconnect_reason = "sleep_retry"
            try:
                try:
                    async with create_task_group() as tg:
                        tg.start_soon(watcher._monitor_health, tg)
                        try:
                            async for event in aiosseclient(
                                endpoint, headers=headers, raise_for_status=True
                            ):
                                event_count += 1
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
                            reconnect_reason = "payload_error"
                            set_span_error(span, err)
                            logger.warning("SSE client payload error", exc_info=err)
                            continue
                        except ClientResponseError as err:
                            reconnect_reason = f"http_{err.status}"
                            set_span_error(span, err)
                            if err.status == 429:
                                record_mbta_api_rate_limit_hit(endpoint)
                                logger.warning(
                                    "Rate limit hit while opening MBTA SSE stream",
                                    exc_info=err,
                                )
                            else:
                                logger.warning("SSE client HTTP error", exc_info=err)
                            tg.cancel_scope.cancel()
                except CancelledError:
                    reconnect_reason = "cancelled"
                    logger.info("SSE watcher cancelled; stopping stream loop")
                    return
                except GeneratorExit as e:
                    reconnect_reason = "generator_exit"
                    add_span_attributes(
                        span,
                        {
                            "error": True,
                            "error.type": "GeneratorExit",
                        },
                    )
                    logger.error(
                        "GeneratorExit in watch_mbta_server_side_events", exc_info=e
                    )
                    return
            except* ClientPayloadError as eg:
                reconnect_reason = "payload_error_group"
                for err in eg.exceptions:
                    set_span_error(span, err)
                    logger.warning("SSE client payload error", exc_info=err)
            except* WatcherRefreshRequested:
                reconnect_reason = "watcher_refresh_requested"
                logger.info(
                    "Reconnecting MBTA SSE watcher after health monitor refresh request"
                )
            finally:
                add_span_attributes(
                    span,
                    {
                        "mbta.sse.events": event_count,
                        "mbta.sse.reconnect_reason": reconnect_reason,
                    },
                )
        await sleep(0.01)


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
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

            tracer = get_tracer(__name__) if is_otel_enabled() else None
            span_cm = (
                tracer.start_as_current_span("mbta_sse.watch_static_schedule.refresh")
                if tracer
                else nullcontext()
            )
            with span_cm as span:
                add_transaction_ids_to_span(span)
                add_entity_id_attribute(span, "stop.id", stop_id, entity_type="stop")
                add_span_attributes(
                    span,
                    {
                        "route.id": route or "all",
                        "direction.filter": direction
                        if direction is not None
                        else "all",
                        "task.type": "static_schedule_refresh",
                        "transit_time_min": transit_time_min,
                    },
                )
                try:
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
                        add_span_attributes(
                            span,
                            {
                                "schedule.refresh.status": "success",
                                "schedule.next_refresh": refresh_time.isoformat(),
                            },
                        )
                except Exception as exc:
                    set_span_error(span, exc)
                    raise
        await sleep(10)


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before=before_log(logger, logging.INFO),
    before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
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

    tracer = get_tracer(__name__) if is_otel_enabled() else None
    span_cm = (
        tracer.start_as_current_span("mbta_sse.watch_vehicles")
        if tracer
        else nullcontext()
    )
    with span_cm as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "route.id": route_id,
                "task.type": "vehicle_sse_watcher",
                "watcher.expiration_time": expiration_time.isoformat()
                if expiration_time
                else None,
            },
        )
        try:
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
        except Exception as exc:
            set_span_error(span, exc)
            raise


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
            add_transaction_ids_to_span(span)
            add_entity_id_attribute(span, "stop.id", stop_id, entity_type="stop")
            add_span_attributes(
                span,
                {
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
    before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
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
    endpoint = f"{MBTA_V3_ENDPOINT}/alerts?api_key={MBTA_AUTH}&filter[lifecycle]=NEW,ONGOING,ONGOING_UPCOMING&filter[datetime]=NOW"
    if route_id:
        endpoint += f"&filter[route]={route_id}"
    headers = {"accept": "text/event-stream"}

    from mbta_client import MBTAApi

    tracer = get_tracer(__name__) if is_otel_enabled() else None
    span_cm = (
        tracer.start_as_current_span("mbta_sse.watch_alerts")
        if tracer
        else nullcontext()
    )
    with span_cm as span:
        add_transaction_ids_to_span(span)
        add_span_attributes(
            span,
            {
                "route.id": route_id or "all",
                "task.type": "alerts_sse_watcher",
            },
        )
        async with MBTAApi(
            r_client,
            route=route_id,
            watcher_type=TaskType.ALERTS,
        ) as watcher:
            tracker_executions.labels("alerts").inc()
            await sleep(randint(1, 15))

            async def write_alerts_heartbeat() -> None:
                heartbeat_key = "heartbeat:events:alerts"
                if route_id:
                    heartbeat_key = f"heartbeat:events:alerts:{route_id}"
                while True:
                    try:
                        await r_client.set(
                            heartbeat_key, datetime.now(UTC).isoformat(), ex=2 * HOUR
                        )
                    except Exception as e:
                        set_span_error(span, e)
                        logger.error("Failed to write alerts heartbeat", exc_info=e)
                    await sleep(30)

            try:
                async with create_task_group() as tg:
                    tg.start_soon(write_alerts_heartbeat)
                    await watch_mbta_server_side_events(
                        watcher,
                        endpoint,
                        headers,
                        None,
                        session=session,
                        transit_time_min=0,
                        config=config,
                    )
            except Exception as exc:
                set_span_error(span, exc)
                raise
