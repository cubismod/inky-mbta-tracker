import logging
import os
from asyncio import CancelledError
from datetime import UTC, datetime, timedelta
from itertools import product
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
    MINUTE,
    TEN_MIN,
    TWO_MONTHS,
    YEAR,
)
from exceptions import RateLimitExceeded
from mbta_rate_limiter import rate_limited_get
from mbta_responses import (
    AlertResource,
    PredictionResource,
    PredictionResourceList,
    Shapes,
)
from opentelemetry.trace import Span
from otel_config import get_tracer, is_otel_enabled
from otel_utils import (
    add_span_attributes,
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
from redis_cache import check_cache, write_cache
from redis_lock.asyncio import RedisLock
from schedule_tracker import ScheduleEvent, VehicleRedisSchema
from shared_types.shared_types import (
    LightStop,
    LineRoute,
    PredictionsRequest,
    RouteShapes,
    StopResponse,
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
    before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
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
            async with rate_limited_get(
                session, r_client, f"/shapes?filter[route]={route}&api_key={MBTA_AUTH}"
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
    tg: TaskGroup,
    fetch_immediately: bool = False,
) -> Optional[LightStop]:
    tracer = get_tracer(__name__) if is_otel_enabled() else None
    if tracer:
        with tracer.start_as_current_span(
            "mbta_client_extended.light_get_stop", attributes={"stop_id": stop_id}
        ) as span:
            return await _light_get_stop_impl(
                r_client, stop_id, tg, span, fetch_immediately
            )
    else:
        return await _light_get_stop_impl(
            r_client, stop_id, tg, None, fetch_immediately
        )


async def _light_get_stop_impl(
    r_client: Redis,
    stop_id: str,
    tg: TaskGroup,
    span: Optional[Span],
    fetch_immediately: bool = False,
) -> Optional[LightStop]:
    key = f"stop:{stop_id}:light"
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

        async def fetch_stop(stop_id: str, tg: TaskGroup):
            import mbta_client

            if not fetch_immediately:
                await sleep(randint(MINUTE, TEN_MIN))
            if await check_cache(r_client, key):
                logger.debug(
                    f"Stop data for {stop_id} was cached while waiting; skipping fetch"
                )
                return

            logger.debug(f"Fetching stop data for {stop_id} from MBTA API")
            MBTAApi = mbta_client.MBTAApi
            ls = None

            async with RedisLock(
                r_client,
                f"stop_fetch:{stop_id}",
                blocking_timeout=5,
                expire_timeout=30,
            ):
                if await check_cache(r_client, key):
                    logger.debug(
                        f"Stop data for {stop_id} was cached while waiting on lock; skipping fetch"
                    )
                    return

                async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
                    async with MBTAApi(
                        r_client, stop_id=stop_id, watcher_type=TaskType.LIGHT_STOP
                    ) as watcher:
                        stop = await watcher.get_stop(
                            session, stop_id, tg, include_facilities=False
                        )
                        stop_name = ""
                        if stop and stop[0]:
                            if stop[0].data.attributes.description:
                                stop_name = stop[0].data.attributes.description
                            elif stop[0].data.attributes.name:
                                stop_name = stop[0].data.attributes.name

                            parent_id: Optional[str] = None
                            if (
                                stop[0].data.relationships
                                and stop[0].data.relationships.parent_station
                                and stop[0].data.relationships.parent_station.data
                            ):
                                parent_id = stop[
                                    0
                                ].data.relationships.parent_station.data.id
                            ls = LightStop(
                                stop_id=stop_id,
                                stop_name=stop_name,
                                long=stop[0].data.attributes.longitude,
                                lat=stop[0].data.attributes.latitude,
                                parent_id=parent_id,
                            )
                        if ls:
                            await write_cache(
                                r_client,
                                key,
                                ls.model_dump_json(),
                                randint(TWO_MONTHS, YEAR),
                            )

        if fetch_immediately:
            await fetch_stop(stop_id, tg)
        else:
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


async def light_get_headsign(
    trip_id: str, session: ClientSession, r_client: Redis, tg: TaskGroup
) -> Optional[str]:
    from mbta_client import MBTAApi

    async with MBTAApi(r_client, watcher_type=TaskType.VEHICLES) as watcher:
        return await watcher.get_headsign(trip_id, session, tg)
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
        try:
            try:
                async with create_task_group() as tg:
                    tg.start_soon(watcher._monitor_health, tg)
                    try:
                        async for event in aiosseclient(
                            endpoint, headers=headers, raise_for_status=True
                        ):
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
                    except ClientResponseError as err:
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
        await sleep(
            randint(5, 15)
        )  # in case we invoke a 429 which is just logged but not thrown from aiosseclient


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
                    logger.error("Failed to write alerts heartbeat", exc_info=e)
                await sleep(30)

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


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before_sleep=before_sleep_log(logger, logging.WARNING, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def get_predictions(
    session: ClientSession,
    stops: list[str],
    r_client: Redis,
    tg: Optional[TaskGroup],
    span: Optional[Span],
) -> Optional[list[PredictionResource]]:
    if session.closed:
        logger.debug("get_predictions called with a closed session; skipping fetch")
        if span:
            span.set_attribute("session.closed", True)
        return None
    key = f"predictions:{','.join(stops)}"
    cached = await check_cache(r_client, key)
    if cached:
        if span:
            span.set_attribute("cache.hit", True)
        try:
            cached_model = PredictionResourceList.model_validate_json(cached)
            return cached_model.data
        except ValidationError as err:
            logger.error("unable to validate json", exc_info=err)
            if span:
                span.set_attribute("error", True)
                span.set_attribute("error.type", "validation_error")
    else:
        if span:
            span.set_attribute("cache.hit", False)
        endpoint = f"{MBTA_V3_ENDPOINT}/predictions?filter[stop]={','.join(stops)}&sort=time&api_key={MBTA_AUTH}"
        async with session.get(endpoint) as response:
            if response.status == 429:
                raise RateLimitExceeded()
            body = await response.text()
            mbta_api_requests.labels("predictions").inc()
            if tg:
                tg.start_soon(write_cache, r_client, key, body, 3 * 50)
            else:
                await write_cache(r_client, key, body, 3 * 60)
            try:
                model = PredictionResourceList.model_validate_json(body)
                return model.data
            except ValidationError as err:
                logger.error("unable to validate json", exc_info=err)
                if span:
                    span.set_attribute("error", True)
                    span.set_attribute("error.type", "validation_error")


async def filter_predictions(
    predictions: list[PredictionResource],
    predictions_request: PredictionsRequest,
    r_client: Redis,
    tg: TaskGroup,
):
    filtered: list[PredictionResource] = []
    now = datetime.now(UTC)
    for prediction in predictions:
        # need to fetch the stop to get the parent station ID
        pred_stop_id = (
            prediction.relationships.stop.data.id
            if prediction.relationships.stop and prediction.relationships.stop.data
            else ""
        )
        pred_stop = await light_get_stop(
            r_client,
            pred_stop_id,
            tg,
            fetch_immediately=True,
        )
        if pred_stop:
            pred_stop_id = (
                pred_stop.parent_id if pred_stop.parent_id else pred_stop.stop_id
            )

        for stop in predictions_request.stops:
            if pred_stop_id == stop.id:
                if (
                    prediction.relationships.route.data
                    and stop.routes
                    and len(stop.routes) > 0
                    and prediction.relationships.route.data.id not in stop.routes
                ):
                    continue
                if prediction.attributes.direction_id != stop.direction:
                    continue
                if (pred_stop and prediction.attributes.trip_headsign) and (
                    prediction.attributes.trip_headsign in pred_stop.stop_name
                ):
                    # skip returning trips
                    continue
                if prediction.attributes.departure_time:
                    departure_time = datetime.fromisoformat(
                        prediction.attributes.departure_time
                    ).astimezone(UTC)
                    time_to_leave = departure_time - timedelta(
                        minutes=stop.transit_time_min
                    )
                    if departure_time < now or time_to_leave < now:
                        continue
                elif prediction.attributes.arrival_time:
                    arrival_time = datetime.fromisoformat(
                        prediction.attributes.arrival_time
                    ).astimezone(UTC)
                    time_to_leave = arrival_time - timedelta(
                        minutes=stop.transit_time_min
                    )
                    if arrival_time < now or time_to_leave < now:
                        continue
                if prediction.relationships.stop.data:
                    prediction.relationships.stop.data.id = pred_stop_id
                filtered.append(prediction)
        # perform a last pass for removing duplicate trips
        for i, j in product(filtered, filtered):
            if i.id == j.id:
                continue
            i_arrival = i.attributes.arrival_time or i.attributes.departure_time
            j_arrival = j.attributes.arrival_time or j.attributes.departure_time
            if (i.relationships.trip.data and j.relationships.trip.data) and (
                i.relationships.trip.data.id == j.relationships.trip.data.id
            ):
                if i_arrival and j_arrival:
                    i_ts = datetime.fromisoformat(i_arrival).astimezone(UTC)
                    j_ts = datetime.fromisoformat(j_arrival).astimezone(UTC)
                    if j_ts > i_ts:
                        filtered.remove(j)
    return filtered


async def transform_predictions(
    predictions: list[PredictionResource],
    tg: TaskGroup,
    r_client: Redis,
    session: ClientSession,
    stop_name_mapping: dict[str, str],
):
    stop_responses: list[StopResponse] = []

    for prediction in predictions:
        stop_info = (
            await light_get_stop(r_client, prediction.relationships.stop.data.id, tg)
            if prediction.relationships.stop.data
            else None
        )
        headsign = (
            await light_get_headsign(
                prediction.relationships.trip.data.id, session, r_client, tg
            )
            if prediction.relationships.trip.data
            else None
        )

        timestamp = datetime.fromisoformat(
            prediction.attributes.departure_time
            or prediction.attributes.arrival_time
            or datetime.now().isoformat()
        ).astimezone(UTC)

        stop_id = (
            prediction.relationships.stop.data.id
            if prediction.relationships.stop and prediction.relationships.stop.data
            else ""
        )

        stop_response = StopResponse(
            stop_id=stop_id,
            stop_name=stop_name_mapping.get(
                stop_id, stop_info.stop_name if stop_info else ""
            ),
            timestamp=timestamp.isoformat(),
            headsign=headsign if headsign else "",
            route_id=prediction.relationships.route.data.id
            if prediction.relationships.route.data
            else "",
        )

        stop_responses.append(stop_response)
    return stop_responses
