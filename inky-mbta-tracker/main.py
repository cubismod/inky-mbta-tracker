import logging
import os
from datetime import UTC, datetime, timedelta
from random import randint
from typing import Optional
from zoneinfo import ZoneInfo

import aiohttp
import click
from anyio import create_memory_object_stream, create_task_group, run, sleep
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectSendStream
from config import Config, StopSetup, load_config
from consts import MBTA_V3_ENDPOINT
from dotenv import load_dotenv
from geojson_utils import background_refresh
from logging_setup import setup_logging
from mbta_client_extended import (
    precache_track_predictions_runner,
    watch_alerts,
    watch_static_schedule,
    watch_station,
    watch_vehicles,
)
from otel_config import initialize_otel, is_otel_enabled, shutdown_otel
from prometheus_client import start_http_server
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool
from redis_backup import RedisBackup
from schedule_tracker import (
    ScheduleEvent,
    VehicleRedisSchema,
    process_queue_async,
)
from shared_types.schema_versioner import schema_versioner
from shared_types.shared_types import TaskType
from track_predictor.track_predictor import TrackPredictor
from utils import get_redis

load_dotenv()
setup_logging()

# Initialize OpenTelemetry for the main worker process
initialize_otel(
    service_name_override=os.getenv("IMT_OTEL_SERVICE_NAME", "inky-mbta-tracker-worker")
)

logger = logging.getLogger(__name__)

MIN_TASK_RESTART_MINS = 45
MAX_TASK_RESTART_MINS = 120


# launches a departures tracking task, target should either be "schedule" or "predictions"
def start_task(
    r_client: Redis,
    target: TaskType,
    send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
    tg: TaskGroup,
    session: aiohttp.ClientSession,
    config: Config,
    stop: Optional[StopSetup] = None,
    route_id: Optional[str] = None,
) -> None:
    exp_time = datetime.now().astimezone(UTC) + timedelta(
        minutes=randint(MIN_TASK_RESTART_MINS, MAX_TASK_RESTART_MINS)
    )
    direction_filter = None
    if stop and stop.direction_filter != -1:
        direction_filter = stop.direction_filter
    match target:
        case TaskType.SCHEDULES:
            if stop:
                tg.start_soon(
                    watch_static_schedule,
                    r_client,
                    stop.stop_id,
                    stop.route_filter,
                    direction_filter,
                    send_stream,
                    stop.transit_time_min,
                    stop.show_on_display,
                    tg,
                    stop.route_substring_filter,
                    session,
                )
        case TaskType.SCHEDULE_PREDICTIONS:
            if stop:
                tg.start_soon(
                    watch_station,
                    r_client,
                    stop.stop_id,
                    stop.route_filter,
                    direction_filter,
                    send_stream,
                    stop.transit_time_min,
                    exp_time,
                    stop.show_on_display,
                    tg,
                    config,
                    stop.route_substring_filter,
                    session,
                )
        case TaskType.VEHICLES:
            tg.start_soon(
                watch_vehicles,
                r_client,
                send_stream,
                exp_time,
                route_id or "",
                config,
                session,
            )


def get_next_backup_time(now: Optional[datetime] = None) -> datetime:
    """Return the next datetime (America/New_York) to run the Redis backup.

    Uses the time of day from env var IMT_REDIS_BACKUP_TIME (HH:MM), defaults to 03:00.
    If the time today has already passed, schedule for the same time tomorrow.
    """
    tz = ZoneInfo("America/New_York")
    if now is None:
        now = datetime.now(tz)
    else:
        # Ensure timezone-aware in target TZ
        if now.tzinfo is None:
            now = now.replace(tzinfo=tz)
        else:
            now = now.astimezone(tz)

    backup_time = os.getenv("IMT_REDIS_BACKUP_TIME", "03:00")
    hour, minute = (0, 0)
    try:
        hour_str, minute_str = backup_time.split(":", 1)
        hour, minute = int(hour_str), int(minute_str)
    except ValueError:
        # Fallback to 03:00 on parse error (invalid integer parsing)
        hour, minute = 3, 0

    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate


async def heartbeat_task(redis: Redis) -> None:
    """
    Background task that writes a periodic heartbeat to Redis for healthcheck monitoring.

    This task writes the current timestamp to Redis every 30 seconds, allowing the
    healthcheck script to verify that the main process is running and healthy.
    """
    from redis_cache import write_cache

    HEARTBEAT_KEY = "healthcheck:heartbeat"
    HEARTBEAT_INTERVAL_SECONDS = 30
    HEARTBEAT_TTL_SECONDS = 120  # 2 minutes

    logger.info("Starting heartbeat task")

    while True:
        try:
            now = datetime.now(UTC)
            await write_cache(
                redis, HEARTBEAT_KEY, now.isoformat(), HEARTBEAT_TTL_SECONDS
            )
            logger.debug(f"Heartbeat written at {now.isoformat()}")
        except Exception as e:
            logger.error(f"Failed to write heartbeat: {e}", exc_info=True)

        await sleep(HEARTBEAT_INTERVAL_SECONDS)


async def __main__() -> None:
    config = load_config()

    send_stream, receive_stream = create_memory_object_stream[
        ScheduleEvent | VehicleRedisSchema
    ](max_buffer_size=5000)

    redis_pool = ConnectionPool().from_url(
        f"redis://:{os.environ.get('IMT_REDIS_PASSWORD', '')}@{os.environ.get('IMT_REDIS_ENDPOINT', '')}:{int(os.environ.get('IMT_REDIS_PORT', '6379'))}"
    )

    await schema_versioner(get_redis(redis_pool))

    if os.getenv("IMT_PROMETHEUS_ENABLE", "false") == "true":
        start_http_server(int(os.getenv("IMT_PROM_PORT", "8000")))
    async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
        async with create_task_group() as tg:
            for stop in config.stops:
                if stop.schedule_only:
                    start_task(
                        get_redis(redis_pool),
                        TaskType.SCHEDULES,
                        send_stream,
                        tg,
                        session,
                        config,
                        stop,
                    )
                else:
                    start_task(
                        get_redis(redis_pool),
                        TaskType.SCHEDULE_PREDICTIONS,
                        send_stream,
                        tg,
                        session,
                        config,
                        stop,
                    )
            if config.vehicles_by_route:
                for route_id in config.vehicles_by_route:
                    start_task(
                        get_redis(redis_pool),
                        TaskType.VEHICLES,
                        send_stream,
                        tg,
                        session,
                        config,
                        stop,
                        route_id,
                    )
                # Start alerts SSE watchers for each configured route
                for route_id in config.vehicles_by_route:
                    tg.start_soon(
                        watch_alerts, get_redis(redis_pool), route_id, session, config
                    )

            # Start track prediction precaching if enabled
            if config.enable_track_predictions:
                tg.start_soon(
                    precache_track_predictions_runner,
                    get_redis(redis_pool),
                    tg,
                    config.track_prediction_routes,
                    config.track_prediction_stations,
                    config.track_prediction_interval_hours,
                )

            # Start ML worker only if enabled via env IMT_ML
            if os.getenv("IMT_ML", "").strip().lower() in {"1", "true", "yes", "on"}:
                track_predictor = TrackPredictor(get_redis(redis_pool))
                await track_predictor.initialize()
                tg.start_soon(track_predictor.ml_prediction_worker)

            # consumer
            tg.start_soon(process_queue_async, receive_stream, tg)

            tg.start_soon(background_refresh, get_redis(redis_pool), tg)

            # Start heartbeat task for healthcheck monitoring
            tg.start_soon(heartbeat_task, get_redis(redis_pool))

            next_backup = get_next_backup_time()
            # cron/timed tasks
            while True:
                now = datetime.now(ZoneInfo("America/New_York"))
                if now >= next_backup:
                    redis_backup = RedisBackup(r_client=get_redis(redis_pool))
                    filename = await redis_backup.create_backup()
                    logger.info(f"Redis backup created at {filename}")
                    # schedule next run for the next day at the configured time
                    next_backup = get_next_backup_time(now)
                # sleep until next check; if far away, sleep the whole duration
                sleep_seconds = max(1, int((next_backup - now).total_seconds()))
                await sleep(min(sleep_seconds, 60))


@click.command()
def run_main() -> None:
    try:
        run(__main__, backend="asyncio", backend_options={"use_uvloop": True})
    finally:
        # Ensure OTEL spans are flushed before exit
        if is_otel_enabled():
            shutdown_otel()
