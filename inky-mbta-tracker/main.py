import asyncio
import logging
import os
import re
from datetime import UTC, datetime, timedelta
from random import randint
from typing import Optional

import click
from anyio import (
    create_memory_object_stream,
    create_task_group,
    run,
)
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectSendStream
from backup_scheduler import BackupScheduler
from config import StopSetup, load_config
from dotenv import load_dotenv
from mbta_client import (
    precache_track_predictions_runner,
    watch_static_schedule,
    watch_station,
    watch_vehicles,
)
from prometheus_client import start_http_server
from redis.asyncio.connection import ConnectionPool
from schedule_tracker import (
    ScheduleEvent,
    VehicleRedisSchema,
    process_queue_async,
)
from shared_types.schema_versioner import schema_versioner
from shared_types.shared_types import TaskType
from utils import get_redis

load_dotenv()


class APIKeyFilter(logging.Filter):
    """Filter to remove API keys from log messages."""

    def __init__(self, name: str = "", mask: str = "[REDACTED]") -> None:
        super().__init__(name)
        self.mask = mask
        self.patterns = [
            re.compile(r"api_key=([^&\s]+)", re.IGNORECASE),
            re.compile(r"Bearer\s+([^\s]+)", re.IGNORECASE),
            re.compile(r'AUTH_TOKEN[\'"]?\s*[:=]\s*[\'"]?([^\'"\s&]+)', re.IGNORECASE),
        ]

    def filter(self, record: logging.LogRecord) -> bool:
        if hasattr(record, "msg") and record.msg:
            record.msg = self._sanitize_message(str(record.msg))

        if hasattr(record, "args") and record.args:
            sanitized_args: list[object] = []
            for arg in record.args:
                if isinstance(arg, str):
                    sanitized_args.append(self._sanitize_message(arg))
                else:
                    sanitized_args.append(arg)
            record.args = tuple(sanitized_args)

        return True

    def _sanitize_message(self, message: str) -> str:
        for pattern in self.patterns:
            message = pattern.sub(
                lambda m: m.group(0).replace(m.group(1), self.mask), message
            )
        return message


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)-8s %(message)s",
)

# Add API key filter to all loggers
api_filter = APIKeyFilter()
logging.getLogger().addFilter(api_filter)

# Also add to all existing handlers
for handler in logging.getLogger().handlers:
    handler.addFilter(api_filter)

logger = logging.getLogger(__name__)

MIN_TASK_RESTART_MINS = 45
MAX_TASK_RESTART_MINS = 120


class TaskTracker:
    event_type: TaskType
    task: asyncio.Task[None]
    expiration_time: Optional[datetime]
    stop: Optional[StopSetup]
    route_id: Optional[str]

    """
    Wrapper around a common task that is running as an asyncio Task.
    """

    def __init__(
        self,
        task: asyncio.Task[None],
        event_type: TaskType,
        expiration_time: Optional[datetime] = None,
        stop: Optional[StopSetup] = None,
        route_id: Optional[str] = None,
    ):
        self.task = task
        self.expiration_time = expiration_time
        self.stop = stop
        self.route_id = route_id
        self.event_type = event_type


# launches a departures tracking task, target should either be "schedule" or "predictions"
def start_task(
    target: TaskType,
    send_stream: MemoryObjectSendStream[ScheduleEvent | VehicleRedisSchema],
    tg: TaskGroup,
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
                    stop.stop_id,
                    stop.route_filter,
                    direction_filter,
                    send_stream,
                    stop.transit_time_min,
                    stop.show_on_display,
                    tg,
                    stop.route_substring_filter,
                )
        case TaskType.SCHEDULE_PREDICTIONS:
            if stop:
                tg.start_soon(
                    watch_station,
                    stop.stop_id,
                    stop.route_filter,
                    direction_filter,
                    send_stream,
                    stop.transit_time_min,
                    exp_time,
                    stop.show_on_display,
                    tg,
                    stop.route_substring_filter,
                )
        case TaskType.VEHICLES:
            tg.start_soon(watch_vehicles, send_stream, exp_time, route_id or "")


async def __main__() -> None:
    config = load_config()

    send_stream, receive_stream = create_memory_object_stream[
        ScheduleEvent | VehicleRedisSchema
    ](max_buffer_size=5000)

    redis_pool = ConnectionPool().from_url(
        f"redis://:{os.environ.get('IMT_REDIS_PASSWORD', '')}@{os.environ.get('IMT_REDIS_ENDPOINT', '')}:{int(os.environ.get('IMT_REDIS_PORT', '6379'))}"
    )

    await schema_versioner(get_redis(redis_pool))

    start_http_server(int(os.getenv("IMT_PROM_PORT", "8000")))
    async with create_task_group() as tg:
        for stop in config.stops:
            if stop.schedule_only:
                start_task(TaskType.SCHEDULES, send_stream, tg, stop)
            else:
                start_task(
                    TaskType.SCHEDULE_PREDICTIONS,
                    send_stream,
                    tg,
                    stop,
                )
        if config.vehicles_by_route:
            for route_id in config.vehicles_by_route:
                start_task(
                    TaskType.VEHICLES,
                    send_stream,
                    tg,
                    stop,
                    route_id,
                )

        # Run backup scheduler as native asyncio task for proper cancellation
        # Parse backup time from env (default 03:00, America/New_York)
        from datetime import time as dt_time

        backup_time_str = os.getenv("IMT_REDIS_BACKUP_TIME", "03:00")
        if isinstance(backup_time_str, str):
            try:
                hour, minute = map(int, backup_time_str.split(":"))
                backup_time = dt_time(hour=hour, minute=minute)
            except Exception:
                logger.error(
                    f"Invalid IMT_REDIS_BACKUP_TIME format: {backup_time_str}, using default 03:00"
                )
                backup_time = dt_time(hour=3, minute=0)
        else:
            backup_time = dt_time(hour=3, minute=0)

        backup_scheduler = BackupScheduler(
            tg, get_redis(redis_pool), backup_time=backup_time
        )
        tg.start_soon(backup_scheduler.run_scheduler, tg)

        # Start track prediction precaching if enabled
        if config.enable_track_predictions:
            tg.start_soon(
                precache_track_predictions_runner,
                config.track_prediction_routes,
                config.track_prediction_stations,
                config.track_prediction_interval_hours,
            )

        # consumer
        await process_queue_async(receive_stream, tg)


@click.command()
@click.option("--api-server", is_flag=True, default=False)
def run_main(api_server: bool) -> None:
    if api_server:
        import api_server as server

        run(server.run_main)
    else:
        run(__main__, backend="asyncio", backend_options={"use_uvloop": True})
