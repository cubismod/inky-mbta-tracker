import asyncio
import logging
import os
import re
from asyncio import Queue as AsyncQueue
from asyncio import Runner, sleep
from datetime import UTC, datetime, timedelta
from random import randint
from typing import Optional

import click
from backup_scheduler import run_backup_scheduler
from config import StopSetup, load_config
from dotenv import load_dotenv
from mbta_client import (
    precache_track_predictions_runner,
    watch_static_schedule,
    watch_station,
    watch_vehicles,
)
from prometheus import running_threads
from prometheus_client import start_http_server
from schedule_tracker import (
    ScheduleEvent,
    VehicleRedisSchema,
    process_queue_async,
)
from shared_types.schema_versioner import schema_versioner
from shared_types.shared_types import TaskType

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


def queue_watcher(queue: AsyncQueue[ScheduleEvent]) -> None:
    # Deprecated: kept for reference; using asyncio.Queue + async consumers now
    pass


# launches a departures tracking thread, target should either be "schedule" or "predictions"
# returns a TaskTracker
def start_task(  # type: ignore
    target: TaskType,
    queue: AsyncQueue[ScheduleEvent | VehicleRedisSchema],
    stop: Optional[StopSetup] = None,
    route_id: Optional[str] = None,
) -> Optional[TaskTracker]:
    exp_time = datetime.now().astimezone(UTC) + timedelta(
        minutes=randint(MIN_TASK_RESTART_MINS, MAX_TASK_RESTART_MINS)
    )
    direction_filter = None
    if stop and stop.direction_filter != -1:
        direction_filter = stop.direction_filter
    match target:
        case TaskType.SCHEDULES:
            if stop:
                task = asyncio.create_task(
                    watch_static_schedule(
                        stop_id=stop.stop_id,
                        route=stop.route_filter,
                        direction=direction_filter,
                        queue=queue,
                        transit_time_min=stop.transit_time_min,
                        show_on_display=stop.show_on_display,
                        route_substring_filter=stop.route_substring_filter,
                    ),
                    name=f"{stop.route_filter}_{stop.stop_id}_schedules",
                )
                return TaskTracker(task=task, stop=stop, event_type=target)
        case TaskType.SCHEDULE_PREDICTIONS:
            if stop:
                task = asyncio.create_task(
                    watch_station(
                        stop_id=stop.stop_id,
                        route=stop.route_filter,
                        direction_filter=direction_filter,
                        queue=queue,
                        transit_time_min=stop.transit_time_min,
                        expiration_time=exp_time,
                        show_on_display=stop.show_on_display,
                        route_substring_filter=stop.route_substring_filter,
                    ),
                    name=f"{stop.route_filter}_{stop.stop_id}_predictions",
                )
                return TaskTracker(
                    task=task, expiration_time=exp_time, stop=stop, event_type=target
                )
        case TaskType.VEHICLES:
            task = asyncio.create_task(
                watch_vehicles(
                    queue=queue,
                    expiration_time=exp_time,
                    route_id=route_id or "",
                ),
                name=f"{route_id}_vehicles",
            )
            return TaskTracker(
                task=task,
                expiration_time=exp_time,
                route_id=route_id,
                event_type=target,
            )
        case TaskType.TRACK_PREDICTIONS:
            # This case requires special handling with config parameters
            return None  # Will be handled separately in __main__()


async def __main__() -> None:
    config = load_config()

    # Use asyncio queue to feed async consumers
    queue: AsyncQueue[ScheduleEvent | VehicleRedisSchema] = AsyncQueue(maxsize=5000)
    tasks: list[TaskTracker] = []

    await schema_versioner()

    start_http_server(int(os.getenv("IMT_PROM_PORT", "8000")))

    for stop in config.stops:
        if stop.schedule_only:
            tk = start_task(TaskType.SCHEDULES, stop=stop, queue=queue)
            if tk:
                tasks.append(tk)
        else:
            tk = start_task(TaskType.SCHEDULE_PREDICTIONS, stop=stop, queue=queue)
            if tk:
                tasks.append(tk)
    if config.vehicles_by_route:
        for route_id in config.vehicles_by_route:
            tk = start_task(
                TaskType.VEHICLES,
                route_id=route_id,
                queue=queue,
            )
            if tk:
                tasks.append(tk)

    # Run backup scheduler via threadpool; no manual thread management
    backup_task = asyncio.create_task(
        asyncio.to_thread(run_backup_scheduler), name="redis_backup_scheduler"
    )
    tasks.append(TaskTracker(backup_task, stop=None, event_type=TaskType.REDIS_BACKUP))

    # Start track prediction precaching if enabled
    if config.enable_track_predictions:
        track_pred_task = asyncio.create_task(
            precache_track_predictions_runner(
                routes=config.track_prediction_routes,
                target_stations=config.track_prediction_stations,
                interval_hours=config.track_prediction_interval_hours,
            ),
            name="track_predictions_precache",
        )
        tasks.append(
            TaskTracker(
                track_pred_task, stop=None, event_type=TaskType.TRACK_PREDICTIONS
            )
        )

    process_tasks: list[asyncio.Task[None]] = []
    num_proc = int(os.getenv("IMT_PROCESS_QUEUE_THREADS", "1"))
    for i in range(num_proc):
        t = asyncio.create_task(process_queue_async(queue), name=f"event_processor_{i}")
        process_tasks.append(t)
        # Stagger processor start to reduce initial contention
        if i < num_proc - 1:
            await sleep(2)

    for t in process_tasks:
        tasks.append(TaskTracker(t, stop=None, event_type=TaskType.PROCESSOR))

    while True:
        running_threads.set(len(tasks))
        await sleep(30)
        # Iterate over a snapshot since we may remove/restart tasks during iteration
        for task in list(tasks):
            if (
                task.expiration_time
                and datetime.now().astimezone(UTC) > task.expiration_time
            ) or task.task.done():
                tasks.remove(task)
                match task.event_type:
                    case TaskType.VEHICLES:
                        tk = start_task(
                            TaskType.VEHICLES,
                            route_id=task.route_id,
                            queue=queue,
                        )
                        if tk:
                            tasks.append(tk)
                    case TaskType.SCHEDULES:
                        tk = start_task(TaskType.SCHEDULES, stop=task.stop, queue=queue)
                        if tk:
                            tasks.append(tk)
                    case TaskType.SCHEDULE_PREDICTIONS:
                        tk = start_task(
                            TaskType.SCHEDULE_PREDICTIONS,
                            stop=task.stop,
                            queue=queue,
                        )
                        if tk:
                            tasks.append(tk)
                    case TaskType.TRACK_PREDICTIONS:
                        # Restart track predictions precaching thread
                        if config.enable_track_predictions:
                            track_pred_task = asyncio.create_task(
                                precache_track_predictions_runner(
                                    routes=config.track_prediction_routes,
                                    target_stations=config.track_prediction_stations,
                                    interval_hours=config.track_prediction_interval_hours,
                                ),
                                name="track_predictions_precache",
                            )
                            tasks.append(
                                TaskTracker(
                                    track_pred_task,
                                    stop=None,
                                    event_type=TaskType.TRACK_PREDICTIONS,
                                )
                            )


@click.command()
@click.option("--api-server", is_flag=True, default=False)
def run_main(api_server: bool) -> None:
    if api_server:
        import api_server as server

        server.run_main()
    else:
        # Install uvloop if available for better async performance
        try:
            import uvloop

            uvloop.install()
            logging.info("uvloop installed as event loop policy")
        except Exception:
            logging.info("uvloop not available; using default event loop")
        with Runner() as runner:
            runner.run(__main__())
