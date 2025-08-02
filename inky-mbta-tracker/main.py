import logging
import os
import re
import threading
import time
from asyncio import Runner, sleep
from datetime import UTC, datetime, timedelta
from queue import Queue
from random import randint
from typing import Optional

import click
from backup_scheduler import run_backup_scheduler
from config import StopSetup, load_config
from dotenv import load_dotenv
from prometheus import running_threads
from prometheus_client import start_http_server
from schedule_tracker import ScheduleEvent, VehicleRedisSchema, process_queue
from shared_types.schema_versioner import schema_versioner
from shared_types.shared_types import TaskType
from utils import thread_runner

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
    task: threading.Thread
    expiration_time: Optional[datetime]
    stop: Optional[StopSetup]
    route_id: Optional[str]

    """
    Wrapper around a common task that is (usually) run in a thread.
    """

    def __init__(
        self,
        task: threading.Thread,
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


def queue_watcher(queue: Queue[ScheduleEvent]) -> None:
    while True:
        item = queue.get()
        logging.info(item)
        time.sleep(10)


# launches a departures tracking thread, target should either be "schedule" or "predictions"
# returns a TaskTracker
def start_thread(  # type: ignore
    target: TaskType,
    queue: Queue[ScheduleEvent | VehicleRedisSchema],
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
                thr = threading.Thread(
                    target=thread_runner,
                    kwargs={
                        "target": target,
                        "stop_id": stop.stop_id,
                        "route": stop.route_filter,
                        "direction_filter": direction_filter,
                        "queue": queue,
                        "transit_time_min": stop.transit_time_min,
                        "show_on_display": stop.show_on_display,
                        "route_substring_filter": stop.route_substring_filter,
                    },
                    name=f"{stop.route_filter}_{stop.stop_id}_schedules",
                )
                thr.start()
                return TaskTracker(task=thr, stop=stop, event_type=target)
        case TaskType.SCHEDULE_PREDICTIONS:
            if stop:
                thr = threading.Thread(
                    target=thread_runner,
                    kwargs={
                        "target": target,
                        "queue": queue,
                        "transit_time_min": stop.transit_time_min,
                        "stop_id": stop.stop_id,
                        "route": stop.route_filter,
                        "direction_filter": direction_filter,
                        "expiration_time": exp_time,
                        "show_on_display": stop.show_on_display,
                        "route_substring_filter": stop.route_substring_filter,
                    },
                    name=f"{stop.route_filter}_{stop.stop_id}_predictions",
                )
                thr.start()
                return TaskTracker(
                    task=thr, expiration_time=exp_time, stop=stop, event_type=target
                )
        case TaskType.VEHICLES:
            thr = threading.Thread(
                target=thread_runner,
                kwargs={
                    "target": target,
                    "route": route_id,
                    "queue": queue,
                    "expiration_time": exp_time,
                },
                name=f"{route_id}_vehicles",
            )
            thr.start()
            return TaskTracker(
                task=thr,
                expiration_time=exp_time,
                route_id=route_id,
                event_type=target,
            )
        case TaskType.TRACK_PREDICTIONS:
            # This case requires special handling with config parameters
            return None  # Will be handled separately in __main__()


async def __main__() -> None:
    config = load_config()

    queue = Queue[ScheduleEvent | VehicleRedisSchema]()
    tasks = list[TaskTracker]()

    await schema_versioner()

    start_http_server(int(os.getenv("IMT_PROM_PORT", "8000")))

    for stop in config.stops:
        if stop.schedule_only:
            thr = start_thread(TaskType.SCHEDULES, stop=stop, queue=queue)
            if thr:
                tasks.append(thr)
        else:
            thr = start_thread(TaskType.SCHEDULE_PREDICTIONS, stop=stop, queue=queue)
            if thr:
                tasks.append(thr)
    if config.vehicles_by_route:
        for route_id in config.vehicles_by_route:
            thr = start_thread(
                TaskType.VEHICLES,
                route_id=route_id,
                queue=queue,
            )
            if thr:
                tasks.append(thr)

    backup_thr = threading.Thread(
        target=run_backup_scheduler, daemon=True, name="redis_backup_scheduler"
    )
    backup_thr.start()
    tasks.append(TaskTracker(backup_thr, stop=None, event_type=TaskType.REDIS_BACKUP))

    # Start track prediction precaching if enabled
    if config.enable_track_predictions:
        track_pred_thr = threading.Thread(
            target=thread_runner,
            kwargs={
                "target": TaskType.TRACK_PREDICTIONS,
                "queue": queue,
                "precache_routes": config.track_prediction_routes,
                "precache_stations": config.track_prediction_stations,
                "precache_interval_hours": config.track_prediction_interval_hours,
            },
            daemon=True,
            name="track_predictions_precache",
        )
        track_pred_thr.start()
        tasks.append(
            TaskTracker(
                track_pred_thr, stop=None, event_type=TaskType.TRACK_PREDICTIONS
            )
        )

    process_threads: list[threading.Thread] = list()
    for i in range(int(os.getenv("IMT_PROCESS_QUEUE_THREADS", "1"))):
        process_threads.append(
            threading.Thread(
                target=process_queue,
                daemon=True,
                args=[queue],
                name=f"event_processor_{i}",
            )
        )
        process_threads[i].start()
        if i < len(process_threads) - 1:
            await sleep(2)

    for process_thread in process_threads:
        tasks.append(
            TaskTracker(process_thread, stop=None, event_type=TaskType.PROCESSOR)
        )

    while True:
        running_threads.set(len(tasks))
        await sleep(30)
        for task in tasks:
            if (
                task.expiration_time
                and datetime.now().astimezone(UTC) > task.expiration_time
            ) or not task.task.is_alive():
                tasks.remove(task)
                match task.event_type:
                    case TaskType.VEHICLES:
                        thr = start_thread(
                            TaskType.VEHICLES,
                            route_id=task.route_id,
                            queue=queue,
                        )
                        if thr:
                            tasks.append(thr)
                    case TaskType.SCHEDULES:
                        thr = start_thread(
                            TaskType.SCHEDULES, stop=task.stop, queue=queue
                        )
                        if thr:
                            tasks.append(thr)
                    case TaskType.SCHEDULE_PREDICTIONS:
                        thr = start_thread(
                            TaskType.SCHEDULE_PREDICTIONS,
                            stop=task.stop,
                            queue=queue,
                        )
                        if thr:
                            tasks.append(thr)
                    case TaskType.TRACK_PREDICTIONS:
                        # Restart track predictions precaching thread
                        if config.enable_track_predictions:
                            track_pred_thr = threading.Thread(
                                target=thread_runner,
                                kwargs={
                                    "target": TaskType.TRACK_PREDICTIONS,
                                    "queue": queue,
                                    "precache_routes": config.track_prediction_routes,
                                    "precache_stations": config.track_prediction_stations,
                                    "precache_interval_hours": config.track_prediction_interval_hours,
                                },
                                daemon=True,
                                name="track_predictions_precache",
                            )
                            track_pred_thr.start()
                            tasks.append(
                                TaskTracker(
                                    track_pred_thr,
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
        with Runner() as runner:
            runner.run(__main__())
