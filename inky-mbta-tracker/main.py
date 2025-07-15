import logging
import os
import threading
import time
from asyncio import Runner, sleep
from datetime import UTC, datetime, timedelta
from enum import Enum
from queue import Queue
from random import randint
from typing import Optional

import click
from config import StopSetup, load_config
from dotenv import load_dotenv
from geojson_creator import run
from mbta_client import thread_runner
from prometheus import running_threads
from prometheus_client import start_http_server
from schedule_tracker import ScheduleEvent, VehicleRedisSchema, process_queue
from shared_types.schema_versioner import schema_versioner
from track_predictor import track_prediction_api

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

MIN_TASK_RESTART_MINS = 45
MAX_TASK_RESTART_MINS = 120


class TrackerType(Enum):
    OTHER = -1
    SCHEDULE_PREDICTIONS = 0
    SCHEDULES = 1
    VEHICLES = 2
    PROCESSOR = 3
    LIGHT_STOP = 4
    GEOJSON = 5
    TRACK_PREDICTIONS = 6


class TaskTracker:
    event_type: TrackerType
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
        event_type: TrackerType,
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
    target: TrackerType,
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
        case TrackerType.SCHEDULES:
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
        case TrackerType.SCHEDULE_PREDICTIONS:
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
        case TrackerType.VEHICLES:
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


async def __main__() -> None:
    config = load_config()

    queue = Queue[ScheduleEvent | VehicleRedisSchema]()
    tasks = list[TaskTracker]()

    await schema_versioner()

    start_http_server(int(os.getenv("IMT_PROM_PORT", "8000")))

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

    for process_thread in process_threads:
        tasks.append(
            TaskTracker(process_thread, stop=None, event_type=TrackerType.PROCESSOR)
        )
    for stop in config.stops:
        if stop.schedule_only:
            thr = start_thread(TrackerType.SCHEDULES, stop=stop, queue=queue)
            if thr:
                tasks.append(thr)
        else:
            thr = start_thread(TrackerType.SCHEDULE_PREDICTIONS, stop=stop, queue=queue)
            if thr:
                tasks.append(thr)
    if config.vehicles_by_route:
        for route_id in config.vehicles_by_route:
            thr = start_thread(
                TrackerType.VEHICLES,
                route_id=route_id,
                queue=queue,
            )
            if thr:
                tasks.append(thr)
        geojson_thr = threading.Thread(
            target=run, daemon=True, args=[config], name="geojson"
        )
        geojson_thr.start()
        tasks.append(
            TaskTracker(geojson_thr, stop=None, event_type=TrackerType.GEOJSON)
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
                    case TrackerType.VEHICLES:
                        thr = start_thread(
                            TrackerType.VEHICLES,
                            route_id=task.route_id,
                            queue=queue,
                        )
                        if thr:
                            tasks.append(thr)
                    case TrackerType.SCHEDULES:
                        thr = start_thread(
                            TrackerType.SCHEDULES, stop=task.stop, queue=queue
                        )
                        if thr:
                            tasks.append(thr)
                    case TrackerType.SCHEDULE_PREDICTIONS:
                        thr = start_thread(
                            TrackerType.SCHEDULE_PREDICTIONS,
                            stop=task.stop,
                            queue=queue,
                        )
                        if thr:
                            tasks.append(thr)


@click.command()
@click.option("--prediction-api", is_flag=True, default=False)
def run_main(prediction_api: bool) -> None:
    if prediction_api:
        track_prediction_api.run_main()
    else:
        with Runner() as runner:
            runner.run(__main__())
