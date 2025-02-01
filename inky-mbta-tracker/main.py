import logging
import os
import threading
import time
import uuid
from asyncio import Runner, sleep
from datetime import UTC, datetime, timedelta
from pathlib import Path
from queue import Queue
from random import getrandbits, randint
from typing import Optional

import yappi
from config import StopSetup, load_config
from dotenv import load_dotenv
from geojson_creator import run
from mbta_client import EventType, thread_runner
from prometheus import running_threads
from prometheus_client import start_http_server
from schedule_tracker import ScheduleEvent, VehicleRedisSchema, process_queue
from zoneinfo import ZoneInfo

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

MIN_TASK_RESTART_MINS = 45
MAX_TASK_RESTART_MINS = 120


class TaskTracker:
    event_type: EventType
    task: threading.Thread
    expiration_time: Optional[datetime]
    stop: Optional[StopSetup]
    route_id: Optional[str]

    def __init__(
        self,
        task: threading.Thread,
        event_type: EventType,
        expiration_time: Optional[datetime] = None,
        stop: Optional[StopSetup] = None,
        route_id: Optional[str] = None,
    ):
        self.task = task
        self.expiration_time = expiration_time
        self.stop = stop
        self.route_id = route_id
        self.event_type = event_type


def queue_watcher(queue: Queue[ScheduleEvent]):
    while True:
        item = queue.get()
        logging.info(item)
        time.sleep(10)


# launches a departures tracking thread, target should either be "schedule" or "predictions"
# returns a TaskTracker
def start_thread(
    target: EventType,
    queue: Queue[ScheduleEvent | VehicleRedisSchema],
    stop: Optional[StopSetup] = None,
    route_id: Optional[str] = None,
):
    exp_time = datetime.now().astimezone(UTC) + timedelta(
        minutes=randint(MIN_TASK_RESTART_MINS, MAX_TASK_RESTART_MINS)
    )
    match target:
        case EventType.SCHEDULES:
            thr = threading.Thread(
                target=thread_runner,
                kwargs={
                    "target": target,
                    "stop_id": stop.stop_id,
                    "route": stop.route_filter,
                    "direction": stop.direction_filter,
                    "queue": queue,
                    "transit_time_min": stop.transit_time_min,
                },
            )
            thr.start()
            return TaskTracker(task=thr, stop=stop, event_type=target)
        case EventType.PREDICTIONS:
            thr = threading.Thread(
                target=thread_runner,
                kwargs={
                    "target": target,
                    "queue": queue,
                    "transit_time_min": stop.transit_time_min,
                    "stop_id": stop.stop_id,
                    "route": stop.route_filter,
                    "direction": stop.direction_filter,
                    "expiration_time": exp_time,
                },
            )
            thr.start()
            return TaskTracker(
                task=thr, expiration_time=exp_time, stop=stop, event_type=target
            )
        case EventType.VEHICLES:
            thr = threading.Thread(
                target=thread_runner,
                kwargs={
                    "target": target,
                    "route": route_id,
                    "queue": queue,
                    "expiration_time": exp_time,
                },
            )
            thr.start()
            return TaskTracker(
                task=thr,
                expiration_time=exp_time,
                route_id=route_id,
                event_type=target,
            )


async def __main__():
    config = load_config()

    profile_dir = os.getenv("IMT_PROFILE_DIR")
    start_time = datetime.now().astimezone(ZoneInfo("US/Eastern"))
    if profile_dir:
        yappi.start()

    queue = Queue[ScheduleEvent | VehicleRedisSchema]()
    tasks = list[TaskTracker]()

    start_http_server(int(os.getenv("IMT_PROM_PORT", "8000")))

    process_thr = threading.Thread(target=process_queue, daemon=True, args=[queue])
    process_thr.start()

    tasks.append(TaskTracker(process_thr, stop=None, event_type=EventType.OTHER))
    for stop in config.stops:
        if stop.schedule_only:
            tasks.append(start_thread(EventType.SCHEDULES, stop=stop, queue=queue))
        else:
            tasks.append(start_thread(EventType.PREDICTIONS, stop=stop, queue=queue))
    if config.vehicles_by_route:
        for route_id in config.vehicles_by_route:
            tasks.append(
                start_thread(
                    EventType.VEHICLES,
                    route_id=route_id,
                    queue=queue,
                )
            )
        geojson_thr = threading.Thread(target=run, daemon=True, args=[config])
        geojson_thr.start()
        tasks.append(TaskTracker(geojson_thr, stop=None, event_type=EventType.OTHER))

    next_profile_time = datetime.now().astimezone(UTC) + timedelta(seconds=10)
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
                    case EventType.VEHICLES:
                        tasks.append(
                            start_thread(
                                EventType.VEHICLES,
                                route_id=task.route_id,
                                queue=queue,
                            )
                        )
                    case EventType.SCHEDULES:
                        tasks.append(
                            start_thread(
                                EventType.SCHEDULES, stop=task.stop, queue=queue
                            )
                        )
                    case EventType.PREDICTIONS:
                        tasks.append(
                            start_thread(
                                EventType.PREDICTIONS, stop=task.stop, queue=queue
                            )
                        )
        if profile_dir and datetime.now().astimezone(UTC) > next_profile_time:
            if not yappi.is_running():
                yappi.start()
            logging.info("started profiling")
            await sleep(180)
            yappi.stop()
            logging.info("stopped profiling")
            end_time = datetime.now().astimezone(ZoneInfo("US/Eastern"))

            file_name = (
                Path(profile_dir)
                / f"{uuid.UUID(int=getrandbits(128), version=4)}-profile.txt"
            )

            with open(file_name, "w") as f:
                f.write(f"{start_time.strftime('%c')} to {end_time.strftime('%c')}")
                threads = yappi.get_thread_stats().sort("id", "asc")
                threads.print_all(out=f)
                for thread in threads:
                    stats = yappi.get_func_stats(
                        ctx_id=thread.id,
                        filter_callback=lambda x: "lib" not in x.module,
                    )
                    if len(stats) > 0:
                        f.write(f"\nStats for Thread {thread.id}")
                        stats.print_all(out=f)
                next_profile_time = datetime.now().astimezone(UTC) + timedelta(
                    minutes=randint(30, 60)
                )
            yappi.clear_stats()


if __name__ == "__main__":
    with Runner() as runner:
        runner.run(__main__())
