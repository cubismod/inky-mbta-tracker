import logging
import os
import threading
import time
from argparse import ArgumentParser
from asyncio import CancelledError, Runner, Task, TaskGroup, sleep
from datetime import UTC, datetime, timedelta
from queue import Queue
from random import randint
from typing import Optional

from config import StopSetup, load_config
from dotenv import load_dotenv
from mbta_client import watch_static_schedule, watch_station
from prometheus_client import start_http_server
from schedule_tracker import ScheduleEvent, process_queue

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

MIN_TASK_RESTART_MINS = 60
MAX_TASK_RESTART_MINS = 180


class TaskTracker:
    task: Task
    expiration_time: Optional[datetime]
    stop: Optional[StopSetup]

    def __init__(
        self, task: Task, expiration_time: Optional[datetime], stop: Optional[StopSetup]
    ):
        self.task = task
        self.expiration_time = expiration_time
        self.stop = stop


def queue_watcher(queue: Queue[ScheduleEvent]):
    while True:
        item = queue.get()
        logging.info(item)
        time.sleep(10)


async def __main__():
    config = load_config()

    parser = ArgumentParser(
        prog="inky-mbta-tracker",
        description="a program which uses the MBTA API to track transit departures",
    )
    parser.add_argument("-p", "--profile_s")
    args = parser.parse_args()
    kill_time = None
    if args.profile_s:
        kill_time = datetime.now().astimezone(UTC) + timedelta(
            seconds=int(args.profile_s)
        )
        logging.info(f"Profiling for {args.profile_s}s")

    queue = Queue[ScheduleEvent]()
    tasks = set[TaskTracker]()

    start_http_server(int(os.getenv("IMT_PROM_PORT", "8000")))
    async with TaskGroup() as tg:
        # all the prediction/schedule watchers run on the main thread using async awaits while
        # the process_queue task runs on a separate thread to ensure updates continue in realtime
        thr = threading.Thread(target=process_queue, daemon=True, args=[queue])
        thr.start()

        for stop in config.stops:
            if stop.schedule_only:
                task = tg.create_task(
                    watch_static_schedule(
                        stop.stop_id,
                        stop.route_filter,
                        stop.direction_filter,
                        queue,
                        stop.transit_time_min,
                    )
                )
                tasks.add(TaskTracker(task=task, expiration_time=None, stop=stop))
            else:
                task = tg.create_task(
                    watch_station(
                        stop.stop_id,
                        stop.route_filter,
                        stop.direction_filter,
                        queue,
                        stop.transit_time_min,
                    )
                )
                tasks.add(
                    TaskTracker(
                        task=task,
                        expiration_time=datetime.now().astimezone(UTC)
                        + timedelta(
                            minutes=randint(
                                MIN_TASK_RESTART_MINS, MAX_TASK_RESTART_MINS
                            )
                        ),
                        stop=stop,
                    )
                )

        while True:
            await sleep(30)
            if kill_time and datetime.now().astimezone(UTC) > kill_time:
                for task in tasks:
                    try:
                        task.task.cancel()
                    except CancelledError:
                        await task.task
                        tasks.remove(task)
                return
            for task in tasks:
                if (
                    task.expiration_time
                    and datetime.now().astimezone(UTC) > task.expiration_time
                ):
                    try:
                        logger.info(f"Restarting task for {task.stop}")
                        # restart the task
                        task.task.cancel()
                        await task.task
                    except CancelledError:
                        new_task = tg.create_task(
                            watch_station(
                                task.stop.stop_id,
                                task.stop.route_filter,
                                task.stop.direction_filter,
                                queue,
                                task.stop.transit_time_min,
                            )
                        )
                        tasks.remove(task)
                        tasks.add(
                            TaskTracker(
                                task=new_task,
                                expiration_time=datetime.now().astimezone(UTC)
                                + timedelta(
                                    minutes=randint(
                                        MIN_TASK_RESTART_MINS, MAX_TASK_RESTART_MINS
                                    )
                                ),
                                stop=task.stop,
                            )
                        )


if __name__ == "__main__":
    with Runner() as runner:
        runner.run(__main__())
