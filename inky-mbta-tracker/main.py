import logging
import os
import threading
import time
from asyncio import CancelledError, Runner, Task, TaskGroup, sleep
from datetime import UTC, datetime, timedelta
from queue import Queue
from random import randint
from typing import Optional

import yappi
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

MIN_TASK_RESTART_MINS = 45
MAX_TASK_RESTART_MINS = 120
PROFILE_TIME = 20


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

    queue = Queue[ScheduleEvent]()
    tasks = set[TaskTracker]()

    start_http_server(int(os.getenv("IMT_PROM_PORT", "8000")))
    async with TaskGroup() as tg:
        try:
            if os.environ.get("IMT_PROFILE") == "true":
                yappi.set_clock_type("WALL")
                yappi.start()
            profile_time = datetime.now().astimezone(UTC) + timedelta(
                minutes=PROFILE_TIME
            )
            # all the prediction/schedule watchers run on the main thread using async awaits while
            # the process_queue task runs on a separate thread to ensure updates continue in realtime
            threading.Thread(target=process_queue, daemon=True, args=[queue]).start()

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
                    if (
                        os.environ.get("IMT_PROFILE") == "true"
                        and datetime.now().astimezone(UTC) > profile_time
                    ):
                        with open("imt_profile.txt", "w") as profile_doc:
                            profile_doc.write(datetime.now().strftime("%c"))
                            yappi.get_func_stats().print_all(out=profile_doc)
                            profile_time = datetime.now().astimezone(UTC) + timedelta(
                                minutes=PROFILE_TIME
                            )
        except KeyboardInterrupt:
            if os.environ.get("IMT_PROFILE") == "true":
                yappi.stop()


if __name__ == "__main__":
    with Runner() as runner:
        runner.run(__main__())
