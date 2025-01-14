import logging
import os
import threading
import time
from asyncio import Runner, sleep
from datetime import UTC, datetime, timedelta
from queue import Queue
from random import randint
from typing import Optional

from config import StopSetup, load_config
from dotenv import load_dotenv
from mbta_client import thread_runner
from prometheus import running_threads
from prometheus_client import start_http_server
from pytz import timezone
from schedule_tracker import ScheduleEvent, process_queue

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)

MIN_TASK_RESTART_MINS = 45
MAX_TASK_RESTART_MINS = 120


class TaskTracker:
    task: threading.Thread
    expiration_time: Optional[datetime]
    stop: Optional[StopSetup]

    def __init__(
        self,
        task: threading.Thread,
        expiration_time: Optional[datetime] = None,
        stop: Optional[StopSetup] = None,
    ):
        self.task = task
        self.expiration_time = expiration_time
        self.stop = stop
        if self.expiration_time:
            logger.info(
                f"{stop.stop_id}/{stop.route_filter} will restart at {expiration_time.astimezone(timezone('US/Eastern')).strftime('%c')}"
            )


def queue_watcher(queue: Queue[ScheduleEvent]):
    while True:
        item = queue.get()
        logging.info(item)
        time.sleep(10)


# launches a departures tracking thread, target should either be "schedule" or "predictions"
# returns a TaskTracker
def start_thread(target: str, stop: StopSetup, queue: Queue[ScheduleEvent]):
    match target:
        case "schedule":
            thr = threading.Thread(
                target=thread_runner,
                args=[
                    target,
                    stop.stop_id,
                    stop.route_filter,
                    stop.direction_filter,
                    queue,
                    stop.transit_time_min,
                ],
            )
            thr.start()
            return TaskTracker(task=thr, stop=stop)
        case "predictions":
            exp_time = datetime.now().astimezone(UTC) + timedelta(
                minutes=randint(MIN_TASK_RESTART_MINS, MAX_TASK_RESTART_MINS)
            )
            thr = threading.Thread(
                target=thread_runner,
                args=[
                    target,
                    stop.stop_id,
                    stop.route_filter,
                    stop.direction_filter,
                    queue,
                    stop.transit_time_min,
                    exp_time,
                ],
            )
            thr.start()
            return TaskTracker(task=thr, expiration_time=exp_time, stop=stop)


async def __main__():
    config = load_config()

    queue = Queue[ScheduleEvent]()
    tasks = list[TaskTracker]()

    start_http_server(int(os.getenv("IMT_PROM_PORT", "8000")))

    process_thr = threading.Thread(target=process_queue, daemon=True, args=[queue])
    process_thr.start()

    tasks.append(TaskTracker(process_thr, stop=None))
    for stop in config.stops:
        if stop.schedule_only:
            tasks.append(start_thread("schedule", stop, queue))
        else:
            tasks.append(start_thread("predictions", stop, queue))

    while True:
        running_threads.set(len(tasks))
        await sleep(30)
        for task in tasks:
            if (
                task.expiration_time
                and datetime.now().astimezone(UTC) > task.expiration_time
            ) or not task.task.is_alive():
                tasks.remove(task)
                tasks.append(start_thread("predictions", task.stop, queue))


if __name__ == "__main__":
    with Runner() as runner:
        runner.run(__main__())
