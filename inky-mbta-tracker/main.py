import concurrent.futures
import logging
import os
import threading
import time
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue

from config import load_config
from dotenv import load_dotenv
from mbta_client import watch_station
from schedule_tracker import ScheduleEvent, process_queue

load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), filename="imt.log", format="%(threadName)s %(levelname)-8s %(message)s")
logger = logging.getLogger(__name__)


def queue_watcher(queue: Queue[ScheduleEvent]):
    while True:
        item = queue.get()
        logging.info(item)
        time.sleep(10)


def __main__():
    config = load_config()
    workers = os.getenv("IMT_WORKERS", len(config.stops) + 2)

    exit_event = threading.Event()
    queue = Queue[ScheduleEvent]()
    try:
        with ThreadPoolExecutor(max_workers=int(workers)) as executor:
                future_results = [
                    executor.submit(
                        watch_station,
                        stop.stop_id,
                        stop.route_filter,
                        stop.direction_filter,
                        queue,
                        exit_event,
                    )
                    for stop in config.stops
                ]

                result = executor.submit(process_queue, queue, exit_event)

                future_results.append(result)
                concurrent.futures.wait(future_results)

    except KeyboardInterrupt:
        logger.error("KeyboardInterrupted detected, sending exit event")
        exit_event.set()
        return


if __name__ == "__main__":
    __main__()
