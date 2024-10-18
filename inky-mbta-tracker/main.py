import concurrent.futures
import logging
import os
import sys
import time
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue

from config import load_config
from dotenv import load_dotenv
from mbta_client import watch_station
from pydantic import ValidationError
from schedule_tracker import ScheduleEvent, process_queue

load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


def queue_watcher(queue: Queue[ScheduleEvent]):
    while True:
        item = queue.get()
        logging.info(item)
        time.sleep(10)


def __main__():

    try:
        config = load_config()
        workers = os.getenv("IMT_WORKERS", len(config.stops) + 2)

        queue = Queue[ScheduleEvent]()

        with ThreadPoolExecutor(max_workers=int(workers)) as executor:
            future_results = [
                executor.submit(
                    watch_station,
                    stop.stop_id,
                    stop.route_filter,
                    stop.direction_filter,
                    queue,
                ) for stop in config.stops
            ]

            result = executor.submit(
                process_queue,
                queue
            )

            future_results.append(result)

            for future in concurrent.futures.as_completed(future_results):
                logging.info(f"thread finished with: {future.result()}")

    except ValidationError as err:
        logger.error(f"Unable to load the configuration file, {err}")
        sys.exit(1)


if __name__ == "__main__":
    __main__()
