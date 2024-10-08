import logging
import os
import sys
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue
from schedule_tracker import ScheduleEvent
from dotenv import load_dotenv
from pydantic import ValidationError
from config import load_config
from mbta_client import watch_station


def __main__():
    load_dotenv()

    try:
        config = load_config()
        workers = os.getenv("IMT_WORKERS", "4")

        queue = Queue[ScheduleEvent]()

        with ThreadPoolExecutor(max_workers=int(workers)) as executor:
            future_results = {
                executor.submit(
                    watch_station,
                    stop.stop_id,
                    stop.route_filter,
                    stop.direction_filter,
                    queue,
                ): stop
                for stop in config.stops
            }


    except ValidationError as err:
        logging.error(f"Unable to load the configuration file, {err}")
        sys.exit(1)


if __name__ == "__main__":
    __main__()
