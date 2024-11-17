import logging
import os
import threading
import time
from queue import Queue

from config import load_config
from dotenv import load_dotenv
from mbta_client import watch_static_schedule, watch_station
from prometheus_client import start_http_server
from schedule_tracker import ScheduleEvent, process_queue

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(threadName)s %(levelname)-8s %(message)s",
)
logger = logging.getLogger(__name__)


def queue_watcher(queue: Queue[ScheduleEvent]):
    while True:
        item = queue.get()
        logging.info(item)
        time.sleep(10)


def __main__():
    config = load_config()

    queue = Queue[ScheduleEvent]()

    threads = list()
    start_http_server(int(os.getenv("IMT_PROM_PORT", "8000")))
    for stop in config.stops:
        thr = None
        if stop.schedule_only:
            thr = threading.Thread(
                target=watch_static_schedule,
                args=(
                    stop.stop_id,
                    stop.route_filter,
                    stop.direction_filter,
                    queue,
                    stop.transit_time_min,
                ),
                daemon=True,
            )
        else:
            thr = threading.Thread(
                target=watch_station,
                args=(
                    stop.stop_id,
                    stop.route_filter,
                    stop.direction_filter,
                    queue,
                    stop.transit_time_min,
                ),
                daemon=True,
            )
        threads.append(thr)
        thr.start()

    thr = threading.Thread(target=process_queue, args=(queue,), daemon=True)
    threads.append(thr)
    thr.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    __main__()
