import logging
import random
from datetime import datetime, timedelta
from enum import Enum
from queue import Queue

from anyio import create_task_group, sleep
from anyio.abc import TaskGroup
from utils import get_redis, get_vehicles_data

logger = logging.getLogger(__name__)


class State(Enum):
    IDLE = 0
    TRAFFIC = 1
    SHUTDOWN = 2


class BackgroundWorker:
    """
    Background worker for fetching vehicle data from the MBTA API using a queue to communicate with the API server.
    The worker spins up based on web traffic.
    """

    queue: Queue[State]
    state: State = State.IDLE
    state_expiration: datetime = datetime.now() + timedelta(seconds=60)
    shutdown: bool = False

    def __init__(self) -> None:
        self.queue = Queue()
        self.redis = get_redis()

    async def run(self, tg: TaskGroup) -> None:
        while not self.shutdown:
            if datetime.now() > self.state_expiration and self.state == State.TRAFFIC:
                self.state = State.IDLE
                self.state_expiration = datetime.now() + timedelta(seconds=60)
                logger.info("Vehicle background worker is now idle")
            while self.queue.qsize() > 0:
                item = self.queue.get()
                await self.process(item)
                self.queue.task_done()
            if self.state == State.TRAFFIC:
                tg.start_soon(get_vehicles_data, self.redis)
                await sleep(random.randint(1, 4))
            else:
                await sleep(random.randint(5, 60))

    async def process(self, item: State) -> None:
        prev_state = self.state
        self.state = item
        if self.state == State.SHUTDOWN:
            self.shutdown = True
            return
        if self.state == State.TRAFFIC:
            if prev_state == State.IDLE:
                logger.info("Started vehicle background worker")
            self.state_expiration = datetime.now() + timedelta(seconds=60)


async def run_background_worker(queue: Queue[State]) -> None:
    async with create_task_group() as tg:
        worker = BackgroundWorker()
        worker.queue = queue
        tg.start_soon(worker.run, tg)
