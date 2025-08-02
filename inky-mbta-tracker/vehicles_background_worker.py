import logging
from asyncio import sleep
from datetime import datetime, timedelta
from enum import Enum
from queue import Queue

from utils import get_redis, get_vehicles_data

logger = logging.getLogger(__name__)


class State(Enum):
    IDLE = 0
    TRAFFIC = 1


class BackgroundWorker:
    """
    Background worker for fetching vehicle data from the MBTA API using a queue to communicate with the API server.
    The worker spins up based on web traffic.
    """

    queue: Queue[State]
    state: State = State.IDLE
    state_expiration: datetime = datetime.now() + timedelta(seconds=60)

    def __init__(self) -> None:
        self.queue = Queue()
        self.redis = get_redis()

    async def run(self) -> None:
        while True:
            if datetime.now() > self.state_expiration and self.state == State.TRAFFIC:
                self.state = State.IDLE
                self.state_expiration = datetime.now() + timedelta(seconds=60)
                logger.info("Vehicle background worker is now idle")
            while self.queue.qsize() > 0:
                item = self.queue.get()
                await self.process(item)
                self.queue.task_done()
            if self.state == State.TRAFFIC:
                _ = await get_vehicles_data(self.redis)
            else:
                await sleep(4)

    async def process(self, item: State) -> None:
        prev_state = self.state
        self.state = item
        if self.state == State.TRAFFIC:
            if prev_state == State.IDLE:
                logger.info("Started vehicle background worker")
            self.state_expiration = datetime.now() + timedelta(seconds=60)


async def run_background_worker(queue: Queue[State]) -> None:
    worker = BackgroundWorker()
    worker.queue = queue
    await worker.run()
