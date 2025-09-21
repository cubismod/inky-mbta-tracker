import logging
import os
import random
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated, Self

import aiohttp
from anyio import AsyncContextManagerMixin, create_task_group
from config import load_config
from fastapi import Request
from fastapi.params import Depends
from logging_setup import setup_logging
from redis.asyncio import Redis
from track_predictor.track_predictor import TrackPredictor

# ----------------------------------------------------------------------------
# CONFIGURATION AND GLOBALS
# ----------------------------------------------------------------------------

# This is intended as a separate entrypoint to be run as a separate container
setup_logging()
logger = logging.getLogger(__name__)


class DIParams(AsyncContextManagerMixin):
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self, None]:
        async with create_task_group() as tg:
            self.tg = tg
            r_client = Redis().from_url(
                f"redis://:{os.environ.get('IMT_REDIS_PASSWORD', '')}@{os.environ.get('IMT_REDIS_ENDPOINT', '')}:{int(os.environ.get('IMT_REDIS_PORT', '6379'))}"
            )
            self.r_client = r_client
            self.config = load_config()
            self.track_predictor = TrackPredictor(r_client)
            await self.track_predictor.initialize()
            try:
                yield self
            finally:
                self.tg = None


async def get_di(request: Request):
    async with DIParams(request.app.state.session) as di:
        yield di


GET_DI = Annotated[DIParams, Depends(get_di)]

# API timeout/config flags
API_REQUEST_TIMEOUT = int(os.environ.get("IMT_API_REQUEST_TIMEOUT", "30"))  # seconds
TRACK_PREDICTION_TIMEOUT = int(os.environ.get("IMT_TRACK_PREDICTION_TIMEOUT", "15"))
RATE_LIMITING_ENABLED = os.getenv("IMT_RATE_LIMITING_ENABLED", "true").lower() == "true"
SSE_ENABLED = os.getenv("IMT_SSE_ENABLED", "true").lower() == "true"

CR_ROUTES = [
    "CR-Fairmount",
    "CR-Fitchburg",
    "CR-Franklin",
    "CR-Greenbush",
    "CR-Haverhill",
    "CR-Kingston",
    "CR-Lowell",
    "CR-Needham",
    "CR-Newburyport",
    "CR-Providence",
    "CR-Worcester",
]

CR_STATIONS = ["place-north", "place-sstat", "place-bbsta"]


# Random helper (used in lifespan tasks)
def rand_sleep(min_seconds: int, max_seconds: int) -> int:
    return random.randint(min_seconds, max_seconds)
