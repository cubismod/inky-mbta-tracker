import logging
import os
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

# ----------------------------------------------------------------------------
# CONFIGURATION AND GLOBALS
# ----------------------------------------------------------------------------

# This is intended as a separate entrypoint to be run as a separate container
setup_logging()
logger = logging.getLogger(__name__)


class DIParams(AsyncContextManagerMixin):
    def __init__(self, session: aiohttp.ClientSession, r_client: Redis) -> None:
        self.session = session
        self.r_client = r_client

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self, None]:
        try:
            async with create_task_group() as tg:
                self.tg = tg
                self.config = load_config()
                try:
                    yield self
                finally:
                    self.tg = None
        except BaseExceptionGroup as exc:
            # AnyIO groups errors raised through the dependency's yield, including
            # HTTPException and RequestValidationError. Restore singleton errors
            # so FastAPI can dispatch them to their registered exception handlers.
            error: BaseException = exc
            while isinstance(error, BaseExceptionGroup) and len(error.exceptions) == 1:
                error = error.exceptions[0]
            raise error


async def get_di(request: Request):
    async with DIParams(request.app.state.session, request.app.state.r_client) as di:
        yield di


GET_DI = Annotated[DIParams, Depends(get_di)]

RATE_LIMITING_ENABLED = os.getenv("IMT_RATE_LIMITING_ENABLED", "true").lower() == "true"
SSE_ENABLED = os.getenv("IMT_SSE_ENABLED", "true").lower() == "true"
