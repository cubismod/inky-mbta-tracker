import logging
import os
import socket
from datetime import UTC, datetime

import aiohttp
from anyio import sleep
from api.models import TotalsByLine
from config import Config
from redis.asyncio import Redis
from vehicle_counting import get_vehicle_route_counts

logger = logging.getLogger(__name__)

NTFY_LIFECYCLE_URL = os.getenv("NTFY_LIFECYCLE_URL")
NTFY_BEARER_TOKEN = os.getenv("NTFY_BEARER_TOKEN")


async def send_ntfy_message(message: str) -> None:
    if not NTFY_LIFECYCLE_URL:
        return
    headers: dict[str, str] = {
        "X-Title": "inky-mbta-tracker",
        "X-Tags": "white_check_mark",
    }
    if NTFY_BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {NTFY_BEARER_TOKEN}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                NTFY_LIFECYCLE_URL,
                data=message.encode(),
                headers=headers,
            ) as response:
                if response.status >= 400:
                    logger.warning("Failed to send Ntfy message")
    except aiohttp.ClientError:
        logger.warning("Failed to send Ntfy message", exc_info=True)


async def notify_startup() -> None:
    if not NTFY_LIFECYCLE_URL:
        return

    hostname = socket.gethostname()
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    message = f"Worker started on {hostname} at {timestamp}"

    headers: dict[str, str] = {
        "X-Title": "inky-mbta-tracker",
        "X-Tags": "white_check_mark",
    }
    if NTFY_BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {NTFY_BEARER_TOKEN}"

    await send_ntfy_message(message)


async def service_start_stop_watcher(r_client: Redis, config: Config) -> None:
    # delay 10 min for vehicle data to load on cold boot
    await sleep(10 * 60)
    counts = await get_vehicle_route_counts(r_client, config)

    service_statuses: dict[str, bool] = {}

    async def update_service_statuses(
        counts: TotalsByLine, statuses: dict[str, bool], send: bool
    ) -> None:
        for k, v in counts.model_dump().items():
            existing = statuses.get(k)
            if v > 0:
                statuses[k] = True
                if send and existing is not None and not existing:
                    await send_ntfy_message(f"Service {k} started")
            else:
                statuses[k] = False
                if send and existing is not None and existing:
                    await send_ntfy_message(f"Service {k} stopped")

    await update_service_statuses(counts[1], service_statuses, False)
    while True:
        counts = await get_vehicle_route_counts(r_client, config)
        await update_service_statuses(counts[1], service_statuses, True)
        await sleep(5 * 60)
