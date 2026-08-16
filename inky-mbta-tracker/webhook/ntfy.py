import logging
import os
import socket
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import aiohttp
from anyio import sleep
from api.models import TotalsByLine
from config import Config
from redis.asyncio import Redis
from vehicle_counting import get_vehicle_route_counts

logger = logging.getLogger(__name__)

NTFY_LIFECYCLE_URL = os.getenv("NTFY_LIFECYCLE_URL")
NTFY_BEARER_TOKEN = os.getenv("NTFY_BEARER_TOKEN")

SERVICE_LINES = ("RL", "GL", "BL", "OL", "SL", "CR")
CONFIRMATION_SAMPLES = 3


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

    service_statuses: dict[str, bool] = {}
    candidate_states: dict[str, bool] = {}
    candidate_counts: dict[str, int] = {}

    async def update_service_statuses(counts: TotalsByLine) -> None:
        for line in SERVICE_LINES:
            active = getattr(counts, line) > 0
            if candidate_states.get(line) != active:
                candidate_states[line] = active
                candidate_counts[line] = 1
            else:
                candidate_counts[line] += 1
            if candidate_counts[line] < CONFIRMATION_SAMPLES:
                continue
            confirmed = service_statuses.get(line)
            if confirmed == active:
                continue
            service_statuses[line] = active
            if confirmed is not None:
                action = "started" if active else "stopped"
                await send_ntfy_message(
                    f"Service {line} {action} at {datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')}"
                )

    while True:
        counts = await get_vehicle_route_counts(r_client, config)
        await update_service_statuses(counts[1])
        await sleep(120)
