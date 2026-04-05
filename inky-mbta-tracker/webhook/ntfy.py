import logging
import os
import socket
from datetime import UTC, datetime

import aiohttp

logger = logging.getLogger(__name__)

NTFY_LIFECYCLE_URL = os.getenv("NTFY_LIFECYCLE_URL")
NTFY_BEARER_TOKEN = os.getenv("NTFY_BEARER_TOKEN")


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

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                NTFY_LIFECYCLE_URL,
                data=message.encode(),
                headers=headers,
            ) as response:
                if response.status >= 400:
                    logger.warning(
                        "Ntfy startup notification failed with status %d",
                        response.status,
                    )
    except aiohttp.ClientError as exc:
        logger.warning("Failed to send Ntfy startup notification", exc_info=exc)
