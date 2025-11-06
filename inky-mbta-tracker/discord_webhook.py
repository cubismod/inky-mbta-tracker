import hashlib
import json
import logging
import os
from asyncio import CancelledError

import aiohttp
from consts import DAY
from exceptions import RateLimitExceeded
from mbta_responses import AlertResource
from pydantic import ValidationError
from redis.asyncio import Redis
from redis_cache import check_cache, write_cache
from shared_types.shared_types import (
    DiscordEmbed,
    DiscordEmbedAuthor,
    DiscordEmbedMedia,
    DiscordWebhook,
    WebhookRedisEntry,
)
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_not_exception_type,
    wait_exponential_jitter,
)
from utils import hex_color_to_int

logger = logging.getLogger(__name__)

WEBHOOK_URL = os.getenv("IMT_DISCORD_URL")


async def process_alert_event(
    alert: AlertResource,
    r_client: Redis,
):
    from geojson_utils import lookup_route_color

    if alert.attributes.severity < 7:
        return
    routes = {e.route for e in alert.attributes.informed_entity if e.route}
    color = 5793266  # blurple by default
    route = ", ".join(routes)
    if len(routes) == 1:
        item = routes.pop()
        color = hex_color_to_int(lookup_route_color(item))
        route = item

    if WEBHOOK_URL:
        async with aiohttp.ClientSession() as session:
            webhook = create_webhook_object(alert, route, color)
            existing = await check_cache(r_client, f"webhook:{alert.id}")
            if existing:
                try:
                    existing_val = WebhookRedisEntry.model_validate_json(existing)
                    await patch_webhook(
                        WEBHOOK_URL,
                        webhook,
                        alert.id,
                        existing_val.message_id,
                        session,
                        r_client,
                    )
                except ValidationError as err:
                    logger.error(
                        f"Failed to parse existing webhook cache for {alert.id}",
                        exc_info=err,
                    )
            else:
                await post_webhook(WEBHOOK_URL, alert.id, webhook, r_client, session)


def create_webhook_object(
    alert: AlertResource, route: str, color: int
) -> DiscordWebhook:
    embed = DiscordEmbed(
        description=alert.attributes.header,
        timestamp=alert.attributes.updated_at,
        author=DiscordEmbedAuthor(
            name=f"MBTA Alert, Sev {alert.attributes.severity}",
            url="https://ryanwallace.cloud/alerts",
        ),
        color=color,
    )
    if alert.attributes.image:
        embed.image = DiscordEmbedMedia(url=alert.attributes.image)
    return DiscordWebhook(embeds=[embed])


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def post_webhook(
    url: str,
    webhook_id: str,
    webhook: DiscordWebhook,
    r_client: Redis,
    session: aiohttp.ClientSession,
):
    async with session.post(
        f"{url}?wait=true",
        data=webhook.model_dump_json(),
        headers={"Content-Type": "application/json"},
    ) as response:
        if response.status == 429:
            raise RateLimitExceeded()
        body = await response.text()
        if response.status != 200 and response.status != 204:
            logger.error(
                f"Failed to post webhook {webhook_id}, status {response.status}, body: {body}"
            )
            return
        json_body = json.loads(body)
        if "id" in json_body:
            message_id = json_body["id"]
            h = hashlib.sha256()
            h.update(webhook.model_dump_json().encode("utf-8"))
            await write_cache(
                r_client,
                f"webhook:{webhook_id}",
                WebhookRedisEntry(
                    message_id=message_id, message_hash=h.hexdigest()
                ).model_dump_json(),
                DAY,
            )


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def patch_webhook(
    url: str,
    webhook: DiscordWebhook,
    webhook_id: str,
    message_id: str,
    session: aiohttp.ClientSession,
    r_client: Redis,
):
    existing = await check_cache(r_client, f"webhook:{webhook_id}")
    if existing:
        try:
            existing_val = WebhookRedisEntry.model_validate_json(existing)
            h = hashlib.sha256()
            h.update(webhook.model_dump_json().encode("utf-8"))
            if existing_val.message_hash == h.hexdigest():
                logger.debug(f"Webhook {webhook_id} unchanged, skipping patch")
                return
            async with session.patch(
                f"{url}/messages/{message_id}",
                data=webhook.model_dump_json(),
                headers={"Content-Type": "application/json"},
            ) as response:
                if response.status == 429:
                    raise RateLimitExceeded()
                body = await response.text()
                if response.status != 200 and response.status != 204:
                    logger.error(
                        f"Failed to patch webhook {webhook_id}, status {response.status}, body: {body}"
                    )
                    return

                h.update(webhook.model_dump_json().encode("utf-8"))
                await write_cache(
                    r_client,
                    f"webhook:{webhook_id}",
                    WebhookRedisEntry(
                        message_id=message_id, message_hash=h.hexdigest()
                    ).model_dump_json(),
                    DAY,
                )
        except ValidationError as err:
            logger.error(
                f"Failed to parse existing webhook cache for {webhook_id}", exc_info=err
            )
