import hashlib
import json
import logging
import os
import random
import time
from asyncio import CancelledError
from typing import Callable, Optional

import aiohttp
import anyio
from anyio.abc import TaskGroup
from config import Config
from consts import DAY, MINUTE
from exceptions import RateLimitExceeded
from mbta_responses import AlertResource
from opentelemetry.trace import Span
from otel_config import get_tracer, is_otel_enabled
from otel_utils import should_trace_operation
from pydantic import BaseModel, ValidationError
from redis.asyncio import Redis
from redis.asyncio.client import Redis as RedisClient
from redis_cache import check_cache, delete_cache, write_cache
from shared_types.shared_types import (
    DiscordEmbed,
    DiscordEmbedAuthor,
    DiscordEmbedField,
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
PENDING_WEBHOOK_PREFIX = "webhook:pending"
PENDING_WEBHOOK_LOCK_PREFIX = "webhook:pending:lock"
PENDING_WEBHOOK_TTL = 10 * MINUTE
PENDING_WEBHOOK_LOCK_TTL = MINUTE
PENDING_WEBHOOK_DELAY_RANGE = (5.0, 15.0)


class PendingWebhookEntry(BaseModel):
    webhook_json: str
    message_hash: str
    ready_at: float


async def process_alert_event(
    alert: AlertResource, r_client: RedisClient, config: Config, tg: TaskGroup
):
    routes = determine_alert_routes(alert)
    color = determine_alert_color(routes)
    route = ", ".join(routes)
    if "CR" in route and alert.attributes.severity <= 6:
        # filter out low severity commuter rail alerts
        return
    if len(routes) == 1:
        route = routes[0]

    if WEBHOOK_URL:
        webhook = create_webhook_object(alert, routes, color, config)
        scheduled = await enqueue_pending_webhook(alert.id, webhook, r_client)
        if scheduled:
            tg.start_soon(_delayed_send_pending, alert.id, r_client, config)


def create_webhook_object(
    alert: AlertResource, routes: list[str], color: int, config: Config
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
    if len(routes) > 1:
        embed.fields = [
            DiscordEmbedField(name="Lines", value=", ".join(routes), inline=False)
        ]
    avatar_url = None
    if alert.attributes.image:
        embed.image = DiscordEmbedMedia(url=alert.attributes.image)
    if config.severity_icons and len(config.severity_icons) >= 10:
        avatar_url = config.severity_icons[alert.attributes.severity - 1]
    return DiscordWebhook(avatar_url=avatar_url, embeds=[embed])


def determine_alert_routes(alert: AlertResource) -> list[str]:
    routes: list[str] = []
    seen: set[str] = set()
    for entity in alert.attributes.informed_entity:
        if entity.route and entity.route not in seen:
            seen.add(entity.route)
            routes.append(entity.route)
    return routes


def determine_alert_color(routes: list[str]) -> int:
    from geojson_utils import lookup_route_color

    if routes and len(routes) > 0:
        return hex_color_to_int(lookup_route_color(routes[0]))
    return 5793266


def _pending_key(webhook_id: str) -> str:
    return f"{PENDING_WEBHOOK_PREFIX}:{webhook_id}"


def _pending_lock_key(webhook_id: str) -> str:
    return f"{PENDING_WEBHOOK_LOCK_PREFIX}:{webhook_id}"


def _webhook_hash(webhook: DiscordWebhook) -> str:
    h = hashlib.sha256()
    h.update(webhook.model_dump_json().encode("utf-8"))
    return h.hexdigest()


async def _get_pending_entry(
    r_client: RedisClient, webhook_id: str
) -> Optional[PendingWebhookEntry]:
    raw = await r_client.get(_pending_key(webhook_id))
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return PendingWebhookEntry.model_validate_json(raw)
    except ValidationError as err:
        logger.error(
            f"Failed to parse pending webhook cache for {webhook_id}", exc_info=err
        )
        return None


async def enqueue_pending_webhook(
    webhook_id: str,
    webhook: DiscordWebhook,
    r_client: RedisClient,
    delay_range: tuple[float, float] = PENDING_WEBHOOK_DELAY_RANGE,
    clock: Callable[[], float] = time.time,
) -> bool:
    pending_key = _pending_key(webhook_id)
    message_hash = _webhook_hash(webhook)
    webhook_json = webhook.model_dump_json()
    existing = await _get_pending_entry(r_client, webhook_id)
    if existing:
        updated = existing.model_copy(
            update={"webhook_json": webhook_json, "message_hash": message_hash}
        )
        await r_client.set(
            pending_key, updated.model_dump_json(), ex=PENDING_WEBHOOK_TTL
        )
        return False

    ready_at = clock() + random.uniform(*delay_range)
    entry = PendingWebhookEntry(
        webhook_json=webhook_json, message_hash=message_hash, ready_at=ready_at
    )
    created = await r_client.set(
        pending_key, entry.model_dump_json(), ex=PENDING_WEBHOOK_TTL, nx=True
    )
    if created:
        return True

    existing = await _get_pending_entry(r_client, webhook_id)
    if existing:
        updated = existing.model_copy(
            update={"webhook_json": webhook_json, "message_hash": message_hash}
        )
        await r_client.set(
            pending_key, updated.model_dump_json(), ex=PENDING_WEBHOOK_TTL
        )
        return False

    await r_client.set(pending_key, entry.model_dump_json(), ex=PENDING_WEBHOOK_TTL)
    return True


async def _delayed_send_pending(
    webhook_id: str, r_client: RedisClient, config: Config
) -> None:
    pending = await _get_pending_entry(r_client, webhook_id)
    if not pending:
        return
    delay = pending.ready_at - time.time()
    if delay > 0:
        await anyio.sleep(delay)
    await send_pending_webhook(webhook_id, r_client, config)


async def send_pending_webhook(
    webhook_id: str,
    r_client: RedisClient,
    config: Config,
    clock: Callable[[], float] = time.time,
) -> None:
    pending = await _get_pending_entry(r_client, webhook_id)
    if not pending:
        return
    if pending.ready_at > clock():
        return

    lock_key = _pending_lock_key(webhook_id)
    lock_acquired = await r_client.set(
        lock_key, "1", ex=PENDING_WEBHOOK_LOCK_TTL, nx=True
    )
    if not lock_acquired:
        return

    pending = await _get_pending_entry(r_client, webhook_id)
    if not pending:
        await r_client.delete(lock_key)
        return

    try:
        webhook = DiscordWebhook.model_validate_json(pending.webhook_json)
    except ValidationError as err:
        logger.error(
            f"Failed to parse pending webhook payload for {webhook_id}", exc_info=err
        )
        await r_client.delete(lock_key)
        return

    if WEBHOOK_URL:
        async with aiohttp.ClientSession() as session:
            existing = await check_cache(r_client, f"webhook:{webhook_id}")
            if existing:
                try:
                    existing_val = WebhookRedisEntry.model_validate_json(existing)
                    await patch_webhook(
                        WEBHOOK_URL,
                        webhook,
                        webhook_id,
                        existing_val.message_id,
                        session,
                        r_client,
                    )
                except ValidationError as err:
                    logger.error(
                        f"Failed to parse existing webhook cache for {webhook_id}",
                        exc_info=err,
                    )
            else:
                await post_webhook(WEBHOOK_URL, webhook_id, webhook, r_client, session)

    await r_client.delete(_pending_key(webhook_id))
    await r_client.delete(lock_key)


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
    tracer = get_tracer(__name__) if is_otel_enabled() else None
    if tracer and should_trace_operation("low_volume"):
        with tracer.start_as_current_span(
            "discord_webhook.post_webhook", attributes={"webhook_id": webhook_id}
        ) as span:
            await _post_webhook_impl(url, webhook_id, webhook, r_client, session, span)
    else:
        await _post_webhook_impl(url, webhook_id, webhook, r_client, session, None)


async def _post_webhook_impl(
    url: str,
    webhook_id: str,
    webhook: DiscordWebhook,
    r_client: Redis,
    session: aiohttp.ClientSession,
    span: Optional[Span],
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
            if span:
                span.set_attribute("error", True)
                span.set_attribute("error.type", "http_error")
                span.set_attribute("http.status_code", response.status)
            return
        if span:
            span.set_attribute("http.status_code", response.status)
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


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def delete_webhook(webhook_id: str, r_client: Redis):
    async with aiohttp.ClientSession() as session:
        existing = await check_cache(r_client, f"webhook:{webhook_id}")
        if existing and WEBHOOK_URL:
            existing_val = WebhookRedisEntry.model_validate_json(existing)
            await delete_cache(r_client, f"webhook:{webhook_id}")
            try:
                async with session.delete(
                    f"{WEBHOOK_URL}/messages/{existing_val.message_id}"
                ) as response:
                    if response.status != 200 and response.status != 204:
                        logger.error(
                            f"Failed to delete webhook msg {existing_val.message_id}, status {response.status}"
                        )
                        return
            except ValidationError as err:
                logger.error(
                    f"Failed to parse existing webhook cache for {webhook_id}",
                    exc_info=err,
                )
