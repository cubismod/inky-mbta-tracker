import hashlib
import json
import logging
import os
import random
import time
from asyncio import CancelledError
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from typing import Callable, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp
import anyio
from anyio.abc import TaskGroup
from config import Config
from consts import DAY
from exceptions import RateLimitExceeded
from mbta_responses import AlertResource
from otel_config import get_tracer, is_otel_enabled
from otel_utils import should_trace_operation
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.asyncio.client import Redis as RedisClient
from redis_cache import delete_cache, get_cache, write_cache
from redis_lock.asyncio import RedisLock
from shared_types.shared_types import (
    DiscordEmbed,
    DiscordEmbedAuthor,
    DiscordEmbedField,
    DiscordEmbedFooter,
    DiscordEmbedMedia,
    DiscordWebhook,
    WebhookRedisEntry,
)
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_exponential_jitter,
)

from webhook import helpers as webhook_helpers

logger = logging.getLogger(__name__)

WEBHOOK_URL = os.getenv("IMT_DISCORD_URL")

PendingBatchItem = webhook_helpers.PendingBatchItem
PendingBatchEntry = webhook_helpers.PendingBatchEntry
BATCH_WINDOW_SECONDS = webhook_helpers.BATCH_WINDOW_SECONDS
SHORT_BATCH_WINDOW_SECONDS = webhook_helpers.SHORT_BATCH_WINDOW_SECONDS
PENDING_BATCH_TTL = webhook_helpers.PENDING_BATCH_TTL
BATCH_ENTRY_TTL = webhook_helpers.BATCH_ENTRY_TTL
BATCH_WEBHOOK_ID = webhook_helpers.BATCH_WEBHOOK_ID


async def process_alert_event(
    alert: AlertResource, r_client: RedisClient, config: Config, tg: TaskGroup
):
    routes = webhook_helpers.determine_alert_routes(alert)
    color = webhook_helpers.determine_alert_color(routes)
    updated_at = datetime.fromisoformat(alert.attributes.updated_at).astimezone(UTC)
    if updated_at < (datetime.now(UTC) - timedelta(minutes=15)):
        logger.debug(
            f"Skipped event for alert {alert.id} due to staleness, timestamp: {updated_at.astimezone(ZoneInfo('America/New_York'))}"
        )
        return

    if WEBHOOK_URL:
        webhook = create_webhook_object(alert, routes, color, config)
        scheduled, batch_id = await enqueue_pending_batch(
            alert.id, webhook, routes, alert.attributes.created_at, r_client
        )
        if batch_id:
            tg.start_soon(send_batch_entry, batch_id, r_client, config)
        elif scheduled:
            tg.start_soon(_delayed_send_batch, r_client, config)


def create_webhook_object(
    alert: AlertResource, routes: list[str], color: int, config: Config
) -> DiscordWebhook:
    embed = DiscordEmbed(
        description=alert.attributes.header,
        timestamp=alert.attributes.updated_at,
        author=DiscordEmbedAuthor(
            name=f"{webhook_helpers.format_route_names(', '.join(routes))} Alert",
            url="https://bostontraintracker.com",
        ),
        color=color,
    )
    if webhook_helpers.alert_is_expired(alert):
        embed.footer = DiscordEmbedFooter(text="EXPIRED")
    if len(routes) > 1:
        embed.fields = [
            DiscordEmbedField(
                name="Lines",
                value=webhook_helpers.format_route_names(", ".join(routes)),
                inline=False,
            )
        ]
    avatar_url = None
    if alert.attributes.image:
        embed.image = DiscordEmbedMedia(url=alert.attributes.image)
    if config.severity_icons and len(config.severity_icons) >= 10:
        severity_index = max(
            0, min(alert.attributes.severity - 1, len(config.severity_icons) - 1)
        )
        avatar_url = config.severity_icons[severity_index]
    return DiscordWebhook(avatar_url=avatar_url, embeds=[embed])


async def get_pending_batch_entry(
    r_client: RedisClient,
) -> Optional[PendingBatchEntry]:
    return await _get_batch_entry_by_key(r_client, webhook_helpers.PENDING_BATCH_KEY)


async def get_batch_entry(
    r_client: RedisClient, batch_id: str
) -> Optional[PendingBatchEntry]:
    return await _get_batch_entry_by_key(
        r_client, webhook_helpers.batch_entry_key(batch_id)
    )


async def _get_batch_entry_by_key(
    r_client: RedisClient, key: str
) -> Optional[PendingBatchEntry]:
    raw = await r_client.get(key)
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return PendingBatchEntry.model_validate_json(raw)
    except ValidationError as err:
        logger.error("Failed to parse pending batch cache", exc_info=err)
        return None


async def enqueue_pending_batch(
    webhook_id: str,
    webhook: DiscordWebhook,
    routes: list[str],
    created_at: Optional[str],
    r_client: RedisClient,
    clock: Callable[[], float] = time.time,
) -> Tuple[bool, Optional[str]]:
    now = clock()
    item = PendingBatchItem(
        webhook_id=webhook_id,
        webhook_json=webhook.model_dump_json(),
        message_hash=webhook_helpers.webhook_hash(webhook),
        updated_at=webhook_helpers.webhook_updated_at(webhook, clock),
        created_at=created_at,
        routes=routes,
    )

    async with RedisLock(
        r_client,
        webhook_helpers.batch_lock_key(),
        blocking_timeout=15,
        expire_timeout=60,
    ):
        existing_batch_id = await r_client.get(
            webhook_helpers.alert_batch_key(webhook_id)
        )
        if existing_batch_id:
            if isinstance(existing_batch_id, bytes):
                existing_batch_id = existing_batch_id.decode("utf-8")
            batch_entry = await get_batch_entry(r_client, existing_batch_id)
            if batch_entry:
                await r_client.set(
                    webhook_helpers.batch_entry_key(existing_batch_id),
                    batch_entry.model_copy(
                        update={
                            "items": webhook_helpers.upsert_batch_items(
                                batch_entry.items, item
                            )
                        }
                    ).model_dump_json(),
                    ex=BATCH_ENTRY_TTL,
                )
                return False, existing_batch_id

        pending = await get_pending_batch_entry(r_client)
        if pending:
            first_seen = pending.first_seen
            if first_seen is None:
                first_seen = pending.ready_at - BATCH_WINDOW_SECONDS
            ready_at = (
                pending.ready_at
                if pending.extended
                else first_seen + BATCH_WINDOW_SECONDS
            )
            updated_entry = pending.model_copy(
                update={
                    "items": webhook_helpers.upsert_batch_items(pending.items, item),
                    "ready_at": ready_at,
                    "first_seen": first_seen,
                    "extended": True,
                }
            )
            await r_client.set(
                webhook_helpers.PENDING_BATCH_KEY,
                updated_entry.model_dump_json(),
                ex=PENDING_BATCH_TTL,
            )
            return updated_entry.ready_at <= now, None

        batch_id = f"{int(now * 1000)}-{random.randint(0, 9999):04d}"
        entry = PendingBatchEntry(
            batch_id=batch_id,
            ready_at=now + SHORT_BATCH_WINDOW_SECONDS,
            items=[item],
            first_seen=now,
            extended=False,
        )
        await r_client.set(
            webhook_helpers.PENDING_BATCH_KEY,
            entry.model_dump_json(),
            ex=PENDING_BATCH_TTL,
        )
        return True, None


async def _delayed_send_batch(r_client: RedisClient, config: Config) -> None:
    if not await r_client.set(
        webhook_helpers.PENDING_BATCH_SENDER_KEY, "1", nx=True, ex=PENDING_BATCH_TTL
    ):
        return
    try:
        while True:
            pending = await get_pending_batch_entry(r_client)
            if not pending:
                return
            delay = pending.ready_at - time.time()
            if delay > 0:
                await anyio.sleep(delay)
            await send_pending_batch(r_client, config)
    finally:
        await r_client.delete(webhook_helpers.PENDING_BATCH_SENDER_KEY)


async def _send_items(
    items: list[PendingBatchItem],
    batch_id: str,
    config: Config,
    r_client: RedisClient,
) -> None:
    if len(items) == 1:
        item = items[0]
        try:
            webhook = DiscordWebhook.model_validate_json(item.webhook_json)
        except ValidationError as err:
            logger.error(
                f"Failed to parse pending batch payload for {item.webhook_id}",
                exc_info=err,
            )
            return
        await send_webhook_payload(item.webhook_id, webhook, r_client)
    elif len(items) > 1:
        grouped = webhook_helpers.build_grouped_webhook(items, config)
        await send_webhook_payload(f"{BATCH_WEBHOOK_ID}:{batch_id}", grouped, r_client)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_not_exception_type(CancelledError),
)
async def send_batch_entry(
    batch_id: str,
    r_client: RedisClient,
    config: Config,
) -> None:
    batch_entry = await get_batch_entry(r_client, batch_id)
    if not batch_entry:
        return
    async with RedisLock(
        r_client,
        webhook_helpers.batch_entry_lock_key(batch_id),
        blocking_timeout=15,
        expire_timeout=60,
    ):
        batch_entry = await get_batch_entry(r_client, batch_id)
        if not batch_entry:
            return
        if not batch_entry.items:
            return
        await _send_items(batch_entry.items, batch_entry.batch_id, config, r_client)


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
async def send_pending_batch(
    r_client: RedisClient,
    config: Config,
    clock: Callable[[], float] = time.time,
) -> None:
    pending = await get_pending_batch_entry(r_client)
    if not pending or pending.ready_at > clock():
        return

    async with RedisLock(
        r_client,
        webhook_helpers.batch_lock_key(),
        blocking_timeout=15,
        expire_timeout=60,
    ):
        pending = await get_pending_batch_entry(r_client)
        if not pending:
            logger.debug("Pending batch missing after lock")
            return
        items = pending.items
        if not items:
            await r_client.delete(webhook_helpers.PENDING_BATCH_KEY)
            return

        await _send_items(items, pending.batch_id, config, r_client)

        storage_batch_id = items[0].webhook_id if len(items) == 1 else pending.batch_id
        batch_entry = PendingBatchEntry(
            batch_id=storage_batch_id,
            ready_at=pending.ready_at if len(items) > 1 else 0.0,
            items=items,
        )
        await r_client.set(
            webhook_helpers.batch_entry_key(storage_batch_id),
            batch_entry.model_dump_json(),
            ex=BATCH_ENTRY_TTL,
        )
        for item in items:
            await r_client.set(
                webhook_helpers.alert_batch_key(item.webhook_id),
                storage_batch_id,
                ex=BATCH_ENTRY_TTL,
            )
        await r_client.delete(webhook_helpers.PENDING_BATCH_KEY)


async def send_webhook_payload(
    webhook_id: str, webhook: DiscordWebhook, r_client: RedisClient
) -> None:
    if not WEBHOOK_URL:
        return
    existing = await get_cache(r_client, f"webhook:{webhook_id}")
    async with aiohttp.ClientSession() as session:
        if existing:
            try:
                existing_val = WebhookRedisEntry.model_validate_json(existing)
            except ValidationError as err:
                logger.error(
                    f"Failed to parse existing webhook cache for {webhook_id}",
                    exc_info=err,
                )
                return
            await patch_webhook(
                WEBHOOK_URL,
                webhook,
                webhook_id,
                existing_val.message_id,
                existing_val.message_hash,
                session,
                r_client,
            )
        else:
            await post_webhook(WEBHOOK_URL, webhook_id, webhook, r_client, session)


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
) -> None:
    tracer = get_tracer(__name__) if is_otel_enabled() else None
    with (
        tracer.start_as_current_span(
            "discord_webhook.post_webhook", attributes={"webhook_id": webhook_id}
        )
        if tracer and should_trace_operation("low_volume")
        else nullcontext() as span
    ):
        if await webhook_helpers.get_webhook_duplicate(r_client, webhook):
            logger.debug(
                f"Webhook {webhook_id} is a duplicate of an existing webhook, skipping post"
            )
            return
        async with session.post(
            f"{url}?wait=true",
            data=webhook.model_dump_json(),
            headers={"Content-Type": "application/json"},
        ) as response:
            if response.status == 429:
                raise RateLimitExceeded()
            body = await response.text()
            if response.status not in (200, 204):
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
                h = hashlib.sha512()
                h.update(webhook.model_dump_json().encode("utf-8"))
                await write_cache(
                    r_client,
                    f"webhook:{webhook_id}",
                    WebhookRedisEntry(
                        message_id=message_id, message_hash=h.hexdigest()
                    ).model_dump_json(),
                    DAY,
                )
            await webhook_helpers.set_webhook_duplicate(r_client, webhook)


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
    existing_hash: str,
    session: aiohttp.ClientSession,
    r_client: Redis,
) -> None:
    h = hashlib.sha512()
    h.update(webhook.model_dump_json().encode("utf-8"))
    new_hash = h.hexdigest()
    if existing_hash == new_hash:
        logger.debug(f"Webhook {webhook_id} unchanged, skipping patch")
        return
    if await webhook_helpers.get_webhook_duplicate(r_client, webhook):
        logger.debug(
            f"Webhook {webhook_id} is a duplicate of an existing webhook, skipping patch"
        )
        return
    async with session.patch(
        f"{url}/messages/{message_id}",
        data=webhook.model_dump_json(),
        headers={"Content-Type": "application/json"},
    ) as response:
        if response.status == 429:
            raise RateLimitExceeded()
        body = await response.text()
        if response.status not in (200, 204):
            logger.error(
                f"Failed to patch webhook {webhook_id}, status {response.status}, body: {body}"
            )
            return
        logger.debug(
            f"Patched webhook ID: {webhook_id}. Old SHA: {existing_hash}, New SHA: {new_hash}"
        )
        await write_cache(
            r_client,
            f"webhook:{webhook_id}",
            WebhookRedisEntry(
                message_id=message_id, message_hash=new_hash
            ).model_dump_json(),
            DAY,
        )
        await webhook_helpers.set_webhook_duplicate(r_client, webhook)


@retry(
    wait=wait_exponential_jitter(initial=5, jitter=20, max=60),
    before_sleep=before_sleep_log(logger, logging.ERROR, exc_info=True),
    retry=retry_if_not_exception_type(CancelledError),
)
async def delete_webhook(webhook_id: str, r_client: Redis):
    existing = await get_cache(r_client, f"webhook:{webhook_id}")
    if not existing or not WEBHOOK_URL:
        return
    try:
        existing_val = WebhookRedisEntry.model_validate_json(existing)
    except ValidationError as err:
        logger.error(
            f"Failed to parse existing webhook cache for {webhook_id}",
            exc_info=err,
        )
        return
    await delete_cache(r_client, f"webhook:{webhook_id}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.delete(
                f"{WEBHOOK_URL}/messages/{existing_val.message_id}"
            ) as response:
                if response.status not in (200, 204):
                    logger.error(
                        f"Failed to delete webhook msg {existing_val.message_id}, status {response.status}"
                    )
                    return
        logger.debug(f"Deleted webhook ID: {webhook_id}")
    except aiohttp.ClientError as err:
        logger.error(f"HTTP error deleting webhook {webhook_id}", exc_info=err)
