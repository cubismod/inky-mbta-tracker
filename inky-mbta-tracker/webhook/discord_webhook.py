import hashlib
import json
import logging
import os
import random
import time
from asyncio import CancelledError
from typing import Callable, Optional, Tuple

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
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.asyncio.client import Redis as RedisClient
from redis_cache import check_cache, delete_cache, write_cache
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
    wait_exponential_jitter,
)

from webhook import helpers as webhook_helpers

logger = logging.getLogger(__name__)

WEBHOOK_URL = os.getenv("IMT_DISCORD_URL")
PENDING_WEBHOOK_LOCK_TTL = MINUTE


PendingWebhookEntry = webhook_helpers.PendingWebhookEntry
PendingBatchItem = webhook_helpers.PendingBatchItem
PendingBatchEntry = webhook_helpers.PendingBatchEntry
PENDING_WEBHOOK_TTL = webhook_helpers.PENDING_WEBHOOK_TTL
PENDING_WEBHOOK_DELAY_RANGE = webhook_helpers.PENDING_WEBHOOK_DELAY_RANGE
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
    route = ", ".join(routes)
    if "CR" in route and alert.attributes.severity <= 6:
        # filter out low severity commuter rail alerts
        return
    if len(routes) == 1:
        route = routes[0]

    if WEBHOOK_URL:
        webhook = create_webhook_object(alert, routes, color, config)
        scheduled, batch_id = await enqueue_pending_batch(
            alert.id,
            webhook,
            routes,
            alert.attributes.created_at,
            r_client,
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
            name=f"MBTA Alert, Sev {alert.attributes.severity}",
            url="https://ryanwallace.cloud/alerts",
        ),
        color=color,
    )
    if webhook_helpers._alert_is_expired(alert):
        embed.footer = DiscordEmbedFooter(text="EXPIRED")
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


async def _get_pending_entry(
    r_client: RedisClient, webhook_id: str
) -> Optional[PendingWebhookEntry]:
    raw = await r_client.get(webhook_helpers._pending_key(webhook_id))
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


async def _get_pending_batch_entry(
    r_client: RedisClient,
) -> Optional[PendingBatchEntry]:
    raw = await r_client.get(webhook_helpers._batch_key())
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return PendingBatchEntry.model_validate_json(raw)
    except ValidationError as err:
        logger.error("Failed to parse pending batch cache", exc_info=err)
        return None


async def _get_batch_entry(
    r_client: RedisClient, batch_id: str
) -> Optional[PendingBatchEntry]:
    raw = await r_client.get(webhook_helpers._batch_entry_key(batch_id))
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return PendingBatchEntry.model_validate_json(raw)
    except ValidationError as err:
        logger.error("Failed to parse batch entry", exc_info=err)
        return None


async def enqueue_pending_webhook(
    webhook_id: str,
    webhook: DiscordWebhook,
    r_client: RedisClient,
    delay_range: tuple[float, float] = PENDING_WEBHOOK_DELAY_RANGE,
    clock: Callable[[], float] = time.time,
) -> bool:
    pending_key = webhook_helpers._pending_key(webhook_id)
    message_hash = webhook_helpers._webhook_hash(webhook)
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


async def enqueue_pending_batch(
    webhook_id: str,
    webhook: DiscordWebhook,
    routes: list[str],
    created_at: Optional[str],
    r_client: RedisClient,
    delay_seconds: float = BATCH_WINDOW_SECONDS,
    clock: Callable[[], float] = time.time,
) -> Tuple[bool, Optional[str]]:
    now = clock()
    item = PendingBatchItem(
        webhook_id=webhook_id,
        webhook_json=webhook.model_dump_json(),
        message_hash=webhook_helpers._webhook_hash(webhook),
        updated_at=webhook_helpers._webhook_updated_at(webhook, clock),
        created_at=created_at,
        routes=routes,
    )

    existing_batch_id = await r_client.get(webhook_helpers._alert_batch_key(webhook_id))
    if existing_batch_id:
        if isinstance(existing_batch_id, bytes):
            existing_batch_id = existing_batch_id.decode("utf-8")
        batch_entry = await _get_batch_entry(r_client, existing_batch_id)
        if batch_entry:
            updated_items = webhook_helpers._upsert_batch_items(batch_entry.items, item)
            updated_entry = batch_entry.model_copy(update={"items": updated_items})
            await r_client.set(
                webhook_helpers._batch_entry_key(existing_batch_id),
                updated_entry.model_dump_json(),
                ex=BATCH_ENTRY_TTL,
            )
            return False, existing_batch_id

    pending = await _get_pending_batch_entry(r_client)
    if pending:
        ready_at = pending.ready_at
        extended = pending.extended
        first_seen = pending.first_seen
        if first_seen is None:
            first_seen = pending.ready_at - BATCH_WINDOW_SECONDS
        if not pending.extended:
            ready_at = first_seen + BATCH_WINDOW_SECONDS
            extended = True
        updated_entry = pending.model_copy(
            update={
                "items": webhook_helpers._upsert_batch_items(pending.items, item),
                "ready_at": ready_at,
                "first_seen": first_seen,
                "extended": extended,
            }
        )
        await r_client.set(
            webhook_helpers._batch_key(),
            updated_entry.model_dump_json(),
            ex=PENDING_BATCH_TTL,
        )
        return updated_entry.ready_at <= now, None

    batch_id = str(int(now * 1000))
    first_seen = now
    ready_at = now + SHORT_BATCH_WINDOW_SECONDS
    entry = PendingBatchEntry(
        batch_id=batch_id,
        ready_at=ready_at,
        items=[item],
        first_seen=first_seen,
        extended=False,
    )
    created = await r_client.set(
        webhook_helpers._batch_key(),
        entry.model_dump_json(),
        ex=PENDING_BATCH_TTL,
        nx=True,
    )
    if created:
        return True, None

    pending = await _get_pending_batch_entry(r_client)
    if pending:
        ready_at = pending.ready_at
        extended = pending.extended
        first_seen = pending.first_seen
        if first_seen is None:
            first_seen = pending.ready_at - BATCH_WINDOW_SECONDS
        if not pending.extended:
            ready_at = first_seen + BATCH_WINDOW_SECONDS
            extended = True
        updated_entry = pending.model_copy(
            update={
                "items": webhook_helpers._upsert_batch_items(pending.items, item),
                "ready_at": ready_at,
                "first_seen": first_seen,
                "extended": extended,
            }
        )
        await r_client.set(
            webhook_helpers._batch_key(),
            updated_entry.model_dump_json(),
            ex=PENDING_BATCH_TTL,
        )
        return updated_entry.ready_at <= now, None

    await r_client.set(
        webhook_helpers._batch_key(),
        entry.model_dump_json(),
        ex=PENDING_BATCH_TTL,
    )
    return True, None


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


async def _delayed_send_batch(r_client: RedisClient, config: Config) -> None:
    pending = await _get_pending_batch_entry(r_client)
    if not pending:
        return
    delay = pending.ready_at - time.time()
    if delay > 0:
        await anyio.sleep(delay)
    while True:
        await send_pending_batch(r_client, config)
        next_pending = await _get_pending_batch_entry(r_client)
        if not next_pending:
            return
        delay = next_pending.ready_at - time.time()
        if delay > 0:
            await anyio.sleep(delay)
        else:
            await anyio.sleep(0)


async def send_batch_entry(
    batch_id: str,
    r_client: RedisClient,
    config: Config,
) -> None:
    batch_entry = await _get_batch_entry(r_client, batch_id)
    if not batch_entry:
        return
    lock_key = webhook_helpers._batch_entry_lock_key(batch_id)
    lock_acquired = await r_client.set(
        lock_key, "1", ex=PENDING_WEBHOOK_LOCK_TTL, nx=True
    )
    if not lock_acquired:
        return
    batch_entry = await _get_batch_entry(r_client, batch_id)
    if not batch_entry:
        await r_client.delete(lock_key)
        return
    items = batch_entry.items
    if not items:
        await r_client.delete(lock_key)
        return
    grouped = webhook_helpers.build_grouped_webhook(items, config)
    batch_message_id = f"{BATCH_WEBHOOK_ID}:{batch_id}"
    await _send_webhook_payload(batch_message_id, grouped, r_client)
    await r_client.delete(lock_key)


async def _send_webhook_payload(
    webhook_id: str, webhook: DiscordWebhook, r_client: RedisClient
) -> None:
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

    lock_key = webhook_helpers._pending_lock_key(webhook_id)
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

    await _send_webhook_payload(webhook_id, webhook, r_client)

    await r_client.delete(webhook_helpers._pending_key(webhook_id))
    await r_client.delete(lock_key)


async def send_pending_batch(
    r_client: RedisClient,
    config: Config,
    clock: Callable[[], float] = time.time,
) -> None:
    pending = await _get_pending_batch_entry(r_client)
    if not pending:
        return
    if pending.ready_at > clock():
        return

    lock_key = webhook_helpers._batch_lock_key()
    lock_acquired = await r_client.set(
        lock_key, "1", ex=PENDING_WEBHOOK_LOCK_TTL, nx=True
    )
    if not lock_acquired:
        logger.info("Batch lock not acquired")
        return

    pending = await _get_pending_batch_entry(r_client)
    if not pending:
        logger.info("Pending batch missing after lock")
        await r_client.delete(lock_key)
        return

    batch_lock = webhook_helpers._batch_entry_lock_key(pending.batch_id)
    batch_lock_acquired = await r_client.set(
        batch_lock, "1", ex=PENDING_WEBHOOK_LOCK_TTL, nx=True
    )
    if not batch_lock_acquired:
        logger.info("Batch entry lock not acquired", extra={"batch": pending.batch_id})
        await r_client.delete(lock_key)
        return

    items: list[PendingBatchItem] = []
    for item in pending.items:
        items.append(item)
    if not items:
        await r_client.delete(webhook_helpers._batch_key())
        await r_client.delete(lock_key)
        return

    if len(items) == 1:
        item = items[0]
        try:
            webhook = DiscordWebhook.model_validate_json(item.webhook_json)
        except ValidationError as err:
            logger.error(
                f"Failed to parse pending batch payload for {item.webhook_id}",
                exc_info=err,
            )
            await r_client.delete(webhook_helpers._batch_key())
            await r_client.delete(lock_key)
            return
        await _send_webhook_payload(item.webhook_id, webhook, r_client)
        await r_client.delete(webhook_helpers._batch_key())
        await r_client.delete(lock_key)
        await r_client.delete(batch_lock)
        return

    grouped = webhook_helpers.build_grouped_webhook(items, config)
    batch_id = f"{BATCH_WEBHOOK_ID}:{pending.batch_id}"
    logger.debug(
        "Sending batch",
        extra={"batch_id": batch_id, "items": len(items), "ready_at": pending.ready_at},
    )
    await _send_webhook_payload(batch_id, grouped, r_client)
    batch_entry = PendingBatchEntry(
        batch_id=pending.batch_id,
        ready_at=pending.ready_at,
        items=items,
    )
    await r_client.set(
        webhook_helpers._batch_entry_key(pending.batch_id),
        batch_entry.model_dump_json(),
        ex=BATCH_ENTRY_TTL,
    )
    for item in items:
        await r_client.set(
            webhook_helpers._alert_batch_key(item.webhook_id),
            pending.batch_id,
            ex=BATCH_ENTRY_TTL,
        )
    await r_client.delete(webhook_helpers._batch_key())
    await r_client.delete(lock_key)
    await r_client.delete(batch_lock)


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
