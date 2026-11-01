"""Expiry-driven cleanup of Discord messages posted for MBTA alerts.

Alerts are only ever removed from Discord today when the SSE stream emits a
`remove` event for them, which is not reliably delivered: alerts that simply
run past every `active_period`, and anything that disappears while the stream
is reconnecting, leave their message behind forever.

This module owns the fallback. Every posted message is registered in the
`webhook:expiry` sorted set, scored by the moment its alert (or, for grouped
messages, its first alert) runs out. The sweeper walks that set, deletes
messages whose alerts have all elapsed and re-renders grouped messages that
lost only some of their items.
"""

import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

import aiohttp
from config import Config
from exceptions import RateLimitExceeded
from mbta_responses import AlertResource
from prometheus import webhook_expiry_actions
from pydantic import ValidationError
from redis.asyncio.client import Redis
from redis_lock.asyncio import RedisLock
from shared_types.shared_types import WebhookRedisEntry

from webhook import helpers as webhook_helpers
from webhook.discord_webhook import (
    WEBHOOK_URL,
    clear_webhook_pointer,
    delete_message,
    patch_message,
    store_webhook_pointer,
)

logger = logging.getLogger(__name__)

SWEEP_INTERVAL_SECONDS = 5 * 60
# Bounded pass: deletes are rate limited per channel, so work through the due
# members in chunks and let the next tick pick up the rest.
SWEEP_BATCH_LIMIT = 100
SWEEP_LOCK_KEY = "webhook:expiry:sweep:lock"

DELETED = "deleted"
REPATCHED = "repatched"
REARMED = "rearmed"
PURGED = "purged"
FAILED = "failed"

_COUNTER_FIELDS = {
    DELETED: "deleted",
    REPATCHED: "repatched",
    REARMED: "rearmed",
    PURGED: "purged",
    FAILED: "failed",
}


@dataclass
class SweepResult:
    """Outcome counts for a single sweep pass."""

    examined: int = 0
    deleted: int = 0
    repatched: int = 0
    rearmed: int = 0
    purged: int = 0
    failed: int = 0
    rate_limited: bool = False


def _decode(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _record(result: SweepResult, outcome: str) -> None:
    webhook_expiry_actions.labels(action=outcome).inc()
    field = _COUNTER_FIELDS.get(outcome)
    if field:
        setattr(result, field, getattr(result, field) + 1)


async def sweep_expired_webhooks(
    r_client: Redis,
    config: Config,
    clock: Callable[[], float] = time.time,
) -> SweepResult:
    """Delete or trim messages whose alerts have all elapsed.

    The work list is the `webhook:expiry` sorted set, so no MBTA API calls and
    no keyspace scan on the steady-state path. Idempotent: every branch re-reads
    the pointer under the lock, and keys are only cleared once Discord confirms
    the change, so a failure or a crash mid-pass is retried on the next tick.
    """
    result = SweepResult()
    url = WEBHOOK_URL
    if not url:
        return result

    now = clock()
    async with RedisLock(
        r_client,
        SWEEP_LOCK_KEY,
        blocking_timeout=15,
        expire_timeout=SWEEP_INTERVAL_SECONDS,
    ):
        members = await r_client.zrangebyscore(
            webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY,
            "-inf",
            now,
            start=0,
            num=SWEEP_BATCH_LIMIT,
        )
        if not members:
            return result

        async with aiohttp.ClientSession() as session:
            for member in members:
                webhook_id = _decode(member)
                result.examined += 1
                try:
                    outcome = await _sweep_member(
                        r_client, config, session, url, webhook_id, now
                    )
                except RateLimitExceeded:
                    # Leave the rest of the batch for the next pass rather than
                    # hammering a rate limited channel.
                    result.rate_limited = True
                    logger.warning(
                        "Discord rate limited the webhook expiry sweep, "
                        "deferring remaining messages"
                    )
                    break
                except Exception as exc:
                    logger.error(
                        f"Failed to sweep webhook message {webhook_id}", exc_info=exc
                    )
                    outcome = FAILED
                _record(result, outcome)
    return result


async def _load_cached_alert(r_client: Redis, alert_id: str) -> Optional[AlertResource]:
    raw = await r_client.get(f"alert:{alert_id}")
    if not raw:
        return None
    try:
        return AlertResource.model_validate_json(_decode(raw), strict=False)
    except ValidationError:
        return None


async def _sweep_member(
    r_client: Redis,
    config: Config,
    session: aiohttp.ClientSession,
    url: str,
    webhook_id: str,
    now: float,
) -> str:
    raw = await r_client.get(f"webhook:{webhook_id}")
    if not raw:
        # Pointer already expired (or never written): nothing left to delete.
        await r_client.zrem(webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY, webhook_id)
        return PURGED
    try:
        entry = WebhookRedisEntry.model_validate_json(_decode(raw))
    except ValidationError as err:
        logger.error(
            f"Unparseable webhook pointer for {webhook_id}, dropping index entry",
            exc_info=err,
        )
        await r_client.zrem(webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY, webhook_id)
        return PURGED
    if entry.expires_at is None:
        await r_client.zrem(webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY, webhook_id)
        return PURGED
    if entry.expires_at > now:
        # Index score drifted behind the pointer; re-score instead of deleting.
        await store_webhook_pointer(r_client, webhook_id, entry, now)
        return REARMED
    if entry.kind == "batch" and entry.batch_id:
        return await _sweep_batch(
            r_client, config, session, url, webhook_id, entry, entry.batch_id, now
        )
    return await _sweep_single(r_client, session, url, webhook_id, entry, now)


async def _sweep_single(
    r_client: Redis,
    session: aiohttp.ClientSession,
    url: str,
    webhook_id: str,
    entry: WebhookRedisEntry,
    now: float,
) -> str:
    # Guards the delete-then-post race: a cached alert means MBTA touched this
    # alert within the last couple of hours, so an end time may have been pushed
    # out by an `update` event we have not processed yet. Re-arm instead of
    # deleting a message that is about to be re-posted.
    alert = await _load_cached_alert(r_client, webhook_id)
    if alert is not None:
        refreshed = webhook_helpers.alert_expiry_ts(alert)
        if refreshed is None or refreshed > now:
            await store_webhook_pointer(
                r_client,
                webhook_id,
                entry.model_copy(update={"expires_at": refreshed}),
                now,
            )
            return REARMED
        entry = entry.model_copy(update={"expires_at": refreshed})

    if not await delete_message(session, url, entry.message_id):
        return FAILED
    await clear_webhook_pointer(r_client, webhook_id, entry)
    return DELETED


async def _sweep_batch(
    r_client: Redis,
    config: Config,
    session: aiohttp.ClientSession,
    url: str,
    webhook_id: str,
    entry: WebhookRedisEntry,
    batch_id: str,
    now: float,
) -> str:
    batch = await _load_batch(r_client, batch_id)
    if not batch or not batch.items:
        # Without the batch entry there is no way to render survivors, and the
        # items are not recoverable from the message itself. Drop the message.
        if not await delete_message(session, url, entry.message_id):
            return FAILED
        await clear_webhook_pointer(r_client, webhook_id, entry)
        return DELETED

    items = batch.items
    survivors = [
        item for item in items if item.expires_at is None or item.expires_at > now
    ]

    if len(survivors) == len(items):
        # Nothing actually elapsed (the batch was extended since it was
        # indexed); only the score needs correcting.
        await _store_batch(r_client, batch, now)
        await store_webhook_pointer(
            r_client,
            webhook_id,
            entry.model_copy(
                update={"expires_at": webhook_helpers.batch_sweep_ts(items)}
            ),
            now,
        )
        return REARMED

    if not survivors:
        if not await delete_message(session, url, entry.message_id):
            return FAILED
        # Drop the per-alert pointers so a later update posts a fresh message
        # instead of patching the one we just deleted.
        for item in items:
            await r_client.delete(webhook_helpers._alert_batch_key(item.webhook_id))
        await clear_webhook_pointer(r_client, webhook_id, entry)
        return DELETED

    grouped = webhook_helpers.build_grouped_webhook(survivors, config)
    if not await patch_message(session, url, entry.message_id, grouped):
        return FAILED
    # Elapsed alerts must not keep the row pointer of the item just removed.
    for item in items:
        if item.webhook_id not in {survivor.webhook_id for survivor in survivors}:
            await r_client.delete(webhook_helpers._alert_batch_key(item.webhook_id))
    await _store_batch(r_client, batch.model_copy(update={"items": survivors}), now)
    await store_webhook_pointer(
        r_client,
        webhook_id,
        entry.model_copy(
            update={
                "expires_at": webhook_helpers.batch_sweep_ts(survivors),
                "message_hash": webhook_helpers._webhook_hash(grouped),
            }
        ),
        now,
    )
    return REPATCHED


async def _load_batch(
    r_client: Redis, batch_id: str
) -> Optional[webhook_helpers.PendingBatchEntry]:
    raw = await r_client.get(webhook_helpers._batch_entry_key(batch_id))
    if not raw:
        return None
    try:
        return webhook_helpers.PendingBatchEntry.model_validate_json(_decode(raw))
    except ValidationError as err:
        logger.error(f"Failed to parse batch entry {batch_id}", exc_info=err)
        return None


async def _store_batch(
    r_client: Redis, batch: webhook_helpers.PendingBatchEntry, now: float
) -> None:
    await r_client.set(
        webhook_helpers._batch_entry_key(batch.batch_id),
        batch.model_dump_json(),
        ex=webhook_helpers.entry_ttl(webhook_helpers.batch_sweep_ts(batch.items), now),
    )
