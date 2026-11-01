import hashlib
import logging
import re
import time
from datetime import datetime, timezone
from typing import Callable, Optional

from config import Config
from consts import DAY, MINUTE
from mbta_responses import AlertResource
from pydantic import BaseModel, Field, ValidationError
from redis import ResponseError
from redis.asyncio.client import Redis
from shared_types.shared_types import (
    DiscordEmbed,
    DiscordEmbedAuthor,
    DiscordEmbedField,
    DiscordEmbedFooter,
    DiscordWebhook,
)

# hex_color_to_int is imported locally inside functions that need it to avoid
# circular import issues when this module is imported during test collection.

logger = logging.getLogger(__name__)

PENDING_WEBHOOK_PREFIX = "webhook:pending"
PENDING_WEBHOOK_LOCK_PREFIX = "webhook:pending:lock"
PENDING_WEBHOOK_TTL = 10 * MINUTE
PENDING_WEBHOOK_DELAY_RANGE = (5.0, 15.0)
BATCH_WINDOW_SECONDS = 8 * MINUTE
SHORT_BATCH_WINDOW_SECONDS = MINUTE
BATCH_ENTRY_PREFIX = "webhook:batch:entry"
ALERT_BATCH_PREFIX = "webhook:batch:alert"
ALERT_DEDUP_KEY = "webhook:dedup"
PENDING_BATCH_KEY = "webhook:pending:batch"
PENDING_BATCH_LOCK_PREFIX = "webhook:pending:batch:lock"
PENDING_BATCH_SENDER_KEY = "webhook:pending:batch:sender"
PENDING_BATCH_TTL = 5 * MINUTE
BATCH_ENTRY_TTL = DAY
# Pointers for alerts with no finite end are never swept, so they must not
# inherit the flat day: losing one strands its message permanently.
OPEN_ENDED_TTL = 30 * DAY
BATCH_WEBHOOK_ID = "batch"
# Sorted set of webhook ids scored by the moment they should be swept. The
# member is the same id used for the `webhook:{id}` pointer key, so `batch:{id}`
# members address grouped messages and bare alert ids address single messages.
WEBHOOK_EXPIRY_INDEX_KEY = "webhook:expiry"


class PendingWebhookEntry(BaseModel):
    webhook_json: str
    message_hash: str
    ready_at: float


class PendingBatchItem(BaseModel):
    webhook_id: str
    webhook_json: str
    message_hash: str
    updated_at: float
    created_at: Optional[str] = None
    routes: list[str] = Field(default_factory=list)
    # Unix timestamp at which every active period of this alert has ended.
    # None means the alert is open-ended and never swept.
    expires_at: Optional[float] = None


class PendingBatchEntry(BaseModel):
    batch_id: str
    ready_at: float
    items: list[PendingBatchItem]
    first_seen: Optional[float] = None
    extended: bool = False


def _create_embeds_hash(webhook: DiscordWebhook) -> str:
    h = hashlib.sha512()
    hashed_str = webhook.model_dump_json(include={"embeds": True})
    regex = re.compile(r"<t:\d+:R>")
    # strip timestamps to increase collisions
    cleaned_str = regex.sub("<t:TIMESTAMP:R>", hashed_str)
    h.update(cleaned_str.encode("utf-8"))
    return h.hexdigest()


async def set_webhook_duplicate(r_client: Redis, webhook: DiscordWebhook) -> None:
    try:
        await r_client.hsetex(
            ALERT_DEDUP_KEY, _create_embeds_hash(webhook), "1", ex=DAY
        )  # pyright: ignore
    except ResponseError as Err:
        logger.error("Redis error during duplicate set", exc_info=Err)


async def get_webhook_duplicate(r_client: Redis, webhook: DiscordWebhook) -> bool:
    try:
        resp = await r_client.hget(ALERT_DEDUP_KEY, _create_embeds_hash(webhook))  # pyright: ignore
        if resp:
            return True
    except ResponseError as Err:
        logger.error("Redis error during duplicate check", exc_info=Err)
    return False


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

    # Import locally to avoid circular import at module import time
    from utils import hex_color_to_int

    if routes and len(routes) > 0:
        return hex_color_to_int(lookup_route_color(routes[0]))
    return 5793266


def _pending_key(webhook_id: str) -> str:
    return f"{PENDING_WEBHOOK_PREFIX}:{webhook_id}"


def _pending_lock_key(webhook_id: str) -> str:
    return f"{PENDING_WEBHOOK_LOCK_PREFIX}:{webhook_id}"


def _batch_key() -> str:
    return PENDING_BATCH_KEY


def _batch_lock_key() -> str:
    return f"{PENDING_BATCH_LOCK_PREFIX}:pending"


def _batch_sender_key() -> str:
    return PENDING_BATCH_SENDER_KEY


def _batch_entry_key(batch_id: str) -> str:
    return f"{BATCH_ENTRY_PREFIX}:{batch_id}"


def _batch_entry_lock_key(batch_id: str) -> str:
    return f"{PENDING_BATCH_LOCK_PREFIX}:{batch_id}"


def _alert_batch_key(alert_id: str) -> str:
    return f"{ALERT_BATCH_PREFIX}:{alert_id}"


def _webhook_hash(webhook: DiscordWebhook) -> str:
    h = hashlib.sha512()
    h.update(webhook.model_dump_json().encode("utf-8"))
    return h.hexdigest()


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        cleaned = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _to_unix_timestamp(value: Optional[str]) -> Optional[int]:
    parsed = _parse_iso_datetime(value)
    if not parsed:
        return None
    return int(parsed.timestamp())


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 1)]}…"


def _mark_webhook_expired(webhook: DiscordWebhook) -> DiscordWebhook:
    if not webhook.embeds:
        return webhook
    embed = webhook.embeds[0]
    embed.footer = DiscordEmbedFooter(text="EXPIRED")
    webhook.embeds[0] = embed
    return webhook


def _upsert_batch_items(
    items: list[PendingBatchItem], new_item: PendingBatchItem
) -> list[PendingBatchItem]:
    updated: list[PendingBatchItem] = []
    replaced = False
    for existing_item in items:
        if existing_item.webhook_id == new_item.webhook_id:
            updated.append(new_item)
            replaced = True
        else:
            updated.append(existing_item)
    if not replaced:
        updated.append(new_item)
    return updated


def _alert_is_expired(
    alert: AlertResource, clock: Callable[[], float] = time.time
) -> bool:
    expiry = alert_expiry_ts(alert)
    return expiry is not None and expiry <= clock()


def alert_expiry_ts(alert: AlertResource) -> Optional[float]:
    """Unix timestamp at which every active period of the alert has ended.

    Returns None when the alert can never be considered elapsed: an empty
    ``active_period`` list, a period with ``end: None``, or an unparseable
    timestamp. This is the single source of truth for both the EXPIRED footer
    and the sweeper, so the two can never disagree.
    """
    periods = alert.attributes.active_period
    if not periods:
        return None
    latest: Optional[float] = None
    for period in periods:
        if period.end is None:
            return None
        parsed = _parse_iso_datetime(period.end)
        if parsed is None:
            return None
        end_ts = parsed.timestamp()
        latest = end_ts if latest is None else max(latest, end_ts)
    return latest


def batch_sweep_ts(items: list[PendingBatchItem]) -> Optional[float]:
    """Earliest moment a batch message needs another look.

    Uses the minimum (not the maximum) of the item expiries: expired rows are
    dropped from the shared embed while live ones stay, so the first item to
    die is the interesting one. Items that never expire are ignored; a batch
    whose items are all open-ended returns None and is never swept.
    """
    scored = [item.expires_at for item in items if item.expires_at is not None]
    if not scored:
        return None
    return min(scored)


def entry_ttl(expires_at: Optional[float], now: float) -> int:
    """TTL that keeps a webhook pointer alive until its sweep is due.

    The pointer must outlive the alert it points at, otherwise a multi-day
    shutdown alert loses the message id and can never be deleted. Open-ended
    pointers are never swept, so they get their own long TTL instead of the
    flat day: they are exactly the messages no `remove` event cleans up, and
    dropping the pointer turns them into unrecoverable orphans.
    """
    if expires_at is None:
        return OPEN_ENDED_TTL
    return int(max(expires_at - now, 0)) + DAY


def _webhook_updated_at(
    webhook: DiscordWebhook, clock: Callable[[], float] = time.time
) -> float:
    if webhook.embeds:
        unix_ts = _to_unix_timestamp(webhook.embeds[0].timestamp)
        if unix_ts is not None:
            return float(unix_ts)
    return float(clock())


def _line_color_emoji(color: Optional[int]) -> str:
    # Import locally to avoid circular import at module import time
    from utils import hex_color_to_int

    if color is None:
        return "⚪"
    if color == hex_color_to_int("#FA2D27"):
        return "🔴"
    if color == hex_color_to_int("#FD8A03"):
        return "🟠"
    if color == hex_color_to_int("#008150"):
        return "🟢"
    if color == hex_color_to_int("#2F5DA6"):
        return "🔵"
    if color == hex_color_to_int("#7B388C"):
        return "🟣"
    if color == hex_color_to_int("#9A9C9D"):
        return "🩶"
    return "⚪"


def _webhook_is_expired(webhook: DiscordWebhook) -> bool:
    if not webhook.embeds:
        return False
    footer = webhook.embeds[0].footer
    return footer is not None and footer.text == "EXPIRED"


def build_grouped_webhook(
    items: list[PendingBatchItem], config: Config
) -> DiscordWebhook:
    unique_items: dict[str, PendingBatchItem] = {}
    for item in items:
        unique_items[item.webhook_id] = item
    items = list(unique_items.values())
    parsed_items: list[tuple[PendingBatchItem, DiscordWebhook]] = []
    for item in items:
        try:
            webhook = DiscordWebhook.model_validate_json(item.webhook_json)
        except ValidationError as err:
            logger.error(
                f"Failed to parse pending batch payload for {item.webhook_id}",
                exc_info=err,
            )
            continue
        parsed_items.append((item, webhook))

    fields: list[DiscordEmbedField] = []
    created_with_webhook: list[tuple[float, PendingBatchItem, DiscordWebhook]] = []
    for item, webhook in parsed_items:
        created_at = _to_unix_timestamp(item.created_at)
        if created_at is None:
            created_at = int(item.updated_at) if item.updated_at else 0
        created_with_webhook.append(
            (
                float(created_at),
                item,
                webhook,
            )
        )
    created_with_webhook.sort(key=lambda entry: entry[0], reverse=True)

    for _, item, webhook in created_with_webhook[:25]:
        if not webhook.embeds:
            continue
        embed = webhook.embeds[0]
        header = embed.description or "Alert"
        route_label = ", ".join(item.routes) if item.routes else ""
        if not route_label and embed.fields:
            for field in embed.fields:
                if field.name == "Lines":
                    route_label = field.value
                    break
        prefix = f"{route_label} — " if route_label else ""
        emoji = _line_color_emoji(embed.color)
        updated_at = _to_unix_timestamp(embed.timestamp)
        updated_text = (
            f"Updated: <t:{updated_at}:R>" if updated_at is not None else "Updated"
        )
        expired = _webhook_is_expired(webhook)
        field_header = _truncate(f"{prefix}{header}", 256)
        if expired:
            name = _truncate(f"{emoji} EXPIRED: ~~{field_header}~~", 256)
            value = _truncate(f"~~{updated_text}~~", 1024)
        else:
            name = _truncate(f"{emoji} {field_header}", 256)
            value = _truncate(updated_text, 1024)
        fields.append(DiscordEmbedField(name=name, value=value, inline=False))

    total_count = len(created_with_webhook)
    description = f"{total_count} alerts"
    if total_count > 25:
        description = f"{total_count} alerts (showing 25)"
    embed = DiscordEmbed(
        description=description,
        author=DiscordEmbedAuthor(
            name="MBTA Alerts (batch)",
            url="https://ryanwallace.cloud/alerts",
        ),
        color=created_with_webhook[0][2].embeds[0].color
        if created_with_webhook and created_with_webhook[0][2].embeds
        else None,
        fields=fields,
    )

    avatar_url = None
    if created_with_webhook:
        avatar_url = created_with_webhook[0][2].avatar_url
    return DiscordWebhook(avatar_url=avatar_url, embeds=[embed])
