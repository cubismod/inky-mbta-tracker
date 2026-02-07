from datetime import datetime, timezone

import pytest
from config import Config
from shared_types.shared_types import (
    DiscordEmbed,
    DiscordEmbedFooter,
    DiscordWebhook,
)
from webhook import helpers as webhook_helpers
from webhook.discord_webhook import (
    BATCH_WINDOW_SECONDS,
    SHORT_BATCH_WINDOW_SECONDS,
    PendingBatchItem,
    _get_pending_batch_entry,
    enqueue_pending_batch,
)


def _webhook_with_color(color: int, header: str, updated_at: str) -> DiscordWebhook:
    embed = DiscordEmbed(description=header, timestamp=updated_at, color=color)
    return DiscordWebhook(embeds=[embed])


def _iso_time(value: int) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


class InMemoryRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(
        self, key: str, value: str, ex: int | None = None, nx: bool = False
    ) -> bool:
        if nx and key in self.store:
            return False
        self.store[key] = value
        return True

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)


def test_line_color_emoji_mapping():
    assert webhook_helpers._line_color_emoji(16395559) == "🔴"
    assert webhook_helpers._line_color_emoji(16615939) == "🟠"
    assert webhook_helpers._line_color_emoji(33104) == "🟢"
    assert webhook_helpers._line_color_emoji(3104166) == "🔵"
    assert webhook_helpers._line_color_emoji(8075404) == "🟣"
    assert webhook_helpers._line_color_emoji(10132637) == "🩶"
    assert webhook_helpers._line_color_emoji(0) == "⚪"


def test_grouped_webhook_includes_timestamp_and_expired():
    updated_at = _iso_time(1_700_000_000)
    active = _webhook_with_color(16460583, "Red Line delay", updated_at)
    expired_embed = DiscordEmbed(
        description="Orange Line alert",
        timestamp=updated_at,
        color=16681731,
        footer=DiscordEmbedFooter(text="EXPIRED"),
    )
    expired = DiscordWebhook(embeds=[expired_embed])

    items = [
        PendingBatchItem(
            webhook_id="a1",
            webhook_json=active.model_dump_json(),
            message_hash="m1",
            updated_at=1_700_000_000,
            created_at=updated_at,
            routes=["Red"],
        ),
        PendingBatchItem(
            webhook_id="a2",
            webhook_json=expired.model_dump_json(),
            message_hash="m2",
            updated_at=1_700_000_000,
            created_at=updated_at,
            routes=["Orange"],
        ),
    ]
    grouped = webhook_helpers.build_grouped_webhook(items, Config(stops=[]))
    assert grouped.embeds
    fields = grouped.embeds[0].fields
    assert fields
    assert "<t:1700000000:R>" in fields[0].value
    assert "EXPIRED" in fields[1].name
    assert "~~" in fields[1].name
    assert "~~" in fields[1].value


def test_grouped_webhook_dedupes_items():
    updated_at = _iso_time(1_700_000_000)
    active = _webhook_with_color(16395559, "Red Line delay", updated_at)
    item = PendingBatchItem(
        webhook_id="a1",
        webhook_json=active.model_dump_json(),
        message_hash="m1",
        updated_at=1_700_000_000,
        created_at=updated_at,
        routes=["Red"],
    )
    grouped = webhook_helpers.build_grouped_webhook([item, item], Config(stops=[]))
    assert grouped.embeds
    assert grouped.embeds[0].fields
    assert len(grouped.embeds[0].fields) == 1


@pytest.mark.asyncio
async def test_batch_short_circuit_extends_to_full_window():
    r_client = InMemoryRedis()
    now = 1_000.0
    webhook = DiscordWebhook(embeds=[DiscordEmbed(description="A")])

    scheduled, batch_id = await enqueue_pending_batch(
        "a",
        webhook,
        ["Red"],
        "2024-01-01T00:00:00Z",
        r_client,  # type: ignore[arg-type]
        clock=lambda: now,
    )
    assert scheduled is True
    assert batch_id is None
    pending = await _get_pending_batch_entry(r_client)  # type: ignore[arg-type]
    assert pending
    assert pending.ready_at == now + SHORT_BATCH_WINDOW_SECONDS
    assert pending.first_seen == now
    assert pending.extended is False

    now += 30
    webhook_b = DiscordWebhook(embeds=[DiscordEmbed(description="B")])
    scheduled, batch_id = await enqueue_pending_batch(
        "b",
        webhook_b,
        ["Red"],
        "2024-01-01T00:00:10Z",
        r_client,  # type: ignore[arg-type]
        clock=lambda: now,
    )
    assert batch_id is None
    pending = await _get_pending_batch_entry(r_client)  # type: ignore[arg-type]
    assert pending
    assert pending.first_seen is not None
    assert pending.ready_at == pending.first_seen + BATCH_WINDOW_SECONDS
    assert pending.extended is True
