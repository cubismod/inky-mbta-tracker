from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from config import Config
from shared_types.shared_types import (
    DiscordEmbed,
    DiscordEmbedFooter,
    DiscordWebhook,
)
from webhook import helpers as webhook_helpers
from webhook.discord_webhook import (
    BATCH_ENTRY_TTL,
    BATCH_WINDOW_SECONDS,
    SHORT_BATCH_WINDOW_SECONDS,
    PendingBatchEntry,
    PendingBatchItem,
    _get_batch_entry,
    _get_pending_batch_entry,
    enqueue_pending_batch,
    send_batch_entry,
    send_pending_batch,
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


class DummyLock:
    def __init__(self, *args: object, **kwargs: object) -> None:
        pass

    async def __aenter__(self) -> "DummyLock":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object | None,
    ) -> None:
        return None


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

    with patch("webhook.discord_webhook.RedisLock", DummyLock):
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
    with patch("webhook.discord_webhook.RedisLock", DummyLock):
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


@pytest.mark.asyncio
async def test_send_pending_batch_single_item_sets_alert_batch_key():
    """After a single-item batch fires, _alert_batch_key must be set so
    subsequent updates don't create a duplicate Discord message."""
    r_client = InMemoryRedis()
    now = 1_000.0
    webhook = DiscordWebhook(embeds=[DiscordEmbed(description="A")])

    with patch("webhook.discord_webhook.RedisLock", DummyLock):
        await enqueue_pending_batch(
            "alert-1",
            webhook,
            ["Red"],
            "2024-01-01T00:00:00Z",
            r_client,  # type: ignore[arg-type]
            clock=lambda: now,
        )

    mock_send = AsyncMock()
    with (
        patch("webhook.discord_webhook._send_webhook_payload", mock_send),
        patch("webhook.discord_webhook.RedisLock", DummyLock),
    ):
        await send_pending_batch(r_client, Config(stops=[]), clock=lambda: now + 120)  # type: ignore[arg-type]

    # _alert_batch_key("alert-1") should now point to the alert's own id
    alert_batch_id = await r_client.get(webhook_helpers._alert_batch_key("alert-1"))
    assert alert_batch_id == "alert-1"

    # batch entry should exist so future updates can update it
    batch_entry = await _get_batch_entry(r_client, "alert-1")  # type: ignore[arg-type]
    assert batch_entry is not None
    assert len(batch_entry.items) == 1
    assert batch_entry.items[0].webhook_id == "alert-1"

    # _send_webhook_payload was called with the alert's own id (not "batch:…")
    mock_send.assert_awaited_once()
    call_webhook_id = mock_send.call_args[0][0]
    assert call_webhook_id == "alert-1"


@pytest.mark.asyncio
async def test_send_batch_entry_single_item_uses_individual_webhook_id():
    """send_batch_entry for a single-item batch must route through the
    alert's own webhook_id so that PATCH hits the right Discord message."""
    r_client = InMemoryRedis()
    webhook = DiscordWebhook(embeds=[DiscordEmbed(description="A")])
    item = PendingBatchItem(
        webhook_id="alert-1",
        webhook_json=webhook.model_dump_json(),
        message_hash="hash1",
        updated_at=1_000.0,
        created_at="2024-01-01T00:00:00Z",
        routes=["Red"],
    )
    entry = PendingBatchEntry(
        batch_id="alert-1",
        ready_at=0.0,
        items=[item],
    )
    await r_client.set(
        webhook_helpers._batch_entry_key("alert-1"),
        entry.model_dump_json(),
        ex=BATCH_ENTRY_TTL,
    )

    mock_send = AsyncMock()
    with (
        patch("webhook.discord_webhook._send_webhook_payload", mock_send),
        patch("webhook.discord_webhook.RedisLock", DummyLock),
    ):
        await send_batch_entry("alert-1", r_client, Config(stops=[]))  # type: ignore[arg-type]

    mock_send.assert_awaited_once()
    call_webhook_id = mock_send.call_args[0][0]
    assert call_webhook_id == "alert-1"
    assert "batch" not in call_webhook_id


@pytest.mark.asyncio
async def test_no_duplicate_on_single_item_batch_then_update_with_new_alert():
    """Regression test: when alert A was previously sent as a single-item
    batch and then alert A's update arrives alongside new alert B, A must
    route through its own batch entry (PATCH) rather than appearing in a
    new batch POST."""
    r_client = InMemoryRedis()
    now = 1_000.0
    webhook_a = DiscordWebhook(embeds=[DiscordEmbed(description="A")])

    # Step 1: alert A arrives alone, batch fires as single item
    with patch("webhook.discord_webhook.RedisLock", DummyLock):
        await enqueue_pending_batch(
            "alert-a",
            webhook_a,
            ["Red"],
            "2024-01-01T00:00:00Z",
            r_client,  # type: ignore[arg-type]
            clock=lambda: now,
        )

    mock_send = AsyncMock()
    with (
        patch("webhook.discord_webhook._send_webhook_payload", mock_send),
        patch("webhook.discord_webhook.RedisLock", DummyLock),
    ):
        await send_pending_batch(r_client, Config(stops=[]), clock=lambda: now + 120)  # type: ignore[arg-type]

    # Verify state after single-item send
    assert await r_client.get(webhook_helpers._alert_batch_key("alert-a")) == "alert-a"
    assert await _get_batch_entry(r_client, "alert-a") is not None  # type: ignore[arg-type]

    # Step 2: alert A updated + new alert B arrives
    now += 300
    webhook_a2 = DiscordWebhook(embeds=[DiscordEmbed(description="A updated")])
    webhook_b = DiscordWebhook(embeds=[DiscordEmbed(description="B")])

    with patch("webhook.discord_webhook.RedisLock", DummyLock):
        scheduled_a, batch_id_a = await enqueue_pending_batch(
            "alert-a",
            webhook_a2,
            ["Red"],
            "2024-01-01T00:05:00Z",
            r_client,  # type: ignore[arg-type]
            clock=lambda: now,
        )
        # A has an existing batch entry, so it returns the existing batch_id
        assert batch_id_a == "alert-a"
        assert scheduled_a is False

    with patch("webhook.discord_webhook.RedisLock", DummyLock):
        scheduled_b, batch_id_b = await enqueue_pending_batch(
            "alert-b",
            webhook_b,
            ["Blue"],
            "2024-01-01T00:05:00Z",
            r_client,  # type: ignore[arg-type]
            clock=lambda: now,
        )
        # B is new so it goes into a fresh pending batch
        assert batch_id_b is None

    # A's update is routed to its own batch entry, not the pending batch
    batch_entry_a = await _get_batch_entry(r_client, "alert-a")  # type: ignore[arg-type]
    assert batch_entry_a is not None
    assert batch_entry_a.items[0].webhook_id == "alert-a"
    assert batch_entry_a.items[0].webhook_json == webhook_a2.model_dump_json()
