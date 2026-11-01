from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from config import Config
from exceptions import RateLimitExceeded
from mbta_responses import AlertResource
from shared_types.shared_types import DiscordEmbed, DiscordWebhook, WebhookRedisEntry
from webhook import helpers as webhook_helpers
from webhook.cleanup import sweep_expired_webhooks
from webhook.discord_webhook import (
    PendingBatchEntry,
    PendingBatchItem,
    delete_message,
    enqueue_pending_batch,
    store_webhook_pointer,
)

NOW = 1_700_000_000.0
WEBHOOK_URL = "https://discord.test/api/webhooks/1/token"


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _alert(alert_id: str, ends: list[str | None]) -> AlertResource:
    return AlertResource.model_validate(
        {
            "type": "alert",
            "id": alert_id,
            "attributes": {
                "cause": "CONSTRUCTION",
                "created_at": _iso(NOW - 600),
                "header": "Red Line delay",
                "short_header": "Red delay",
                "updated_at": _iso(NOW - 60),
                "effect": "DELAY",
                "active_period": [{"start": None, "end": end} for end in ends],
                "informed_entity": [{"route": "Red"}],
                "severity": 5,
            },
        }
    )


def _webhook(header: str) -> DiscordWebhook:
    return DiscordWebhook(embeds=[DiscordEmbed(description=header)])


class InMemoryRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttl: dict[str, int | None] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(
        self,
        key: str,
        value: str | None = None,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool:
        if nx and key in self.store:
            return False
        self.store[key] = value or ""
        self.ttl[key] = ex
        return True

    async def delete(self, *keys: str) -> int:
        for key in keys:
            self.store.pop(key, None)
            self.ttl.pop(key, None)
        return len(keys)

    async def zadd(self, name: str, mapping: dict[str, float]) -> int:
        zset = self.zsets.setdefault(name, {})
        for member, score in mapping.items():
            zset[str(member)] = float(score)
        return len(mapping)

    async def zrem(self, name: str, *members: str) -> int:
        zset = self.zsets.get(name)
        if zset is None:
            return 0
        return sum(1 for member in members if zset.pop(str(member), None) is not None)

    async def zrangebyscore(
        self,
        name: str,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
    ) -> list[str]:
        zset = self.zsets.get(name, {})
        low = float("-inf") if min == "-inf" else float(min)
        high = float("inf") if max == "+inf" else float(max)
        members = sorted(
            (m for m, score in zset.items() if low <= score <= high),
            key=lambda member: (zset[member], member),
        )
        if start:
            members = members[start:]
        if num is not None:
            members = members[:num]
        return members


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


def _enable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("webhook.cleanup.WEBHOOK_URL", WEBHOOK_URL)
    monkeypatch.setattr("webhook.cleanup.RedisLock", DummyLock)


def _item(alert_id: str, header: str, expires_at: float | None) -> PendingBatchItem:
    return PendingBatchItem(
        webhook_id=alert_id,
        webhook_json=_webhook(header).model_dump_json(),
        message_hash=f"hash-{alert_id}",
        updated_at=NOW,
        routes=["Red"],
        expires_at=expires_at,
    )


async def _seed_batch(r: InMemoryRedis, items: list[PendingBatchItem]) -> str:
    batch = PendingBatchEntry(batch_id="b1", ready_at=0.0, items=items)
    await r.set(webhook_helpers._batch_entry_key("b1"), batch.model_dump_json())
    for item in items:
        await r.set(webhook_helpers._alert_batch_key(item.webhook_id), "b1")
    webhook_id = "batch:b1"
    await store_webhook_pointer(
        r,  # type: ignore[arg-type]
        webhook_id,
        WebhookRedisEntry(
            message_id="m1",
            message_hash="h",
            expires_at=webhook_helpers.batch_sweep_ts(items),
            kind="batch",
            batch_id="b1",
        ),
        NOW,
    )
    return webhook_id


async def _seed_single(r: InMemoryRedis, expires_at: float | None) -> None:
    await store_webhook_pointer(
        r,  # type: ignore[arg-type]
        "alert-1",
        WebhookRedisEntry(message_id="m1", message_hash="h", expires_at=expires_at),
        NOW,
    )


async def _sweep(r: InMemoryRedis):
    return await sweep_expired_webhooks(
        r,  # type: ignore[arg-type]
        Config(stops=[]),
        clock=lambda: NOW,
    )


# --- expiry rules -----------------------------------------------------------


@pytest.mark.parametrize(
    ("ends", "expected"),
    [
        # Latest end wins: an alert with a future period is not over.
        ([_iso(NOW - 100), _iso(NOW + 500)], NOW + 500),
        # One open-ended period makes the whole alert unsweepable.
        ([_iso(NOW + 100), None], None),
        ([], None),
    ],
)
def test_alert_expiry_ts(ends: list[str | None], expected: float | None):
    got = webhook_helpers.alert_expiry_ts(_alert("a1", ends))
    if expected is None:
        assert got is None
    else:
        assert got == pytest.approx(expected)


def test_alert_is_expired_matches_expiry():
    assert webhook_helpers._alert_is_expired(
        _alert("a1", [_iso(NOW - 10)]), clock=lambda: NOW
    )
    assert not webhook_helpers._alert_is_expired(
        _alert("a2", [_iso(NOW + 10)]), clock=lambda: NOW
    )
    assert not webhook_helpers._alert_is_expired(
        _alert("a3", [None]), clock=lambda: NOW
    )


@pytest.mark.parametrize(
    ("expiries", "expected"),
    [
        # The first row to die drives the wake-up, even if one never does.
        ([NOW + 900, None, NOW + 300], NOW + 300),
        ([None], None),
    ],
)
def test_batch_sweep_ts(expiries: list[float | None], expected: float | None):
    items = [_item(f"a{i}", f"A{i}", ts) for i, ts in enumerate(expiries)]
    assert webhook_helpers.batch_sweep_ts(items) == expected


def test_entry_ttl_outlives_the_alert():
    expires_at = NOW + 5 * webhook_helpers.DAY
    assert webhook_helpers.entry_ttl(expires_at, NOW) >= 6 * webhook_helpers.DAY
    assert webhook_helpers.entry_ttl(None, NOW) == webhook_helpers.OPEN_ENDED_TTL
    assert webhook_helpers.OPEN_ENDED_TTL > webhook_helpers.BATCH_ENTRY_TTL


# --- pointer registration ---------------------------------------------------


@pytest.mark.asyncio
async def test_store_pointer_indexes_by_expiry():
    r = InMemoryRedis()
    await _seed_single(r, NOW + 60)
    assert r.zsets[webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY]["alert-1"] == NOW + 60
    assert r.ttl["webhook:alert-1"] == webhook_helpers.entry_ttl(NOW + 60, NOW)


@pytest.mark.asyncio
async def test_store_pointer_never_indexes_open_ended_alert():
    r = InMemoryRedis()
    await _seed_single(r, None)
    assert not r.zsets.get(webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY)
    assert r.ttl["webhook:alert-1"] == webhook_helpers.OPEN_ENDED_TTL


@pytest.mark.asyncio
async def test_enqueue_pending_batch_carries_expiry_into_item(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr("webhook.discord_webhook.RedisLock", DummyLock)
    r = InMemoryRedis()
    scheduled, batch_id = await enqueue_pending_batch(
        "alert-1",
        _webhook("A"),
        ["Red"],
        _iso(NOW),
        r,  # type: ignore[arg-type]
        clock=lambda: NOW,
        expires_at=NOW + 3600,
    )
    assert scheduled is True
    assert batch_id is None
    pending = webhook_helpers.PendingBatchEntry.model_validate_json(
        r.store[webhook_helpers._batch_key()]
    )
    assert pending.items[0].expires_at == NOW + 3600


# --- sweeper ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_sweep_noops_without_work(monkeypatch: pytest.MonkeyPatch):
    delete_mock = AsyncMock(return_value=True)
    monkeypatch.setattr("webhook.cleanup.delete_message", delete_mock)
    r = InMemoryRedis()

    # Nothing to delete with: no webhook configured.
    monkeypatch.setattr("webhook.cleanup.WEBHOOK_URL", None)
    assert (await _sweep(r)).examined == 0

    # Configured but nothing due.
    _enable(monkeypatch)
    assert (await _sweep(r)).examined == 0
    delete_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_elapsed_single_alert_is_deleted_and_cleared(
    monkeypatch: pytest.MonkeyPatch,
):
    _enable(monkeypatch)
    r = InMemoryRedis()
    await _seed_single(r, NOW - 10)
    await r.set(webhook_helpers._alert_batch_key("alert-1"), "alert-1", ex=86400)
    await r.set(webhook_helpers._batch_entry_key("alert-1"), "{}", ex=86400)

    delete_mock = AsyncMock(return_value=True)
    monkeypatch.setattr("webhook.cleanup.delete_message", delete_mock)

    result = await _sweep(r)

    assert result.deleted == 1
    assert result.examined == 1
    delete_mock.assert_awaited_once()
    assert delete_mock.call_args[0][2] == "m1"
    assert "webhook:alert-1" not in r.store
    assert webhook_helpers._alert_batch_key("alert-1") not in r.store
    assert webhook_helpers._batch_entry_key("alert-1") not in r.store
    assert not r.zsets.get(webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY)


@pytest.mark.asyncio
async def test_alert_that_still_has_a_future_period_is_kept(
    monkeypatch: pytest.MonkeyPatch,
):
    _enable(monkeypatch)
    r = InMemoryRedis()
    await _seed_single(r, NOW + 3600)
    delete_mock = AsyncMock(return_value=True)
    monkeypatch.setattr("webhook.cleanup.delete_message", delete_mock)

    # Scored in the future, so the member is not even in the due window.
    assert (await _sweep(r)).examined == 0
    delete_mock.assert_not_awaited()
    assert "webhook:alert-1" in r.store


@pytest.mark.asyncio
async def test_cached_alert_rearms_instead_of_deleting(
    monkeypatch: pytest.MonkeyPatch,
):
    """MBTA can extend an alert right as it expires; a freshly cached alert
    means the update event may still be in flight, so keep the message."""
    _enable(monkeypatch)
    r = InMemoryRedis()
    await _seed_single(r, NOW - 10)
    await r.set(
        "alert:alert-1", _alert("alert-1", [_iso(NOW + 7200)]).model_dump_json()
    )
    delete_mock = AsyncMock(return_value=True)
    monkeypatch.setattr("webhook.cleanup.delete_message", delete_mock)

    result = await _sweep(r)

    assert result.rearmed == 1
    delete_mock.assert_not_awaited()
    entry = WebhookRedisEntry.model_validate_json(r.store["webhook:alert-1"])
    assert entry.expires_at == pytest.approx(NOW + 7200)
    assert r.zsets[webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY][
        "alert-1"
    ] == pytest.approx(NOW + 7200)


@pytest.mark.asyncio
async def test_cached_alert_that_is_genuinely_over_still_deletes(
    monkeypatch: pytest.MonkeyPatch,
):
    _enable(monkeypatch)
    r = InMemoryRedis()
    await _seed_single(r, NOW - 10)
    await r.set("alert:alert-1", _alert("alert-1", [_iso(NOW - 10)]).model_dump_json())
    delete_mock = AsyncMock(return_value=True)
    monkeypatch.setattr("webhook.cleanup.delete_message", delete_mock)

    result = await _sweep(r)

    assert result.deleted == 1
    delete_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_delete_keeps_pointer_for_retry(monkeypatch: pytest.MonkeyPatch):
    _enable(monkeypatch)
    r = InMemoryRedis()
    await _seed_single(r, NOW - 10)
    monkeypatch.setattr("webhook.cleanup.delete_message", AsyncMock(return_value=False))

    result = await _sweep(r)

    assert result.failed == 1
    assert result.deleted == 0
    assert "webhook:alert-1" in r.store
    assert webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY in r.zsets


@pytest.mark.asyncio
async def test_rate_limited_delete_stops_the_pass(monkeypatch: pytest.MonkeyPatch):
    _enable(monkeypatch)
    r = InMemoryRedis()
    for index in range(3):
        await store_webhook_pointer(
            r,  # type: ignore[arg-type]
            f"alert-{index}",
            WebhookRedisEntry(
                message_id=f"m{index}", message_hash="h", expires_at=NOW - 10
            ),
            NOW,
        )
    monkeypatch.setattr(
        "webhook.cleanup.delete_message", AsyncMock(side_effect=RateLimitExceeded())
    )

    result = await _sweep(r)

    assert result.rate_limited is True
    assert result.examined == 1
    assert len(r.zsets[webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY]) == 3


@pytest.mark.asyncio
async def test_index_member_without_pointer_is_purged(monkeypatch: pytest.MonkeyPatch):
    _enable(monkeypatch)
    r = InMemoryRedis()
    await r.zadd(webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY, {"alert-gone": NOW - 10})
    delete_mock = AsyncMock(return_value=True)
    monkeypatch.setattr("webhook.cleanup.delete_message", delete_mock)

    result = await _sweep(r)

    assert result.purged == 1
    delete_mock.assert_not_awaited()
    assert not r.zsets[webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY]


# --- batched messages -------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_drops_expired_rows_and_repatchs(monkeypatch: pytest.MonkeyPatch):
    _enable(monkeypatch)
    r = InMemoryRedis()
    webhook_id = await _seed_batch(
        r,
        [
            _item("a1", "Alert A", NOW - 60),
            _item("a2", "Alert B", NOW + 3600),
        ],
    )
    patch_mock = AsyncMock(return_value=True)
    delete_mock = AsyncMock(return_value=True)
    monkeypatch.setattr("webhook.cleanup.patch_message", patch_mock)
    monkeypatch.setattr("webhook.cleanup.delete_message", delete_mock)

    result = await _sweep(r)

    assert result.repatched == 1
    delete_mock.assert_not_awaited()
    patch_mock.assert_awaited_once()
    assert patch_mock.call_args[0][2] == "m1"
    fields = patch_mock.call_args[0][3].embeds[0].fields or []
    assert len(fields) == 1
    assert "Alert B" in fields[0].name
    assert "Alert A" not in fields[0].name

    stored = PendingBatchEntry.model_validate_json(
        r.store[webhook_helpers._batch_entry_key("b1")]
    )
    assert [item.webhook_id for item in stored.items] == ["a2"]
    entry = WebhookRedisEntry.model_validate_json(r.store[f"webhook:{webhook_id}"])
    assert entry.expires_at == NOW + 3600
    assert r.zsets[webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY][webhook_id] == NOW + 3600


@pytest.mark.asyncio
async def test_batch_is_deleted_once_every_row_expired(
    monkeypatch: pytest.MonkeyPatch,
):
    _enable(monkeypatch)
    r = InMemoryRedis()
    webhook_id = await _seed_batch(
        r,
        [
            _item("a1", "Alert A", NOW - 60),
            _item("a2", "Alert B", NOW - 30),
        ],
    )
    patch_mock = AsyncMock(return_value=True)
    delete_mock = AsyncMock(return_value=True)
    monkeypatch.setattr("webhook.cleanup.patch_message", patch_mock)
    monkeypatch.setattr("webhook.cleanup.delete_message", delete_mock)

    result = await _sweep(r)

    assert result.deleted == 1
    patch_mock.assert_not_awaited()
    delete_mock.assert_awaited_once()
    assert f"webhook:{webhook_id}" not in r.store
    assert webhook_helpers._batch_entry_key("b1") not in r.store
    assert webhook_helpers._alert_batch_key("a1") not in r.store
    assert webhook_helpers._alert_batch_key("a2") not in r.store
    assert not r.zsets.get(webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY)


@pytest.mark.asyncio
async def test_batch_with_open_ended_survivor_is_never_deleted(
    monkeypatch: pytest.MonkeyPatch,
):
    _enable(monkeypatch)
    r = InMemoryRedis()
    webhook_id = await _seed_batch(
        r,
        [
            _item("a1", "Alert A", NOW - 60),
            _item("a2", "Perpetual", None),
        ],
    )
    patch_mock = AsyncMock(return_value=True)
    delete_mock = AsyncMock(return_value=True)
    monkeypatch.setattr("webhook.cleanup.patch_message", patch_mock)
    monkeypatch.setattr("webhook.cleanup.delete_message", delete_mock)

    result = await _sweep(r)

    assert result.repatched == 1
    delete_mock.assert_not_awaited()
    fields = patch_mock.call_args[0][3].embeds[0].fields or []
    assert len(fields) == 1
    assert "Perpetual" in fields[0].name

    # All remaining rows are open ended, so the message leaves the index.
    assert not r.zsets.get(webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY)
    entry = WebhookRedisEntry.model_validate_json(r.store[f"webhook:{webhook_id}"])
    assert entry.expires_at is None


@pytest.mark.asyncio
async def test_batch_extended_before_sweep_only_rearms(monkeypatch: pytest.MonkeyPatch):
    _enable(monkeypatch)
    r = InMemoryRedis()
    webhook_id = await _seed_batch(r, [_item("a1", "Alert A", NOW + 3600)])
    # Simulate a stale index score: the alert was extended after indexing.
    await r.zadd(webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY, {webhook_id: NOW - 10})
    entry = WebhookRedisEntry.model_validate_json(r.store[f"webhook:{webhook_id}"])
    await r.set(
        f"webhook:{webhook_id}",
        entry.model_copy(update={"expires_at": NOW - 10}).model_dump_json(),
    )
    patch_mock = AsyncMock(return_value=True)
    delete_mock = AsyncMock(return_value=True)
    monkeypatch.setattr("webhook.cleanup.patch_message", patch_mock)
    monkeypatch.setattr("webhook.cleanup.delete_message", delete_mock)

    result = await _sweep(r)

    assert result.rearmed == 1
    patch_mock.assert_not_awaited()
    delete_mock.assert_not_awaited()
    assert r.zsets[webhook_helpers.WEBHOOK_EXPIRY_INDEX_KEY][webhook_id] == NOW + 3600


@pytest.mark.asyncio
async def test_batch_without_entry_is_deleted(monkeypatch: pytest.MonkeyPatch):
    _enable(monkeypatch)
    r = InMemoryRedis()
    await store_webhook_pointer(
        r,  # type: ignore[arg-type]
        "batch:b1",
        WebhookRedisEntry(
            message_id="m1",
            message_hash="h",
            expires_at=NOW - 10,
            kind="batch",
            batch_id="b1",
        ),
        NOW,
    )
    delete_mock = AsyncMock(return_value=True)
    monkeypatch.setattr("webhook.cleanup.delete_message", delete_mock)

    assert (await _sweep(r)).deleted == 1
    delete_mock.assert_awaited_once()


# --- HTTP helpers -----------------------------------------------------------


class _FakeResponse:
    def __init__(self, status: int) -> None:
        self.status = status

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object | None,
    ) -> None:
        return None

    async def text(self) -> str:
        return ""


class _FakeSession:
    def __init__(self, status: int) -> None:
        self.status = status

    def delete(self, url: str) -> _FakeResponse:
        return _FakeResponse(self.status)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (204, True),
        # Discord answers 404 for an already-deleted message: treat as gone so
        # a partial delete converges instead of orphaning the pointer.
        (404, True),
        (500, False),
    ],
)
async def test_delete_message_status_handling(status: int, expected: bool):
    assert (
        await delete_message(_FakeSession(status), WEBHOOK_URL, "m1")  # type: ignore[arg-type]
        is expected
    )


@pytest.mark.asyncio
async def test_delete_message_raises_on_rate_limit():
    with pytest.raises(RateLimitExceeded):
        await delete_message(_FakeSession(429), WEBHOOK_URL, "m1")  # type: ignore[arg-type]
