# Global Discord Alert Batching Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Batch multiple alerts within a 2-minute window into one Discord message with embed fields, line-color emojis, timestamps, and expired formatting. Single alert within the window remains a single message.

**Architecture:** Use a global Redis batch entry (`webhook:pending:batch`) to collect alert webhooks for 2 minutes. On send, if one item, keep existing single-alert path; if multiple, build a grouped webhook with fields for each alert. Use a batch message cache key for patching.

**Tech Stack:** Python 3.13, Redis asyncio client, Discord webhook models.

---

### Task 1: Batch keys and helpers

**Files:**
- Modify: `inky-mbta-tracker/discord_webhook.py`

**Step 1: Write the failing test**

Add a minimal pure-function test for batch formatting helpers (see Task 6). This task does not require Redis or async clients.

**Step 2: Add constants and key helpers**

Add constants and key helpers near existing PENDING_WEBHOOK_* constants:

```python
BATCH_WINDOW_SECONDS = 2 * MINUTE
PENDING_BATCH_KEY = "webhook:pending:batch"
PENDING_BATCH_LOCK_KEY = "webhook:pending:batch:lock"
PENDING_BATCH_TTL = 5 * MINUTE


def _batch_key() -> str:
    return PENDING_BATCH_KEY


def _batch_lock_key() -> str:
    return PENDING_BATCH_LOCK_KEY
```

**Step 3: Commit**

```bash
git add inky-mbta-tracker/discord_webhook.py inky-mbta-tracker/tests/test_discord_webhook_batching.py
git commit -m "add global batching keys"
```

---

### Task 2: Batch entry model and enqueue

**Files:**
- Modify: `inky-mbta-tracker/discord_webhook.py`

**Step 1: Write the failing test**

Add a test that builds a grouped webhook from a list of webhooks and expects correct emoji mapping and timestamps.

**Step 2: Define batch entry model**

```python
class PendingBatchEntry(BaseModel):
    ready_at: float
    alerts_json: list[str]
```

**Step 3: Implement enqueue function**

```python
async def enqueue_pending_batch(
    webhook: DiscordWebhook,
    r_client: RedisClient,
    delay_seconds: float = BATCH_WINDOW_SECONDS,
    clock: Callable[[], float] = time.time,
) -> bool:
    ...
```

Behavior:
- If existing: append webhook JSON, extend TTL, return False
- If new: create `ready_at = now + delay_seconds`, return True

**Step 4: Add delayed send**

Implement `_delayed_send_batch(...)` similar to `_delayed_send_pending`.

**Step 5: Commit**

```bash
git add inky-mbta-tracker/discord_webhook.py inky-mbta-tracker/tests/test_discord_webhook_batching.py
git commit -m "add batch queueing"
```

---

### Task 3: Grouped webhook formatting (line-color emojis)

**Files:**
- Modify: `inky-mbta-tracker/discord_webhook.py`

**Step 1: Write the failing test**

Add tests for:
- Emoji mapping for known line colors, including purple -> `🟣`
- `Updated: <t:...:R>` in each field
- Expired formatting uses `EXPIRED` and `~~`

**Step 2: Implement emoji mapping**

Map `embed.color` to emoji:
- Red -> `🔴`
- Orange -> `🟠`
- Green -> `🟢`
- Blue -> `🔵`
- Purple (Commuter Rail) -> `🟣`
- Fallback -> `⚪`

**Step 3: Implement grouped webhook builder**

```python
def build_grouped_webhook(
    webhooks: list[DiscordWebhook],
    config: Config,
) -> DiscordWebhook:
    ...
```

Use one embed with fields:
- Field name: `"{emoji} {header}"` or `"{emoji} EXPIRED: {header}"`
- Field value: `"Updated: <t:UNIX:R>"` plus optional details
- Expired: wrap strings in `~~`

**Step 4: Commit**

```bash
git add inky-mbta-tracker/discord_webhook.py inky-mbta-tracker/tests/test_discord_webhook_batching.py
git commit -m "add grouped webhook formatting"
```

---

### Task 4: Integrate global batching

**Files:**
- Modify: `inky-mbta-tracker/discord_webhook.py`

**Step 1: Update process flow**

In `process_alert_event`, enqueue into global batch and start delayed batch sender if scheduled.

**Step 2: Implement send_pending_batch**

Acquire batch lock, load entry, and:
- If one webhook: use existing single-alert post/patch flow
- If multiple: build grouped webhook and post/patch using a batch cache key

**Step 3: Commit**

```bash
git add inky-mbta-tracker/discord_webhook.py
git commit -m "use global batch for discord alerts"
```

---

### Task 5: Expired formatting

**Files:**
- Modify: `inky-mbta-tracker/discord_webhook.py`

**Step 1: Ensure expired is detectable**

If expired state is not already present in the webhook JSON, add a minimal marker when creating the embed (e.g. prefix in description) and parse it in `build_grouped_webhook`.

**Step 2: Apply formatting**

Use `EXPIRED` in field name and `~~` strikethrough in values.

**Step 3: Commit**

```bash
git add inky-mbta-tracker/discord_webhook.py inky-mbta-tracker/tests/test_discord_webhook_batching.py
git commit -m "format expired alerts in batch"
```

---

### Task 6: Basic unit tests (no Redis)

**Files:**
- Create: `inky-mbta-tracker/tests/test_discord_webhook_batching.py`

Tests only call pure functions or construct `DiscordWebhook` instances. Do not mock Redis clients.

Run:
```
uv run pytest inky-mbta-tracker/tests/test_discord_webhook_batching.py -v
```

---

### Task 7: Lint/format

Run:
```
task check
task format
```

---

### Task 8: Commit

```bash
git add inky-mbta-tracker/discord_webhook.py inky-mbta-tracker/tests/test_discord_webhook_batching.py
git commit -m "add global batching for discord alerts"
```
