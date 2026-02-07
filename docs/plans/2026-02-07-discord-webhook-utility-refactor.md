# Discord Webhook Utility Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move utility/helper logic from `inky-mbta-tracker/discord_webhook.py` into a dedicated utilities module within a new directory, keeping behavior identical.

**Architecture:** Extract pure helper functions and related types into a new module under a new `discord_webhook` package. Keep orchestration and IO in `discord_webhook.py`, importing helpers from the new module to reduce file size and improve cohesion.

**Tech Stack:** Python 3.13, Pydantic, AnyIO, aiohttp, Redis, Tenacity

---

### Task 1: Create new utilities module with extracted helpers

**Files:**
- Create: `inky-mbta-tracker/discord_webhook/helpers.py`
- Modify: `inky-mbta-tracker/discord_webhook.py`

**Step 1: Write the failing test**

Create a new test that imports a helper function from the new module to ensure the module is discoverable and wired correctly.

```python
from inky_mbta_tracker.discord_webhook.helpers import determine_alert_routes


def test_helpers_module_importable():
    assert callable(determine_alert_routes)
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest inky-mbta-tracker/tests/test_discord_webhook_helpers.py::test_helpers_module_importable -v`

Expected: FAIL with `ModuleNotFoundError` for the new helpers module.

**Step 3: Write minimal implementation**

Create `inky-mbta-tracker/discord_webhook/helpers.py` and move the following from `discord_webhook.py`:

- `determine_alert_routes`
- `determine_alert_color`
- `_pending_key`
- `_pending_lock_key`
- `_batch_key`
- `_batch_lock_key`
- `_batch_entry_key`
- `_batch_entry_lock_key`
- `_alert_batch_key`
- `_webhook_hash`
- `_parse_iso_datetime`
- `_to_unix_timestamp`
- `_truncate`
- `_mark_webhook_expired`
- `_upsert_batch_items`
- `_alert_is_expired`
- `_webhook_updated_at`
- `_line_color_emoji`
- `_webhook_is_expired`
- `build_grouped_webhook`

Also move the `PendingWebhookEntry`, `PendingBatchItem`, and `PendingBatchEntry` Pydantic models if they are only used as helpers (keep if referenced widely; otherwise move).

Update imports in `discord_webhook.py` to use the new module.

**Step 4: Run test to verify it passes**

Run: `uv run pytest inky-mbta-tracker/tests/test_discord_webhook_helpers.py::test_helpers_module_importable -v`

Expected: PASS

**Step 5: Commit**

```bash
git add inky-mbta-tracker/discord_webhook/helpers.py inky-mbta-tracker/discord_webhook.py inky-mbta-tracker/tests/test_discord_webhook_helpers.py
git commit -m "refactor discord webhook helpers"
```

### Task 2: Introduce new directory/package for discord webhook utilities

**Files:**
- Create: `inky-mbta-tracker/discord_webhook/__init__.py`
- Modify: `inky-mbta-tracker/discord_webhook.py`

**Step 1: Write the failing test**

Add a test that imports from the package path to ensure the directory is a package.

```python
from inky_mbta_tracker.discord_webhook import helpers


def test_helpers_package_importable():
    assert hasattr(helpers, "determine_alert_routes")
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest inky-mbta-tracker/tests/test_discord_webhook_helpers.py::test_helpers_package_importable -v`

Expected: FAIL with `ModuleNotFoundError` or attribute error until `__init__.py` exists.

**Step 3: Write minimal implementation**

Create `inky-mbta-tracker/discord_webhook/__init__.py` with a minimal export (or keep empty). Ensure `discord_webhook.py` imports helpers via the package.

**Step 4: Run test to verify it passes**

Run: `uv run pytest inky-mbta-tracker/tests/test_discord_webhook_helpers.py::test_helpers_package_importable -v`

Expected: PASS

**Step 5: Commit**

```bash
git add inky-mbta-tracker/discord_webhook/__init__.py inky-mbta-tracker/discord_webhook.py inky-mbta-tracker/tests/test_discord_webhook_helpers.py
git commit -m "add discord webhook helpers package"
```

### Task 3: Update all references and run full checks

**Files:**
- Modify: `inky-mbta-tracker/discord_webhook.py`
- Modify: any files importing moved helpers

**Step 1: Write the failing test**

Add a regression test if any helper is used outside `discord_webhook.py` (only if needed). Otherwise skip new tests and proceed with verification.

**Step 2: Run test to verify it fails**

Run: `uv run pytest -k discord_webhook -v`

Expected: FAIL if any import paths are broken.

**Step 3: Write minimal implementation**

Update imports across the codebase to reference `inky-mbta-tracker/discord_webhook/helpers.py`.

**Step 4: Run test to verify it passes**

Run: `uv run pytest -k discord_webhook -v`

Expected: PASS

**Step 5: Commit**

```bash
git add inky-mbta-tracker/discord_webhook.py inky-mbta-tracker/discord_webhook/helpers.py
git commit -m "chore: update discord webhook imports"
```

### Task 4: Run project checks

**Files:**
- None

**Step 1: Run formatting**

Run: `task format`

Expected: Completed without errors.

**Step 2: Run lint + type check**

Run: `task check`

Expected: Completed without errors.

**Step 3: Run tests**

Run: `task test`

Expected: All tests pass.

**Step 4: Commit**

```bash
git add -u
git commit -m "chore: verify discord webhook refactor"
```

---

## Notes

- Keep behavior identical; no functional changes.
- Ensure `helpers.py` uses only ASCII characters unless the file already contains Unicode (it may, due to emoji constants).
- Avoid using `Exception` directly; keep existing narrow exception handling.
