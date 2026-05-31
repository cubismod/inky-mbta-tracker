# Refactoring Opportunities

## 1. Extract Alert Stream Handling

`MBTAApi.parse_live_api_response` handles alert reset, add, update, and remove events inline. That mixes stream parsing, Redis alert cache writes, alert route/trip set membership, validation, and webhook deletion with schedule and vehicle parsing.

Recommended shape:
- Move alert SSE handling into a focused helper module.
- Add helpers such as `store_alert`, `remove_alert`, and `sync_alert_memberships`.
- Keep `parse_live_api_response` responsible for dispatching by watcher type and event type.

## 2. Remove Duplicate TTL Parsing In Cache Middleware

`api/middleware/cache_middleware.py` has both `_ttl_for_request` and `_ttl_with_source`, but the middleware uses `_ttl_with_source`. The older helper duplicates most parsing logic and can be removed or delegated to `_ttl_with_source`.

## 3. Centralize Redis Connection Construction

Redis URL/client creation is repeated in multiple modules. A shared helper would reduce drift and make environment handling easier to validate.

Recommended shape:
- `redis_url_from_env()`
- `create_redis()`
- `create_redis_pool()`

## 4. Tighten Redis Cache Helper Semantics

`redis_cache.get_cache` treats falsey Redis values as misses. If an empty string is valid cached data, it should check `item is not None`. Some callers also increment Redis `get` metrics after calling `get_cache`, even though `get_cache` already records that metric.

## 5. Move Webhook Hash And Cache Entry Logic Into Helpers

`webhook/helpers.py` already has webhook hash helpers, but `webhook/discord_webhook.py` still recomputes hashes manually while posting and patching. Centralizing that behavior would reduce duplicate hash/cache-entry construction.

## 6. Split `schedule_tracker.Tracker` Responsibilities

`Tracker` handles speed estimation, Redis cleanup, MQTT formatting/sending, queue processing, and event persistence. It would be easier to test and change if split into smaller services:
- Speed estimation
- Redis persistence and cleanup
- MQTT rendering and publishing
- Queue orchestration

## 7. Refactor Route Speed Rules

`Tracker.is_speed_reasonable` is a chain of route-specific conditionals. A table-driven implementation would be clearer. The bus route condition is precedence-sensitive:

```python
line.startswith("7") or line.isnumeric() and speed <= 66
```

That means any route starting with `7` is always considered reasonable regardless of speed. If intentional, it should be made explicit with parentheses or a comment; otherwise it should be corrected.
