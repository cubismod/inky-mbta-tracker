# Predictions-Based ETA for Vehicle Features

## Summary

Replace the naive haversine-distance/speed ETA calculation in `calculate_stop_eta` with actual MBTA predictions API data, falling back to the speed-based method when predictions are unavailable.

## Data Flow

Instead of computing ETA per-vehicle via distance/speed, add a batch prediction lookup step before the vehicle feature loop in `get_vehicle_features()`:

1. Gather unique `(trip_id, stop_id)` pairs from eligible vehicles
2. Batch-fetch MBTA predictions for those trips in parallel via `anyio` task groups
3. Build a lookup dict: `{(trip_id, stop_id) -> predicted_arrival_time}`
4. Loop over vehicles, passing `predicted_arrival` into `calculate_stop_eta()`

## Component Changes

### `calculate_stop_eta()` — modified signature

```python
def calculate_stop_eta(
    stop: Feature,
    vehicle: Feature,
    speed: float,
    predicted_arrival: datetime | None = None,
) -> str:
```

- If `predicted_arrival` is provided and is in the future: return `naturaldelta(predicted_arrival - now)`
- If `predicted_arrival` is None or in the past: fall back to haversine/speed calculation
- The existing parameter is optional — no other callers need updating

### `get_vehicle_features()` — batch fetch integration

- After the vehicle eligibility filter (not Amtrak, not STOPPED_AT, speed >= 10), collect unique `(trip_id, stop_id)` pairs
- If pairs exist, call the new batch prediction function
- Pass results into `calculate_stop_eta()` per vehicle
- If batch fetch fails entirely, all vehicles fall through to speed-based ETA

### `api/services/predictions.py` — new `batch_fetch_trip_predictions()`

Takes `list[str]` of trip IDs. For each:
- Check Redis cache first (key: `prediction:trip:{trip_id}`, TTL: 30s)
- Fetch uncached trips in parallel via `anyio.create_task_group()`, with concurrency capped by `anyio.CapacityLimiter`
- Returns `dict[tuple[str, str], datetime]` keyed by `(trip_id, stop_id)`
- Reuses the existing `aiohttp.ClientSession` and `mbta_rate_limiter` from API server DI

## Error Handling

| Scenario | Behavior |
|----------|----------|
| API timeout/HTTP error | Log warning, fall back to speed calc for failed trips |
| Empty response | Fall back to speed calc for that trip |
| Stale prediction (in past) | Fall back to speed calc |
| Redis cache failure | Fetch from API directly (no error propagation) |
| Complete batch failure | All vehicles use speed-based fallback |

## Caching

- Redis key pattern: `prediction:trip:{trip_id}`
- TTL: 30 seconds
- Cache hit: skip API call for that trip
- Cache miss/failure: fetch from API, write to cache on success

## Testing

- `test_calculate_stop_eta_with_prediction` — mock future `predicted_arrival`, verify `naturaldelta` output
- `test_calculate_stop_eta_prediction_in_past` — verify falls back to speed calc
- `test_batch_fetch_trip_predictions` — mock API responses, verify correct dict output
- `test_batch_fetch_handles_failure` — mock partial API failure, verify partial results + fallback
- `test_get_vehicle_features_integrates_predictions` — end-to-end with mocked predictions API
