# Predictions-Based ETA Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace naive haversine/speed ETA with MBTA predictions API data for vehicles served through the API server.

**Architecture:** Add a batch prediction fetch step in `get_vehicle_features()` that queries `/predictions?filter[trip]={trip_id}` for all unique trips in parallel via `anyio` task groups. Pass predicted arrival times into `calculate_stop_eta()` as optional param. Fall back to speed-based calculation when predictions are unavailable. The `session` param is optional — when not provided (worker/background contexts), predictions are skipped entirely.

**Tech Stack:** Python 3.14, anyio, aiohttp, Redis (cache), MBTA V3 API, Pydantic

---

### Task 1: Add `batch_fetch_trip_predictions` to `api/services/predictions.py`

**Files:**
- Modify: `inky-mbta-tracker/api/services/predictions.py`
- Create: `inky-mbta-tracker/tests/test_predictions_service.py`

**What to build:**
- New async function `batch_fetch_trip_predictions(session, r_client, trip_ids)` → `dict[tuple[str, str], datetime]`
- Accepts a list of trip IDs, fetches predictions for each via `/predictions?filter[trip]={id}`
- Uses `anyio.create_task_group()` to fan out requests with concurrency limit of 5
- Checks Redis cache first (key: `prediction:trip:{trip_id}`, TTL: 30s) via `get_cache`/`write_cache`
- Reuses `rate_limited_get` from `mbta_rate_limiter`
- Parses responses with `Predictions.model_validate_json`, collects `arrival_time` keyed by `(trip_id, stop_id)`
- Returns empty dict on any failure rather than raising

**Tests:**
- `test_batch_fetch_returns_predicted_arrival_times` — mock API returning a prediction with `arrival_time`, verify `(trip_id, stop_id)` key and correct parsed datetime
- `test_batch_fetch_handles_api_failure` — mock 500 response, verify empty dict returned
- `test_batch_fetch_uses_cache` — mock cached response, verify no API call made

---

### Task 2: Modify `calculate_stop_eta` with optional `predicted_arrival` parameter

**Files:**
- Modify: `inky-mbta-tracker/geojson_utils.py`
- Modify: `inky-mbta-tracker/tests/test_geojson_utils.py`

**What to build:**
- Change `calculate_stop_eta(stop, vehicle, speed)` to accept `predicted_arrival: datetime | None = None`
- If `predicted_arrival` provided and in the future: return `naturaldelta(predicted_arrival - now)`
- If None or in the past: fall back to existing distance/speed calculation
- No other callers change — param is optional

**Tests (add to `test_geojson_utils.py`):**
- `test_calculate_stop_eta_uses_predicted_arrival_when_provided` — mock future arrival, verify "5 minutes"
- `test_calculate_stop_eta_falls_back_to_speed_when_no_prediction` — no param, verify existing behavior
- `test_calculate_stop_eta_falls_back_when_prediction_is_past` — past time, verify falls back to speed

---

### Task 3: Integrate predictions into `get_vehicle_features`

**Files:**
- Modify: `inky-mbta-tracker/geojson_utils.py`
- Modify: `inky-mbta-tracker/api/endpoints/vehicles.py`
- Verify: `inky-mbta-tracker/tests/test_geojson_utils.py` (existing tests should pass with no session)

**What to build:**
- Add `session: aiohttp.ClientSession | None = None` param to `get_vehicle_features`
- Add a pre-scan phase before building features:
  1. Parse all vehicles, collect `(trip_id, stop_id)` pairs for eligible ones (not Amtrak, not STOPPED_AT, speed >= 10, has stop)
  2. Extract `trip_id` from Redis key by removing `"vehicle:"` prefix
  3. If session is provided and pairs exist, call `batch_fetch_trip_predictions` (wrapped in try/except — log warning on failure)
  4. Build lookup dict → pass `predicted_arrival` into `calculate_stop_eta`
- In `api/endpoints/vehicles.py`, pass `session=commons.session` to `get_vehicle_features`

**Key details:**
- Trip ID is embedded in the Redis key: keys are `vehicle:{trip_id}`
- Existing tests pass MagicMock for redis and TaskGroup — they don't pass a session, so predictions are skipped
- `background_refresh` and `utils.py` callers don't pass session — prediction lookup is skipped, speed-based ETA only

---

### Verification

Run: `task check` (ruff + pyright) then `task test` (pytest).
Expected: All checks pass, all 100+ tests pass.
