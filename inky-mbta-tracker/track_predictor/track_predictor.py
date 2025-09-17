import json
import logging
import os
import random
import time
import uuid
from asyncio import CancelledError
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, TypedDict

import aiohttp
import anyio
import numpy as np
import shared_types.shared_types
from anyio import open_file, to_thread
from anyio.abc import TaskGroup
from consts import DAY, INSTANCE_ID, MBTA_V3_ENDPOINT, MONTH, WEEK, YEAR
from exceptions import RateLimitExceeded
from huggingface_hub import hf_hub_download
from keras import Model
from mbta_client import MBTAApi, determine_station_id
from mbta_responses import Schedules
from numpy.typing import NDArray
from prometheus import (
    mbta_api_requests,
    redis_commands,
    track_historical_assignments_stored,
    track_pattern_analysis_duration,
    track_prediction_confidence,
    track_predictions_cached,
    track_predictions_generated,
    track_predictions_ml_wins,
    track_predictions_validated,
)
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from redis_cache import check_cache, write_cache
from shared_types.shared_types import (
    MLPredictionRequest,
    TrackAssignment,
    TrackPrediction,
    TrackPredictionStats,
)

# Local ML / helper utilities extracted for testability
from .ml import (
    bayes_alpha,
    conf_gamma,
    conf_hist_weight,
    cyclical_time_features,
    margin_confidence,
    ml_compare_enabled,
    ml_compare_wait_ms,
    ml_enabled,
    ml_replace_delta,
    ml_sample_prob,
    sharpen_probs,
    vocab_get_or_add,
)

# Pattern-scoring helpers separated for easier testing
from .pattern import compute_assignment_scores, compute_final_probabilities
from .utils import (
    EXPRESS_KEYWORDS as _EXPRESS_KEYWORDS,
)
from .utils import (
    LOCAL_KEYWORDS as _LOCAL_KEYWORDS,
)
from .utils import (
    STATION_CONFIDENCE_THRESHOLDS as _STATION_CONFIDENCE_THRESHOLDS,
)


class DepartureInfo(TypedDict):
    """Typed representation of an upcoming departure returned from schedules."""

    trip_id: str
    station_id: str
    route_id: str
    direction_id: int
    departure_time: str


logger = logging.getLogger(__name__)

# Route family mappings for cross-route pattern learning
ROUTE_FAMILIES: dict[str, list[str]] = {
    "CR-Worcester": ["CR-Framingham"],
    "CR-Framingham": ["CR-Worcester"],
    "CR-Franklin": ["CR-Foxboro", "CR-Franklin"],
    "CR-Foxboro": ["CR-Franklin", "CR-Foxboro"],
    "CR-Providence": ["CR-Providence", "CR-Stoughton"],
    "CR-Stoughton": ["CR-Providence", "CR-Stoughton"],
    "CR-Needham": ["CR-Needham"],
    "CR-Fairmount": ["CR-Fairmount"],
    "CR-Fitchburg": ["CR-Fitchburg"],
    "CR-Lowell": ["CR-Lowell"],
    "CR-Haverhill": ["CR-Haverhill"],
    "CR-Newburyport": ["CR-Newburyport"],
    "CR-Rockport": ["CR-Rockport"],
    "CR-Kingston": ["CR-Kingston", "CR-Plymouth", "CR-Greenbush"],
    "CR-Plymouth": ["CR-Kingston", "CR-Plymouth", "CR-Greenbush"],
    "CR-Greenbush": ["CR-Kingston", "CR-Plymouth", "CR-Greenbush"],
}


EXPRESS_KEYWORDS: list[str] = list(_EXPRESS_KEYWORDS)
LOCAL_KEYWORDS: list[str] = list(_LOCAL_KEYWORDS)
STATION_CONFIDENCE_THRESHOLDS: dict[str, float] = dict(_STATION_CONFIDENCE_THRESHOLDS)


class TrackPredictor:
    """
    Track prediction system for MBTA commuter rail trains.

    This system analyzes historical track assignments to predict future track
    assignments before they are officially announced by the MBTA.
    """

    def __init__(self, r_client: Redis) -> None:
        self.redis = r_client
        self._child_stations_map: Dict[str, str] = {}
        self._supported_stations: Set[str] = set()

    async def _load_child_stations_map(self) -> Dict[str, str]:
        """Load the child stations JSON file and create a mapping from child -> parent."""
        try:
            child_stations_path = Path("child_stations.json")
            async with await open_file(child_stations_path) as f:
                content = await f.read()
                data = json.loads(content)

            # Create reverse mapping: child_id -> parent_station_id
            mapping = {}
            for parent_station, children in data.items():
                for child in children:
                    mapping[child["id"]] = parent_station
            return mapping
        except (json.JSONDecodeError, FileNotFoundError, PermissionError) as e:
            logger.warning("Failed to load child stations mapping", exc_info=e)
            return {}

    async def initialize(self) -> None:
        """Initialize the TrackPredictor by loading child stations mapping."""
        self._child_stations_map = await self._load_child_stations_map()
        self._supported_stations = (
            set(self._child_stations_map.values())
            if self._child_stations_map
            else set()
        )

    def normalize_station(self, station_id: str) -> str:
        """
        Normalize station/stop identifiers to canonical 'place-...' station ids
        using the child_stations.json lookup table as the source of truth.
        """
        if not station_id:
            return station_id

        # First check if this ID is directly in our child stations mapping
        if station_id in self._child_stations_map:
            return self._child_stations_map[station_id]

        # If already a place-* ID, return as-is
        if station_id.startswith("place-"):
            return station_id

        try:
            resolved, has = determine_station_id(station_id)
            if resolved and has:
                return resolved
        except (TypeError, ValueError):
            # determine_station_id is simple but protect against unexpected input types
            pass

        return station_id

    def supports_track_predictions(self, station_id: str) -> bool:
        """
        Check if a station supports track predictions based on our child_stations.json mapping.
        Returns True if the normalized station ID is in our supported stations list.
        """
        normalized_station = self.normalize_station(station_id)
        return normalized_station in self._supported_stations

    async def store_historical_assignment(
        self, assignment: TrackAssignment, tg: TaskGroup
    ) -> None:
        """
        Store a historical track assignment for future analysis.

        Args:
            assignment: The track assignment data to store
        """
        try:
            # Store in Redis with a key that includes station, route, and timestamp
            key = f"track_history:{self.normalize_station(assignment.station_id)}:{assignment.route_id}:{assignment.trip_id}:{assignment.scheduled_time.date()}"

            # check if the key already exists
            if await check_cache(self.redis, key):
                return

            # Store for 1 yr for later ML-analysis/pattern matching
            tg.start_soon(
                write_cache,
                self.redis,
                key,
                assignment.model_dump_json(),
                1 * YEAR,
            )

            # Also store in a time-series format for easier querying
            time_series_key = f"track_timeseries:{self.normalize_station(assignment.station_id)}:{assignment.route_id}"
            tg.start_soon(
                self.redis.zadd,
                time_series_key,
                {key: assignment.scheduled_time.timestamp()},
            )
            redis_commands.labels("zadd").inc()

            # Set expiration for time series (1 year)
            tg.start_soon(self.redis.expire, time_series_key, 1 * YEAR)
            redis_commands.labels("expire").inc()

            track_historical_assignments_stored.labels(
                station_id=self.normalize_station(assignment.station_id),
                route_id=assignment.route_id,
                track_number=assignment.track_number or "unknown",
                instance=INSTANCE_ID,
            ).inc()

            logger.debug(
                f"Stored track assignment: {assignment.station_id} {assignment.route_id} -> {assignment.track_number}"
            )

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                "Failed to store track assignment due to Redis connection issue",
                exc_info=e,
            )
        except ValidationError as e:
            logger.error(
                "Failed to store track assignment due to validation error",
                exc_info=e,
            )

    # ------------------------------
    # ML Queue + Worker (Ensemble)
    # ------------------------------

    # Redis keys for ML prediction queueing
    ML_QUEUE_KEY = "ml:track_predict:requests"
    ML_RESULT_KEY_PREFIX = "ml:track_predict:result:"
    ML_RESULT_LATEST_PREFIX = "ml:track_predict:latest:"
    ML_STATION_VOCAB_KEY = "ml:vocab:station"
    ML_ROUTE_VOCAB_KEY = "ml:vocab:route"

    @staticmethod
    def _conf_gamma() -> float:
        """Read sharpening gamma from env; delegated to ml helper."""
        return conf_gamma()

    @staticmethod
    def _conf_hist_weight() -> float:
        """Weight for blending historical accuracy into confidence (0..1)."""
        return conf_hist_weight()

    @staticmethod
    def _bayes_alpha() -> float:
        """Blend weight for pattern vs ML in Bayes fusion (0..1)."""
        return bayes_alpha()

    @staticmethod
    def _ml_sample_prob() -> float:
        """Fraction of successful traditional predictions to enqueue for ML as exploration."""
        return ml_sample_prob()

    @staticmethod
    def _ml_replace_delta() -> float:
        """Minimum improvement required for ML to overwrite an existing prediction."""
        return ml_replace_delta()

    @staticmethod
    def _ml_compare_enabled() -> bool:
        """
        If enabled, compare the ML ensemble prediction against the traditional
        pattern-based prediction at runtime and choose the result with higher
        final confidence. Controlled by env IMT_ML_COMPARE.
        """
        return ml_compare_enabled()

    @staticmethod
    def _ml_compare_wait_ms() -> int:
        """
        Maximum time in milliseconds to wait for a recent ML result when doing a
        live ML-vs-pattern comparison. If ML result isn't available within this
        window, the code will enqueue a request and proceed using the historical
        result. Controlled by env IMT_ML_COMPARE_WAIT_MS (default 200).
        """
        return ml_compare_wait_ms()

    @staticmethod
    def _sharpen_probs(
        vec: NDArray[np.floating[Any]],
        gamma: float,
    ) -> Optional[NDArray[np.floating[Any]]]:
        """Sharpen a probability vector by exponentiation and renormalization (delegated).

        The wrapper accepts an Optional array to satisfy callers that may pass None;
        if None is provided this returns None rather than delegating to the helper.
        """

        return sharpen_probs(vec, gamma)

    @staticmethod
    def _margin_confidence(
        vec: NDArray[np.floating[Any]],
        index: int,
    ) -> float:
        """Compute margin-based confidence (delegated).

        Accept an Optional input for compatibility with callers that may pass None.
        When vec is None, return a conservative confidence of 0.0.
        """
        return margin_confidence(vec, index)

    async def _get_allowed_tracks(self, station_id: str, route_id: str) -> set[str]:
        """Return a set of allowed track numbers for a station/route based on recent history.

        Cached in Redis for 1 day to avoid heavy scans. Falls back to empty set
        meaning "no restriction" if nothing found.
        """
        cache_key = f"allowed_tracks:{self.normalize_station(station_id)}:{route_id}"
        cached = await check_cache(self.redis, cache_key)
        if cached:
            try:
                lst = json.loads(cached)
                return set(str(x) for x in lst)
            except (json.JSONDecodeError, TypeError):
                # cached value couldn't be parsed or isn't iterable as expected
                pass

        # Build from historical assignments over the last 180 days
        try:
            end = datetime.now(UTC)
            start = end - timedelta(days=180)
            assignments = await self.get_historical_assignments(
                station_id, route_id, start, end
            )
            tracks = {a.track_number for a in assignments if a.track_number}
            await write_cache(self.redis, cache_key, json.dumps(sorted(tracks)), DAY)
            return set(tracks)
        except (ConnectionError, TimeoutError, ValueError) as e:
            # If we can't fetch history due to connectivity or parsing errors,
            # return an empty set (permissive fallback).
            logger.debug("Unable to build allowed tracks from history", exc_info=e)
            return set()

    @staticmethod
    def _ml_enabled() -> bool:
        return ml_enabled()

    async def enqueue_ml_prediction(
        self,
        *,
        station_id: str,
        route_id: str,
        direction_id: int,
        scheduled_time: datetime,
        trip_id: Optional[str] = None,
        headsign: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> str:
        """
        Enqueue a request for ML-powered track prediction. Returns the request_id.

        Other modules can call this to request a prediction; the ML worker
        will write the result to Redis when ready for asynchronous consumers.
        """
        rid = request_id or str(uuid.uuid4())
        payload = MLPredictionRequest(
            id=rid,
            station_id=station_id,
            route_id=route_id,
            direction_id=int(direction_id),
            scheduled_time=scheduled_time,
            trip_id=trip_id or "",
            headsign=headsign or "",
        )
        await self.redis.lpush(self.ML_QUEUE_KEY, payload.model_dump_json())  # pyright:ignore
        return rid

    @staticmethod
    def _cyclical_time_features(ts: datetime) -> dict[str, float]:
        """Compute cyclical encodings for hour/minute/day_of_week and raw timestamp (delegated)."""
        return cyclical_time_features(ts)

    async def _vocab_get_or_add(self, key: str, token: str) -> int:
        """Get a stable integer index for a token from a Redis hash, adding if missing.

        This maintains consistent integer ids for categorical features like station_id
        and route_id across ML predictions.
        """
        # Delegate to shared ml helper which handles redis access
        return await vocab_get_or_add(self.redis, key, token)

    async def ml_prediction_worker(self) -> None:
        """
        Background worker that consumes prediction requests from Redis and
        generates predictions using the Keras ensemble model.

        Runs blocking model load/inference in an anyio thread.
        """
        if not self._ml_enabled():
            logger.info("ML worker not started: IMT_ML not enabled")
            return

        def load_models_blocking() -> List[Model]:
            # Set backend before importing keras
            import keras

            models: List[Model] = list()

            filenames = [
                f"track_prediction_ensemble_model_{i}_best.keras" for i in range(5)
            ]
            model_paths = [
                hf_hub_download("cubis/mbta-track-predictor", filename)
                for filename in filenames
            ]

            try:
                # Load without compiling to avoid requiring custom loss/metrics
                models = [
                    keras.saving.load_model(model_path, compile=False)
                    for model_path in model_paths
                ]  # pyright: ignore
                # model = keras.saving.load_model("hf://cubis/mbta-track-predictor")
            except (OSError, ImportError, RuntimeError) as e:  # noqa: BLE001 (surface errors to logs)
                # Narrow to errors that can happen while downloading/loading model artifacts
                logger.error("Failed to load ML model", exc_info=e)
                raise
            return models

        # Load model once in a worker thread
        logger.info("Starting ML prediction worker: loading model…")
        try:
            models = await to_thread.run_sync(load_models_blocking)
        except (OSError, RuntimeError) as e:
            # Loading model failed for an expected runtime/IO reason — disable worker
            logger.error("ML worker disabled due to model load failure.", exc_info=e)
            return
        logger.info("ML model loaded. Worker ready.")

        # Consume requests
        while True:
            try:
                item = await self.redis.brpop([self.ML_QUEUE_KEY], timeout=5)  # pyright:ignore
                redis_commands.labels("brpop").inc()
                if not item:
                    continue

                _, raw = item
                payload = MLPredictionRequest.model_validate_json(raw)

                req_id = payload.id
                station_id = payload.station_id
                route_id = payload.route_id
                direction_id = payload.direction_id
                scheduled_time = payload.scheduled_time
                trip_id = payload.trip_id
                headsign = payload.headsign

                # Vocab lookups
                station_idx = await self._vocab_get_or_add(
                    self.ML_STATION_VOCAB_KEY, station_id
                )
                route_idx = await self._vocab_get_or_add(
                    self.ML_ROUTE_VOCAB_KEY, route_id
                )

                feats = self._cyclical_time_features(scheduled_time)

                # Prepare tensors
                features = {
                    "station_id": np.array([station_idx], dtype=np.int64),
                    "route_id": np.array([route_idx], dtype=np.int64),
                    "direction_id": np.array([direction_id], dtype=np.int64),
                    "hour_sin": np.array([feats["hour_sin"]], dtype=np.float32),
                    "hour_cos": np.array([feats["hour_cos"]], dtype=np.float32),
                    "minute_sin": np.array([feats["minute_sin"]], dtype=np.float32),
                    "minute_cos": np.array([feats["minute_cos"]], dtype=np.float32),
                    "day_sin": np.array([feats["day_sin"]], dtype=np.float32),
                    "day_cos": np.array([feats["day_cos"]], dtype=np.float32),
                    "scheduled_timestamp": np.array(
                        [feats["scheduled_timestamp"]], dtype=np.float32
                    ),
                }

                def predict_blocking() -> Optional[List[Any]]:  # type: ignore[no-untyped-def]
                    # Import inside the thread to ensure backend is respected
                    if models:
                        probs = [model.predict(features) for model in models]
                        return probs

                probs = await to_thread.run_sync(predict_blocking)
                # Each model returns class probabilities shaped like (1, num_classes).
                # Take per-model argmax, then majority vote across models,
                # and compute mean probabilities for confidence reporting.
                model_top_classes: list[int] = []
                flat_probs: list[np.ndarray] = []
                for p in probs or []:
                    arr = np.array(p)
                    try:
                        if arr.ndim == 2 and arr.shape[0] == 1:
                            flat = arr[0]
                        elif arr.ndim == 1:
                            flat = arr
                        else:
                            flat = arr.reshape(-1)
                        idx = int(np.argmax(flat))
                        model_top_classes.append(idx)
                        flat_probs.append(flat.astype(np.float32))
                    except (ValueError, IndexError, TypeError):
                        # Skip malformed model output
                        continue

                if model_top_classes:
                    # Majority vote; ties resolve to smallest index via argmax
                    predicted_track = int(np.bincount(model_top_classes).argmax())
                    # Mean probabilities across models for confidence
                    mean_probs = (
                        np.mean(np.stack(flat_probs, axis=0), axis=0)
                        if flat_probs
                        else None
                    )
                    # Apply allowed-tracks mask from history, and reselect from masked distribution
                    if mean_probs is not None:
                        allowed = await self._get_allowed_tracks(station_id, route_id)
                        if allowed:
                            masked = mean_probs.copy()
                            for idx in range(masked.shape[0]):
                                if str(idx + 1) not in allowed:
                                    masked[idx] = 0.0
                            total = float(np.sum(masked))
                            # If masking removed all probability mass, keep original mean_probs
                            if total > 0:
                                mean_probs = masked / total
                                predicted_track = int(np.argmax(mean_probs))
                            else:
                                # Try to pick top allowed track from history as fallback
                                best_allowed = await self._choose_top_allowed_track(
                                    station_id, route_id, allowed
                                )
                                if best_allowed:
                                    logger.info(
                                        f"Allowed mask removed ML probs for {station_id} {route_id}; falling back to most frequent allowed track {best_allowed}"
                                    )
                                    predicted_track = int(best_allowed) - 1
                                    track_number = best_allowed
                                else:
                                    logger.debug(
                                        f"Allowed-tracks mask removed all probabilities for {station_id} {route_id}; keeping original mean_probs"
                                    )
                    # Compute model confidence (pre-sharpen, pre-history)
                    model_confidence = self._margin_confidence(
                        mean_probs, predicted_track
                    )
                    # Sharpened margin-based display confidence around the selected class
                    probs_for_conf = self._sharpen_probs(mean_probs, self._conf_gamma())
                    confidence = self._margin_confidence(
                        probs_for_conf, predicted_track
                    )
                    track_number = str(predicted_track + 1)  # 1-based for MBTA tracks
                    # Drop very low-confidence ML predictions
                    if confidence < 0.25:
                        logger.debug(
                            f"Dropping low-confidence ML prediction for {station_id} {route_id} at {scheduled_time}: track={track_number}, confidence={confidence:.3f}"
                        )
                        continue
                    # Historical accuracy for predicted track number
                    hist_acc = await self._get_prediction_accuracy(
                        station_id, route_id, track_number
                    )
                    # Blend confidence with historical accuracy
                    w_hist = self._conf_hist_weight()
                    confidence = (1.0 - w_hist) * confidence + w_hist * hist_acc

                    result = {
                        "id": req_id,
                        "station_id": self.normalize_station(station_id),
                        "route_id": route_id,
                        "direction_id": direction_id,
                        "scheduled_time": scheduled_time.isoformat(),
                        "predicted_track_index": predicted_track,
                        "track_number": track_number,
                        "confidence": confidence,
                        "model_confidence": model_confidence,
                        "accuracy": hist_acc,
                        "probabilities": mean_probs.tolist()
                        if mean_probs is not None
                        else None,
                        "model": "hf://cubis/mbta-track-predictor",
                        "backend": os.environ.get("KERAS_BACKEND", "jax"),
                        "created_at": datetime.now(UTC).isoformat(),
                        "station_vocab_index": station_idx,
                        "route_vocab_index": route_idx,
                    }

                    result_key = f"{self.ML_RESULT_KEY_PREFIX}{req_id}"
                    await self.redis.set(result_key, json.dumps(result), ex=DAY)
                    latest_key = f"{self.ML_RESULT_LATEST_PREFIX}{self.normalize_station(station_id)}:{route_id}:{direction_id}:{int(scheduled_time.timestamp())}"
                    await self.redis.set(latest_key, json.dumps(result), ex=DAY)

                    # Persist a TrackPrediction entry conditionally replacing a weak traditional result
                    cache_key = f"track_prediction:{self.normalize_station(station_id)}:{route_id}:{trip_id}:{scheduled_time.date()}"
                    do_write = False
                    existing_json = await check_cache(self.redis, cache_key)
                    if existing_json:
                        try:
                            existing = TrackPrediction.model_validate_json(
                                existing_json
                            )
                            delta = self._ml_replace_delta()
                            if existing.prediction_method != "ml_ensemble":
                                if confidence > (existing.confidence_score + delta):
                                    do_write = True
                            else:
                                if confidence > existing.confidence_score:
                                    do_write = True
                        except (ValidationError, ValueError, TypeError):
                            # If parsing the existing cache fails, decide conservatively by confidence
                            do_write = confidence >= 0.5
                    else:
                        do_write = confidence >= 0.5

                    if do_write:
                        prediction = TrackPrediction(
                            station_id=self.normalize_station(station_id),
                            route_id=route_id,
                            trip_id=trip_id,
                            headsign=headsign,
                            direction_id=direction_id,
                            scheduled_time=scheduled_time,
                            track_number=track_number,
                            confidence_score=confidence,
                            accuracy=hist_acc,
                            model_confidence=model_confidence,
                            display_confidence=confidence,
                            prediction_method="ml_ensemble",
                            historical_matches=0,
                            created_at=datetime.now(UTC),
                        )

                        negative_cache_key = f"negative_{cache_key}"
                        try:
                            await self.redis.delete(negative_cache_key)  # pyright: ignore
                        except (ConnectionError, TimeoutError, RedisError, OSError):
                            # best-effort negative cache cleanup; ignore transient failures
                            pass
                        await write_cache(
                            self.redis,
                            cache_key,
                            prediction.model_dump_json(),
                            2 * MONTH,
                        )

                        track_predictions_generated.labels(
                            station_id=self.normalize_station(station_id),
                            route_id=route_id,
                            prediction_method="ml_ensemble",
                            instance=INSTANCE_ID,
                        ).inc()
                        track_prediction_confidence.labels(
                            station_id=self.normalize_station(station_id),
                            route_id=route_id,
                            track_number=track_number,
                            instance=INSTANCE_ID,
                        ).set(confidence)
                    # redis_commands.labels("set").inc()

            except ValidationError as e:
                logger.error("Unable to validate model", exc_info=e)
            except (
                RuntimeError,
                OSError,
            ) as e:  # keep worker alive for recoverable runtime errors
                logger.error("Error in ML prediction worker loop", exc_info=e)
                await anyio.sleep(0.5)

    # tenacity decorator removed - using local retry/backoff inside function
    async def fetch_upcoming_departures(
        self,
        session: Any,
        route_id: str,
        station_ids: List[str],
        target_date: Optional[datetime] = None,
    ) -> List[DepartureInfo]:
        """
        Fetch upcoming departures for specific stations on a commuter rail route.

        Args:
            session: HTTP session for API calls
            route_id: Commuter rail route ID (e.g., "CR-Worcester")
            station_ids: List of station IDs to fetch departures for
            target_date: Date to fetch departures for (defaults to today)

        Returns:
            List of departure data dictionaries with scheduled times
        """
        # Local retry/backoff implementation (replaces tenacity decorator).
        # This will attempt the schedule fetch a few times with exponential
        # backoff + jitter before giving up, while still propagating CancelledError.
        max_attempts = int(os.getenv("IMT_PRECACHE_MAX_ATTEMPTS", "5"))
        base_backoff = float(os.getenv("IMT_PRECACHE_BASE_BACKOFF", "1.0"))
        attempt = 0

        while True:
            try:
                if target_date is None:
                    target_date = datetime.now(UTC)
                # Proceed with the normal fetch logic below on success
                break
            except CancelledError:
                # Preserve cancellation semantics
                raise
            except Exception as e:
                attempt += 1
                if attempt >= max_attempts:
                    logger.error(
                        f"Exceeded max attempts ({max_attempts}) in fetch_upcoming_departures for route {route_id}",
                        exc_info=e,
                    )
                    return []
                # Exponential backoff with small random jitter
                backoff = min(60.0, base_backoff * (2 ** (attempt - 1)))
                jitter = random.random() * 0.5
                sleep_for = backoff + jitter
                logger.debug(
                    f"fetch_upcoming_departures attempt {attempt}/{max_attempts} failed for route {route_id}; retrying in {sleep_for:.2f}s",
                    exc_info=e,
                )
                await anyio.sleep(sleep_for)
                # loop and retry

        # Local imports to avoid top-level import diagnostics and heavy deps at module import time

        date_str = target_date.date().isoformat()
        auth_token = os.environ.get("AUTH_TOKEN", "")
        upcoming_departures: list[DepartureInfo] = []

        # Fetch schedules for all stations in a single API call
        stations_str = ",".join(station_ids)

        endpoint = f"{MBTA_V3_ENDPOINT}/schedules?filter[stop]={stations_str}&filter[route]={route_id}&filter[date]={date_str}&sort=departure_time&include=trip&api_key={auth_token}"

        try:
            async with session.get(endpoint) as response:
                if response.status == 429:
                    raise RateLimitExceeded()

                if response.status != 200:
                    logger.error(
                        f"Failed to fetch schedules for {stations_str} on {route_id}: HTTP {response.status}"
                    )
                    return []

                body = await response.text()
                mbta_api_requests.labels("schedules").inc()

                try:
                    schedules_data = Schedules.model_validate_json(body, strict=False)
                except ValidationError as e:
                    logger.error(
                        f"Unable to parse schedules for {stations_str} on {route_id}",
                        exc_info=e,
                    )
                    return []

                # Process each scheduled departure
                for schedule in schedules_data.data:
                    if not schedule.attributes.departure_time:
                        continue

                    # Get trip information from relationships
                    trip_id = ""
                    if (
                        hasattr(schedule, "relationships")
                        and hasattr(schedule.relationships, "trip")
                        and schedule.relationships.trip.data
                    ):
                        trip_id = schedule.relationships.trip.data.id

                    # Get station ID from relationships
                    station_id = ""
                    if (
                        hasattr(schedule, "relationships")
                        and hasattr(schedule.relationships, "stop")
                        and schedule.relationships.stop.data
                    ):
                        station_id = schedule.relationships.stop.data.id

                    departure_info: DepartureInfo = {
                        "trip_id": trip_id,
                        "station_id": station_id,
                        "route_id": route_id,
                        "direction_id": schedule.attributes.direction_id,
                        "departure_time": schedule.attributes.departure_time,
                    }

                    upcoming_departures.append(departure_info)

        except (aiohttp.ClientError, TimeoutError) as e:
            logger.error(
                f"Error fetching schedules for {stations_str} on {route_id}",
                exc_info=e,
            )
            return []

        logger.debug(
            f"Found {len(upcoming_departures)} upcoming departures for route {route_id} across {len(station_ids)} stations"
        )
        return upcoming_departures

    async def precache(
        self,
        tg: TaskGroup,
        routes: Optional[List[str]] = None,
        target_stations: Optional[List[str]] = None,
    ) -> int:
        """
        Precache track predictions for upcoming departures.

        Args:
            tg: TaskGroup used to schedule background prediction work
            routes: List of route IDs to precache (defaults to all CR routes)
            target_stations: List of station IDs to precache for (defaults to supported stations)

        Returns:
            Number of predictions cached
        """
        # Local import for RateLimitExceeded used in this function's error handling

        if routes is None:
            routes = [
                "CR-Worcester",
                "CR-Framingham",
                "CR-Franklin",
                "CR-Foxboro",
                "CR-Providence",
                "CR-Stoughton",
                "CR-Needham",
                "CR-Fairmount",
                "CR-Fitchburg",
                "CR-Lowell",
                "CR-Haverhill",
                "CR-Newburyport",
                "CR-Rockport",
                "CR-Kingston",
                "CR-Plymouth",
                "CR-Greenbush",
            ]

        if target_stations is None:
            target_stations = [
                "place-sstat",  # South Station
                "place-north",  # North Station
                "place-bbsta",  # Back Bay
                "place-rugg",  # Ruggles
            ]

        predictions_cached = 0
        current_time = datetime.now(UTC)

        logger.info(
            f"Starting precache for {len(routes)} routes and {len(target_stations)} stations"
        )

        try:
            async with aiohttp.ClientSession() as session:

                async def process_route(route_id: str) -> int:
                    """Process a single route and return number of predictions cached."""
                    route_predictions_cached = 0
                    logger.debug(f"Precaching predictions for route {route_id}")

                    try:
                        # Fetch upcoming departures with actual scheduled times
                        upcoming_departures = await self.fetch_upcoming_departures(
                            session, route_id, target_stations, current_time
                        )

                        async def process_departure(
                            departure_data: DepartureInfo,
                        ) -> bool:
                            """Process a single departure and return True if cached."""
                            try:
                                trip_id = departure_data["trip_id"]
                                station_id = departure_data["station_id"]
                                direction_id = departure_data["direction_id"]
                                scheduled_time = datetime.fromisoformat(
                                    departure_data["departure_time"]
                                )

                                if not trip_id:
                                    return False  # Skip if no trip ID available

                                # Normalize station id so Redis keys use canonical station identifiers
                                norm_station_id = self.normalize_station(station_id)

                                # Check if we already have a cached prediction (use normalized station id)
                                cache_key = f"track_prediction:{norm_station_id}:{route_id}:{trip_id}:{scheduled_time.date()}"
                                if await check_cache(self.redis, cache_key):
                                    return False  # Already cached

                                # Generate prediction using actual scheduled departure time (pass normalized station id)
                                prediction = await self.predict_track(
                                    station_id=norm_station_id,
                                    route_id=route_id,
                                    trip_id=trip_id,
                                    headsign="",  # Will be fetched in predict_track method
                                    direction_id=direction_id,
                                    scheduled_time=scheduled_time,
                                    tg=tg,
                                )

                                if prediction:
                                    logger.debug(
                                        f"Precached prediction: {station_id} {route_id} {trip_id} @ {scheduled_time.strftime('%H:%M')} -> Track {prediction.track_number}"
                                    )
                                    return True

                                return False

                            except (
                                ConnectionError,
                                TimeoutError,
                                ValidationError,
                                RateLimitExceeded,
                            ) as e:
                                logger.error(
                                    f"Error precaching prediction for {departure_data.get('station_id')} {route_id} {departure_data.get('trip_id')}",
                                    exc_info=e,
                                )
                                return False

                        # Process departures sequentially with explicit error handling
                        for dep in upcoming_departures:
                            try:
                                if await process_departure(dep):
                                    route_predictions_cached += 1
                            except (
                                ConnectionError,
                                TimeoutError,
                                ValidationError,
                            ) as e:
                                logger.error(
                                    f"Unexpected error in departure processing for {route_id}",
                                    exc_info=e,
                                )

                    except (
                        ConnectionError,
                        TimeoutError,
                        ValidationError,
                        RateLimitExceeded,
                    ) as e:
                        logger.error(f"Error processing route {route_id}", exc_info=e)

                    return route_predictions_cached

                # Run route processing concurrently to improve precache throughput
                route_results: list[int] = []

                async def _run_route(rid: str) -> None:
                    try:
                        res = await process_route(rid)
                        route_results.append(res)
                    except Exception as e:
                        logger.error(f"Error running route {rid}", exc_info=e)

                async with anyio.create_task_group() as tg2:
                    for rid in routes:
                        tg2.start_soon(_run_route, rid)

                # Sum up all predictions cached
                for result in route_results:
                    if isinstance(result, int):
                        predictions_cached += result
                    else:
                        logger.error(
                            "Unexpected non-int result in route processing",
                            exc_info=result,
                        )

        except (ConnectionError, TimeoutError, aiohttp.ClientError) as e:
            logger.error("Error in precache_track_predictions", exc_info=e)

        logger.info(f"Precached {predictions_cached} track predictions")
        return predictions_cached

    async def get_historical_assignments(
        self,
        station_id: str,
        route_id: str,
        start_date: datetime,
        end_date: datetime,
        include_related_routes: bool = False,
    ) -> List[TrackAssignment]:
        """
        Retrieve historical track assignments for a given station and route.

        Args:
            station_id: The station to query
            route_id: The route to query
            start_date: Start date for the query
            end_date: End date for the query
            include_related_routes: Whether to include assignments from related routes

        Returns:
            List of historical track assignments
        """
        try:
            routes_to_check = [route_id]
            if include_related_routes and route_id in ROUTE_FAMILIES:
                routes_to_check.extend(ROUTE_FAMILIES[route_id])

            async def fetch_route_assignments(
                current_route: str,
            ) -> List[TrackAssignment]:
                """Fetch assignments for a single route."""
                route_results: list[TrackAssignment] = []
                time_series_key = f"track_timeseries:{self.normalize_station(station_id)}:{current_route}"

                # Get assignments within the time range
                assignments = await self.redis.zrangebyscore(
                    time_series_key, start_date.timestamp(), end_date.timestamp()
                )
                redis_commands.labels("zrangebyscore").inc()

                if not assignments:
                    return route_results

                # Fetch all assignment data
                async def fetch_assignment_data(
                    assignment_key: bytes,
                ) -> Optional[TrackAssignment]:
                    try:
                        assignment_data = await check_cache(
                            self.redis, assignment_key.decode()
                        )
                        if assignment_data:
                            return TrackAssignment.model_validate_json(assignment_data)
                    except ValidationError as e:
                        logger.error("Failed to parse assignment data", exc_info=e)
                    return None

                for assignment_key in assignments:
                    try:
                        result = await fetch_assignment_data(assignment_key)
                        if isinstance(result, TrackAssignment):
                            route_results.append(result)
                    except (ConnectionError, TimeoutError) as e:
                        logger.error("Error fetching assignment data", exc_info=e)

                return route_results

            # Fetch assignments for all routes concurrently
            results: list[TrackAssignment] = []

            for route in routes_to_check:
                try:
                    route_results = await fetch_route_assignments(route)
                    results.extend(route_results)
                except (ConnectionError, TimeoutError) as e:
                    logger.error("Error fetching route assignments", exc_info=e)

            return results

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                "Failed to retrieve historical assignments due to Redis connection issue",
                exc_info=e,
            )
            return []
        except ValidationError as e:
            logger.error(
                "Failed to retrieve historical assignments due to validation error",
                exc_info=e,
            )
            return []

    def _enhanced_headsign_similarity(self, headsign1: str, headsign2: str) -> float:
        """
        Delegates enhanced headsign similarity computation to the utils module.

        Kept as an instance method for call-site compatibility but delegates the
        actual algorithm to a pure helper to make unit testing straightforward.
        """

        from .utils import enhanced_headsign_similarity as _ehs

        return _ehs(headsign1, headsign2)

    def _detect_service_type(self, headsign: str) -> str:
        """
        Delegate detection of service type to utils.


        """
        from .utils import detect_service_type as _detect

        return _detect(headsign)

    def _is_weekend_service(self, day_of_week: int) -> bool:
        """
        Delegate weekend detection to utils.

        Keeps logic centralized and testable.
        """
        from .utils import is_weekend_service as _is_wkend

        return _is_wkend(day_of_week)

    def _get_station_confidence_threshold(self, station_id: str) -> float:
        """
        Delegate station-specific threshold lookup to utils.

        This allows thresholds to be tested independently of TrackPredictor.
        """
        from .utils import get_station_confidence_threshold as _thr

        return _thr(station_id)

    async def analyze_patterns(
        self,
        station_id: str,
        route_id: str,
        trip_headsign: str,
        direction_id: int,
        scheduled_time: datetime,
    ) -> Dict[str, float]:
        """
        Analyze historical patterns to determine track assignment probabilities.

        This implementation delegates core scoring to the `pattern` helpers which
        are pure and unit-testable. It then uses an injected async accuracy lookup
        to convert the scored results into a normalized probability distribution.
        """
        start_time = time.time()

        try:
            # Fetch historical assignments (may include related routes)
            start_date = scheduled_time - timedelta(days=180)
            end_date = scheduled_time
            assignments = await self.get_historical_assignments(
                station_id, route_id, start_date, end_date, include_related_routes=True
            )

            if not assignments:
                logger.debug(
                    f"No historical assignments found for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}"
                )
                duration = time.time() - start_time
                track_pattern_analysis_duration.labels(
                    station_id=station_id, route_id=route_id, instance=INSTANCE_ID
                ).set(duration)
                return {}

            # Compute combined scores and sample counts using pure helper
            combined_scores, sample_counts, _patterns = compute_assignment_scores(
                assignments, trip_headsign, direction_id, scheduled_time
            )

            # Accuracy lookup injected to compute final probabilities
            async def _accuracy_lookup(track: str) -> float:
                return await self._get_prediction_accuracy(station_id, route_id, track)

            # Convert scores -> normalized distribution accounting for sample-size and accuracy
            total = await compute_final_probabilities(
                combined_scores, sample_counts, _accuracy_lookup
            )

            # Record analysis duration metric
            duration = time.time() - start_time
            track_pattern_analysis_duration.labels(
                station_id=station_id, route_id=route_id, instance=INSTANCE_ID
            ).set(duration)

            if total:
                logger.debug(
                    f"Calculated pattern for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}"
                )
                logger.debug(f"Total score distribution={total}")

            return total

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                "Failed to analyze patterns due to Redis connection issue",
                exc_info=e,
            )
            return {}
        except ValidationError as e:
            logger.error(
                "Failed to analyze patterns due to validation error",
                exc_info=e,
            )
            return {}

    async def predict_track(
        self,
        station_id: str,
        route_id: str,
        trip_id: str,
        headsign: str,
        direction_id: int,
        scheduled_time: datetime,
        tg: TaskGroup,
    ) -> Optional[TrackPrediction]:
        """
        Generate a track prediction for a given trip.

        Args:
            station_id: The station ID
            route_id: The route ID
            trip_id: The trip ID
            headsign: The trip headsign
            direction_id: The direction ID
            scheduled_time: The scheduled departure time

        Returns:
            TrackPrediction object or None if no prediction can be made
        """
        # Added rich debug logging throughout this method to help diagnose
        # missing predictions (e.g., for North Station / place-north).
        try:
            logger.debug(
                "predict_track called",
                extra={
                    "station_id": station_id,
                    "route_id": route_id,
                    "trip_id": trip_id,
                    "headsign": headsign,
                    "direction_id": direction_id,
                    "scheduled_time": scheduled_time.isoformat(),
                    "ml_enabled": self._ml_enabled(),
                    "ml_compare": self._ml_compare_enabled(),
                },
            )
            # Only predict for commuter rail
            if not route_id.startswith("CR"):
                logger.debug(
                    "Route not commuter rail; skipping prediction",
                    extra={"route_id": route_id},
                )
                return None

            cache_key = f"track_prediction:{self.normalize_station(station_id)}:{route_id}:{trip_id}:{scheduled_time.date()}"
            negative_cache_key = f"negative_{cache_key}"
            logger.debug(
                "Checking caches",
                extra={
                    "cache_key": cache_key,
                    "negative_cache_key": negative_cache_key,
                },
            )
            cached_prediction = await check_cache(self.redis, cache_key)
            if cached_prediction:
                track_predictions_cached.labels(
                    station_id=self.normalize_station(station_id),
                    route_id=route_id,
                    instance=INSTANCE_ID,
                ).inc()
                logger.debug(
                    "Returning cached prediction", extra={"cache_key": cache_key}
                )
                return TrackPrediction.model_validate_json(cached_prediction)

            negative_cached = await check_cache(self.redis, negative_cache_key)
            if negative_cached:
                logger.debug(
                    "Negative cache hit; skipping prediction",
                    extra={"negative_cache_key": negative_cache_key},
                )
                return None

            # it makes more sense to get the headsign client-side using the exact trip_id due to API rate limits
            async with aiohttp.ClientSession(base_url=MBTA_V3_ENDPOINT) as session:
                async with MBTAApi(
                    r_client=self.redis,
                    watcher_type=shared_types.shared_types.TaskType.TRACK_PREDICTIONS,
                ) as api:
                    # Fetch headsign and stop/platform once each
                    try:
                        new_hs = await api.get_headsign(trip_id, session, tg)
                        logger.debug(
                            "Fetched headsign",
                            extra={"trip_id": trip_id, "new_headsign": new_hs},
                        )
                        if new_hs:
                            headsign = new_hs
                    except (aiohttp.ClientError, TimeoutError) as e:
                        # network / client-related issues while fetching headsign
                        logger.debug("Failed to get headsign", exc_info=e)

                    try:
                        stop_data = await api.get_stop(session, station_id, tg)
                        logger.debug(
                            "Fetched stop_data",
                            extra={
                                "station_id": station_id,
                                "stop_data_len": len(stop_data)
                                if isinstance(stop_data, (list, tuple))
                                else None,
                            },
                        )
                    except (aiohttp.ClientError, TimeoutError) as e:
                        # Treat stop fetch failures as transient and continue
                        logger.debug("Failed to get stop data", exc_info=e)
                        stop_data = None

                    # If stop/platform info available, prefer platform_code which is authoritative
                    try:
                        if (
                            stop_data
                            and stop_data[0]
                            and getattr(stop_data[0], "data", None)
                            and getattr(stop_data[0].data, "attributes", None)
                            and getattr(
                                stop_data[0].data.attributes, "platform_code", None
                            )
                        ):
                            platform_code = stop_data[0].data.attributes.platform_code
                            logger.debug(
                                "Platform code found in stop_data",
                                extra={"platform_code": platform_code},
                            )
                            # confirmed from the MBTA that this is the platform code
                            prediction = TrackPrediction(
                                station_id=self.normalize_station(station_id),
                                route_id=route_id,
                                trip_id=trip_id,
                                headsign=headsign,
                                direction_id=direction_id,
                                scheduled_time=scheduled_time,
                                track_number=platform_code,
                                confidence_score=1.0,
                                accuracy=1.0,
                                prediction_method="platform_code",
                                historical_matches=0,
                                created_at=datetime.now(UTC),
                            )

                            track_predictions_generated.labels(
                                station_id=self.normalize_station(station_id),
                                route_id=route_id,
                                prediction_method="platform_code",
                                instance=INSTANCE_ID,
                            ).inc()

                            track_prediction_confidence.labels(
                                station_id=self.normalize_station(station_id),
                                route_id=route_id,
                                track_number=prediction.track_number,
                                instance=INSTANCE_ID,
                            ).set(1.0)

                            track_number = prediction.track_number
                            track_confidence = prediction.confidence_score
                            logger.info(
                                f"Generated track prediction (platform_code): {route_id} {scheduled_time.strftime('%H:%M')} {headsign} -> {track_number}"
                                f"(confidence: {track_confidence:.2f})"
                            )

                            tg.start_soon(
                                write_cache,
                                self.redis,
                                cache_key,
                                prediction.model_dump_json(),
                                2 * MONTH,
                            )

                            return prediction
                        else:
                            logger.debug(
                                "No platform_code present in stop_data",
                                extra={"station_id": station_id},
                            )
                    except Exception as e:
                        logger.debug(
                            "Error while checking stop_data for platform_code",
                            exc_info=e,
                        )

            # Analyze historical patterns
            logger.debug(
                "Analyzing historical patterns",
                extra={
                    "station_id": station_id,
                    "route_id": route_id,
                    "headsign": headsign,
                },
            )
            patterns = await self.analyze_patterns(
                station_id,
                route_id,
                headsign,
                direction_id,
                scheduled_time,
            )

            # If no patterns, enqueue ML (non-blocking) and negative-cache briefly
            if not patterns:
                logger.debug(
                    "No patterns returned by analyze_patterns",
                    extra={
                        "station_id": station_id,
                        "route_id": route_id,
                        "headsign": headsign,
                    },
                )
                if self._ml_enabled():
                    try:
                        await self.enqueue_ml_prediction(
                            station_id=station_id,
                            route_id=route_id,
                            direction_id=direction_id,
                            scheduled_time=scheduled_time,
                            trip_id=trip_id,
                            headsign=headsign,
                        )
                        logger.debug(
                            "Enqueued ML prediction due to no patterns",
                            extra={
                                "station_id": station_id,
                                "route_id": route_id,
                                "trip_id": trip_id,
                            },
                        )
                    except Exception as e:
                        logger.debug("Failed to enqueue ML prediction", exc_info=e)
                await write_cache(self.redis, negative_cache_key, "True", 4 * 60)
                logger.debug(
                    "Wrote short negative cache because no patterns present",
                    extra={"negative_cache_key": negative_cache_key},
                )
                return None

            # Debug: log patterns summary
            try:
                logger.debug(
                    "Patterns summary",
                    extra={
                        "station_id": station_id,
                        "route_id": route_id,
                        "patterns": patterns,
                        "pattern_keys": list(patterns.keys()),
                    },
                )
            except (AttributeError, TypeError):
                # Logging failed due to unexpected pattern structure; continue
                logger.debug("Patterns present but failed to log details")

            # Find the most likely track and compute margin-based confidence
            try:
                best_track = max(patterns.keys(), key=lambda x: patterns[x])
            except (ValueError, TypeError) as e:
                # empty or malformed patterns dict; negative-cache briefly and bail out
                logger.debug(
                    "Failed to select best_track from patterns",
                    extra={"patterns": patterns},
                    exc_info=e,
                )
                await write_cache(self.redis, negative_cache_key, "True", 4 * 60)
                return None

            p1 = float(patterns[best_track])
            sorted_vals = sorted(patterns.values(), reverse=True)
            p2 = float(sorted_vals[1]) if len(sorted_vals) > 1 else 0.0
            confidence = 0.5 * (p1 + max(0.0, p1 - p2))
            model_confidence = confidence

            logger.debug(
                "Pattern-derived result",
                extra={
                    "station_id": station_id,
                    "route_id": route_id,
                    "best_track": best_track,
                    "p1": p1,
                    "p2": p2,
                    "initial_confidence": confidence,
                },
            )

            # Non-blocking Bayes fusion with the latest ML result to raise confidence when possible.
            # Additionally, if IMT_ML_COMPARE is enabled, attempt a short (configurable)
            # wait for a recent ML result and compare the ML confidence against the
            # pattern-derived confidence, choosing the higher-confidence result.
            used_bayes = False
            if self._ml_enabled():
                latest_key = f"{self.ML_RESULT_LATEST_PREFIX}{self.normalize_station(station_id)}:{route_id}:{direction_id}:{int(scheduled_time.timestamp())}"
                raw_ml = await self.redis.get(latest_key)  # pyright: ignore
                ml_res = None
                if raw_ml:
                    try:
                        ml_res = json.loads(
                            raw_ml.decode()
                            if isinstance(raw_ml, (bytes, bytearray))
                            else str(raw_ml)
                        )
                        logger.debug(
                            "Found recent ML result",
                            extra={
                                "latest_key": latest_key,
                                "ml_res_keys": list(ml_res.keys())
                                if isinstance(ml_res, dict)
                                else None,
                            },
                        )
                    except (json.JSONDecodeError, TypeError):
                        ml_res = None
                        logger.debug(
                            "Failed to decode recent ML result",
                            extra={"latest_key": latest_key},
                        )

                # If ML-compare mode is enabled, try to get an ML result and compare confidences.
                if self._ml_compare_enabled():
                    logger.debug(
                        "ML-compare mode enabled; attempting to compare ML and pattern",
                        extra={"station_id": station_id, "route_id": route_id},
                    )
                    # If no ML result is available, enqueue a request and wait briefly for it.
                    waited_ms = 0
                    if not ml_res:
                        try:
                            req_id = await self.enqueue_ml_prediction(
                                station_id=station_id,
                                route_id=route_id,
                                direction_id=direction_id,
                                scheduled_time=scheduled_time,
                                trip_id=trip_id,
                                headsign=headsign,
                            )
                            logger.debug(
                                "Enqueued ML request for compare",
                                extra={"req_id": req_id},
                            )
                        except Exception as e:
                            logger.debug(
                                "Failed to enqueue ML request for compare", exc_info=e
                            )

                        wait_ms = self._ml_compare_wait_ms()
                        poll = 50  # ms
                        while waited_ms < wait_ms:
                            await anyio.sleep(poll / 1000.0)
                            waited_ms += poll
                            raw_ml = await self.redis.get(latest_key)  # pyright: ignore
                            if raw_ml:
                                try:
                                    ml_res = json.loads(
                                        raw_ml.decode()
                                        if isinstance(raw_ml, (bytes, bytearray))
                                        else str(raw_ml)
                                    )
                                    logger.debug(
                                        "ML result arrived during compare wait",
                                        extra={
                                            "waited_ms": waited_ms,
                                            "latest_key": latest_key,
                                        },
                                    )
                                    break
                                except (
                                    json.JSONDecodeError,
                                    TypeError,
                                    ValueError,
                                ) as e:
                                    ml_res = None
                                    logger.debug(
                                        "ML result could not be parsed during compare wait",
                                        extra={"waited_ms": waited_ms},
                                        exc_info=e,
                                    )
                                    break
                        logger.debug(
                            "Finished waiting for ML compare",
                            extra={
                                "waited_ms": waited_ms,
                                "wait_ms_config": self._ml_compare_wait_ms(),
                            },
                        )

                    # If we obtained an ML result, compare final (blended) confidence scores.
                    if ml_res and isinstance(ml_res, dict):
                        try:
                            ml_conf = float(ml_res.get("confidence", 0.0))
                        except (TypeError, ValueError):
                            ml_conf = 0.0
                        ml_track = ml_res.get("track_number")
                        logger.debug(
                            "ML result for compare",
                            extra={"ml_conf": ml_conf, "ml_track": ml_track},
                        )

                        # Normalize ML track to string if necessary
                        if isinstance(ml_track, (int, float)):
                            ml_track = str(int(ml_track))
                        # If ML provides a numeric track and it beats the pattern confidence, choose ML
                        if (
                            ml_track
                            and isinstance(ml_track, str)
                            and ml_track.isdigit()
                        ):
                            # If ML wins, adopt ML's prediction (ML confidence is already blended in worker)
                            if ml_conf > confidence + 1e-9:
                                logger.debug(
                                    "ML confidence exceeds pattern confidence; promoting ML result",
                                    extra={
                                        "ml_conf": ml_conf,
                                        "pattern_conf": confidence,
                                        "ml_track": ml_track,
                                    },
                                )
                                best_track = ml_track
                                # adopt blended ML confidence
                                confidence = ml_conf
                                # prefer explicit model_confidence from ML result if present
                                try:
                                    model_confidence = float(
                                        ml_res.get("model_confidence", model_confidence)
                                    )
                                except (ValueError, TypeError):
                                    # If parsing fails, keep previous model_confidence
                                    pass
                                # blend with historical accuracy for display consistency
                                hist_acc = await self._get_prediction_accuracy(
                                    station_id, route_id, best_track
                                )
                                w_hist = self._conf_hist_weight()
                                old_conf = confidence
                                confidence = (
                                    1.0 - w_hist
                                ) * confidence + w_hist * hist_acc
                                logger.debug(
                                    "Blended ML confidence with historical accuracy",
                                    extra={
                                        "old_conf": old_conf,
                                        "hist_acc": hist_acc,
                                        "w_hist": w_hist,
                                        "final_confidence": confidence,
                                    },
                                )
                                used_bayes = False
                                # Record that ML was chosen over the historical/pattern result
                                try:
                                    track_predictions_ml_wins.labels(
                                        station_id=self.normalize_station(station_id),
                                        route_id=route_id,
                                        instance=INSTANCE_ID,
                                    ).inc()
                                except (AttributeError, RuntimeError, ValueError) as e:
                                    # Metric failures should not break prediction flow; log debug with context
                                    logger.debug(
                                        "Failed to increment ML-wins metric",
                                        exc_info=e,
                                    )
                            else:
                                # keep the pattern-derived result; we may still use ML for Bayes fusion below
                                logger.debug(
                                    "ML result did not beat pattern confidence; keeping pattern result",
                                    extra={
                                        "ml_conf": ml_conf,
                                        "pattern_conf": confidence,
                                    },
                                )
                        else:
                            # invalid ML track, ignore and fall back to Bayes fusion path below
                            logger.debug(
                                "ML result had invalid/non-numeric track_number",
                                extra={"ml_track": ml_track},
                            )
                            ml_res = None

                # If ML-compare mode is NOT enabled, preserve original Bayes fusion behavior.
                if not self._ml_compare_enabled() and ml_res:
                    logger.debug(
                        "Applying Bayes fusion with ML probabilities",
                        extra={"station_id": station_id, "route_id": route_id},
                    )
                    try:
                        ml_probs_list = ml_res.get("probabilities")
                        if isinstance(ml_probs_list, list) and len(ml_probs_list) > 0:
                            ml_probs = np.array(ml_probs_list, dtype=np.float64)
                            n_classes = ml_probs.shape[0]
                            # Map pattern dict into aligned vector over classes (track 1->idx0)
                            pattern_vec = np.zeros(n_classes, dtype=np.float64)
                            for t_str, prob in patterns.items():
                                if t_str and t_str.isdigit():
                                    idx = int(t_str) - 1
                                    if 0 <= idx < n_classes:
                                        pattern_vec[idx] = float(prob)
                            s = pattern_vec.sum()
                            if s > 0:
                                pattern_vec = pattern_vec / s
                            logger.debug(
                                "Pattern vec and ML probs prepared for fusion",
                                extra={
                                    "pattern_vec": pattern_vec.tolist(),
                                    "ml_probs": ml_probs.tolist(),
                                    "alpha": self._bayes_alpha(),
                                },
                            )
                            # Bayes fusion: posterior ∝ pattern^alpha * ml^(1-alpha)
                            alpha = self._bayes_alpha()
                            eps = 1e-9
                            fused = np.power(
                                np.clip(pattern_vec, eps, 1.0), alpha
                            ) * np.power(np.clip(ml_probs, eps, 1.0), 1.0 - alpha)
                            total = fused.sum()
                            if total > 0:
                                fused_norm = fused / total
                                logger.debug(
                                    "Fused distribution computed",
                                    extra={"fused_norm": fused_norm.tolist()},
                                )
                                # Apply allowed-tracks mask before selection
                                allowed = await self._get_allowed_tracks(
                                    station_id, route_id
                                )
                                logger.debug(
                                    "Allowed tracks retrieved for fusion",
                                    extra={
                                        "allowed": list(allowed) if allowed else None
                                    },
                                )
                                if allowed:
                                    for idx in range(fused_norm.shape[0]):
                                        if str(idx + 1) not in allowed:
                                            fused_norm[idx] = 0.0
                                    t2 = float(np.sum(fused_norm))
                                    if t2 > 0:
                                        fused_norm = fused_norm / t2
                                        logger.debug(
                                            "Applied allowed mask to fused distribution",
                                            extra={
                                                "fused_norm_masked": fused_norm.tolist()
                                            },
                                        )
                                    else:
                                        # Allowed mask removed all mass; choose top allowed historical track
                                        best_allowed = (
                                            await self._choose_top_allowed_track(
                                                station_id, route_id, allowed
                                            )
                                        )
                                        if best_allowed:
                                            logger.info(
                                                f"Allowed mask removed fused probs for {station_id} {route_id}; falling back to most frequent allowed track {best_allowed}"
                                            )
                                            best_idx = int(best_allowed) - 1
                                            best_track = best_allowed
                                            # compute confidences from fused (all-zero) as conservative defaults
                                            fused_norm = None
                                        else:
                                            logger.debug(
                                                f"Allowed mask removed all fused probabilities for {station_id} {route_id}; no allowed historical fallback found"
                                            )
                                # Select from fused distribution (pre-sharpen) if available
                                if (
                                    fused_norm is not None
                                    and float(np.sum(fused_norm)) > 0
                                ):
                                    best_idx = int(np.argmax(fused_norm))
                                    best_track = str(best_idx + 1)
                                    # Compute confidence from a sharpened version of fused
                                    fused_sharp = self._sharpen_probs(
                                        fused_norm, self._conf_gamma()
                                    )
                                    confidence = self._margin_confidence(
                                        fused_sharp, best_idx
                                    )
                                    # Pre-sharpen, pre-history confidence for reporting
                                    model_confidence = self._margin_confidence(
                                        fused_norm, best_idx
                                    )
                                    # Blend with historical accuracy for the chosen track
                                    hist_acc = await self._get_prediction_accuracy(
                                        station_id, route_id, best_track
                                    )
                                    w_hist = self._conf_hist_weight()
                                else:
                                    # No valid fused distribution. If we selected a best_allowed earlier
                                    # we'll use it as the choice and compute a conservative confidence.
                                    if "best_idx" in locals():
                                        # best_idx was set from allowed historical fallback
                                        best_track = str(best_idx + 1)
                                        hist_acc = await self._get_prediction_accuracy(
                                            station_id, route_id, best_track
                                        )
                                        w_hist = self._conf_hist_weight()
                                        # Blend existing pattern confidence with historical accuracy
                                        confidence = (
                                            1.0 - w_hist
                                        ) * confidence + w_hist * hist_acc
                                        model_confidence = 0.0
                                    else:
                                        # No fused info and no allowed fallback; keep original pattern result
                                        hist_acc = await self._get_prediction_accuracy(
                                            station_id, route_id, best_track
                                        )
                                        w_hist = self._conf_hist_weight()
                                confidence = (
                                    1.0 - w_hist
                                ) * confidence + w_hist * hist_acc
                                used_bayes = True
                    except (ValueError, TypeError, RuntimeError) as e:
                        # Fusion may raise numeric/type errors if shapes mismatch or invalid data
                        logger.debug("Exception during Bayes fusion", exc_info=e)
                        pass

            # Use station-specific confidence threshold with a hard floor of 0.25
            confidence_threshold = self._get_station_confidence_threshold(station_id)
            if confidence_threshold < 0.25:
                confidence_threshold = 0.25
            logger.debug(
                "Confidence and threshold check",
                extra={
                    "confidence": confidence,
                    "confidence_threshold": confidence_threshold,
                    "best_track": best_track,
                },
            )
            if confidence < confidence_threshold or not best_track.isdigit():
                logger.debug(
                    f"No prediction made because confidence below threshold or invalid track. station_id={station_id}, route_id={route_id}, trip_id={trip_id}, headsign={headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, confidence={confidence}, threshold={confidence_threshold}, best_track={best_track}"
                )
                if self._ml_enabled():
                    try:
                        await self.enqueue_ml_prediction(
                            station_id=station_id,
                            route_id=route_id,
                            direction_id=direction_id,
                            scheduled_time=scheduled_time,
                            trip_id=trip_id,
                            headsign=headsign,
                        )
                        logger.debug(
                            "Enqueued ML request because pattern confidence was low",
                            extra={"station_id": station_id, "route_id": route_id},
                        )
                    except (ConnectionError, TimeoutError, RedisError) as e:
                        # Redis/enqueue related failures; log and continue
                        logger.debug(
                            "Failed to enqueue ML request when pattern confidence was low",
                            exc_info=e,
                        )
                await write_cache(self.redis, negative_cache_key, "True", 2 * MONTH)
                logger.debug(
                    "Wrote longer negative cache due to low confidence",
                    extra={"negative_cache_key": negative_cache_key},
                )
                return None

            # Determine prediction method
            method = "bayes_fusion" if used_bayes else "historical_pattern"
            if confidence >= 0.8:
                method = "high_confidence_pattern"
            elif confidence >= 0.6:
                method = "medium_confidence_pattern"
            else:
                method = "low_confidence_pattern"

            logger.debug(
                f"Predicting track for station_id={station_id}, route_id={route_id}, trip_id={trip_id}, headsign={headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, confidence={confidence}, method={method}"
            )
            # Count historical matches
            try:
                historical_matches = sum(
                    1
                    for _ in await self.get_historical_assignments(
                        station_id,
                        route_id,
                        scheduled_time - timedelta(days=180),
                        scheduled_time,
                    )
                )
            except (ConnectionError, TimeoutError) as e:
                # If historical lookups fail due to Redis issues, treat as zero matches
                logger.debug("Error counting historical matches", exc_info=e)
                historical_matches = 0
            logger.debug(
                f"Historical matches={historical_matches}",
                extra={"station_id": station_id, "route_id": route_id},
            )

            # Create prediction
            # Historical accuracy for predicted track
            hist_acc = await self._get_prediction_accuracy(
                station_id, route_id, best_track
            )
            prediction = TrackPrediction(
                station_id=self.normalize_station(station_id),
                route_id=route_id,
                trip_id=trip_id,
                headsign=headsign,
                direction_id=direction_id,
                scheduled_time=scheduled_time,
                track_number=best_track,
                confidence_score=confidence,
                accuracy=hist_acc,
                model_confidence=model_confidence,
                display_confidence=confidence,
                prediction_method=method,
                historical_matches=historical_matches,
                created_at=datetime.now(UTC),
            )

            # Log an INFO-level summary each time a prediction is generated
            try:
                logger.info(
                    f"Track prediction generated: {prediction.station_id} {prediction.route_id} {prediction.scheduled_time.strftime('%Y-%m-%d %H:%M')} -> {prediction.track_number} "
                    f"(conf={prediction.confidence_score:.2f}, model_conf={prediction.model_confidence:.3f}, method={prediction.prediction_method}, historical_matches={prediction.historical_matches})"
                )
            except Exception:
                # Ensure logging failures do not affect prediction flow
                logger.debug("Failed to emit prediction INFO log", exc_info=True)

            # Record metrics
            track_predictions_generated.labels(
                station_id=self.normalize_station(station_id),
                route_id=route_id,
                prediction_method=method,
                instance=INSTANCE_ID,
            ).inc()

            track_prediction_confidence.labels(
                station_id=self.normalize_station(station_id),
                route_id=route_id,
                track_number=best_track,
                instance=INSTANCE_ID,
            ).set(confidence)

            # Store prediction for later validation
            tg.start_soon(self._store_prediction, prediction)
            # If traditional confidence is weak, funnel to ML for a possible replacement
            if self._ml_enabled() and prediction.confidence_score < 0.5:
                try:
                    await self.enqueue_ml_prediction(
                        station_id=station_id,
                        route_id=route_id,
                        direction_id=direction_id,
                        scheduled_time=scheduled_time,
                        trip_id=trip_id,
                        headsign=headsign,
                    )
                    logger.debug(
                        "Enqueued ML request post-prediction due to low confidence",
                        extra={"station_id": station_id, "route_id": route_id},
                    )
                except Exception:
                    logger.debug(
                        "Failed to enqueue ML request post-prediction", exc_info=True
                    )
            # Additionally, sample a percentage of successful predictions for ML exploration
            elif self._ml_enabled() and random.random() < self._ml_sample_prob():
                try:
                    await self.enqueue_ml_prediction(
                        station_id=station_id,
                        route_id=route_id,
                        direction_id=direction_id,
                        scheduled_time=scheduled_time,
                        trip_id=trip_id,
                        headsign=headsign,
                    )
                    logger.debug(
                        "Random-sampled enqueue for ML exploration",
                        extra={"station_id": station_id, "route_id": route_id},
                    )
                except Exception:
                    logger.debug("Failed to enqueue ML for exploration", exc_info=True)
            logger.debug(
                f"Predicted track={prediction.track_number}, station_id={station_id}, route_id={route_id}, trip_id={trip_id}, headsign={headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}, confidence={confidence}, method={method}, historical_matches={historical_matches}"
            )

            return prediction

        except ValidationError as e:
            logger.error("Failed to predict track", exc_info=e)
            await write_cache(self.redis, negative_cache_key, "True", 2 * MONTH)
            return None

    async def _store_prediction(self, prediction: TrackPrediction) -> None:
        """Store a prediction for later validation."""
        try:
            key = f"track_prediction:{self.normalize_station(prediction.station_id)}:{prediction.route_id}:{prediction.trip_id}:{prediction.scheduled_time.date()}"
            await write_cache(
                self.redis,
                key,
                prediction.model_dump_json(),
                WEEK,  # Store for 1 week
            )
        except (ConnectionError, TimeoutError) as e:
            logger.error(
                "Failed to store prediction due to Redis connection issue",
                exc_info=e,
            )
        except ValidationError as e:
            logger.error(
                "Failed to store prediction due to validation error",
                exc_info=e,
            )

    async def _get_prediction_accuracy(
        self, station_id: str, route_id: str, track_number: str
    ) -> float:
        """Get historical prediction accuracy for a specific track."""
        try:
            # Get recent prediction validation results
            accuracy_key = f"track_accuracy:{self.normalize_station(station_id)}:{route_id}:{track_number}"
            accuracy_data = await self.redis.get(accuracy_key)

            if accuracy_data:
                # Parse stored accuracy data (format: "correct:total")
                parts = accuracy_data.decode().split(":")
                if len(parts) == 2:
                    correct = int(parts[0])
                    total = int(parts[1])
                    if total > 0:
                        return correct / total

            # Default accuracy for new tracks (slightly conservative)
            return 0.7

        except (ConnectionError, TimeoutError, ValueError) as e:
            logger.error("Failed to get prediction accuracy", exc_info=e)
            return 0.7  # Default accuracy

    async def _choose_top_allowed_track(
        self, station_id: str, route_id: str, allowed: set[str]
    ) -> Optional[str]:
        """Choose the most frequent historical track among the allowed set.

        Returns the track string or None if unable to decide.
        """
        try:
            end = datetime.now(UTC)
            start = end - timedelta(days=180)
            assignments = await self.get_historical_assignments(
                station_id, route_id, start, end
            )
            counts: dict[str, int] = defaultdict(int)
            for a in assignments:
                if a.track_number and a.track_number in allowed:
                    counts[a.track_number] += 1
            if not counts:
                return None
            # return most frequent; ties resolved by smallest track string
            best = max(sorted(counts.items(), key=lambda x: x[0]), key=lambda x: x[1])
            return best[0]
        except (ConnectionError, TimeoutError, ValueError) as e:
            # Errors fetching historical assignments or processing counts -> no decision
            logger.debug("Unable to choose top allowed track", exc_info=e)
            return None

    async def validate_prediction(
        self,
        station_id: str,
        route_id: str,
        trip_id: str,
        scheduled_time: datetime,
        actual_track_number: Optional[str],
        tg: TaskGroup,
    ) -> None:
        """
        Validate a previous prediction against actual track assignment.

        Args:
            station_id: The station ID
            route_id: The route ID
            trip_id: The trip ID
            scheduled_time: The scheduled departure time
            actual_track_number: The actual track number assigned
        """
        try:
            # Check if this prediction has already been validated
            validation_key = f"track_validation:{self.normalize_station(station_id)}:{route_id}:{trip_id}:{scheduled_time.date()}"
            if await check_cache(self.redis, validation_key):
                logger.debug(
                    f"Prediction already validated for {self.normalize_station(station_id)} {route_id} {trip_id} on {scheduled_time.date()}"
                )
                return

            key = f"track_prediction:{self.normalize_station(station_id)}:{route_id}:{trip_id}:{scheduled_time.date()}"
            prediction_data = await check_cache(self.redis, key)

            if not prediction_data:
                return

            prediction = TrackPrediction.model_validate_json(prediction_data)

            # Check if prediction was correct
            is_correct = prediction.track_number == actual_track_number

            # Update statistics
            tg.start_soon(
                self._update_prediction_stats,
                station_id,
                route_id,
                is_correct,
                prediction.confidence_score,
            )

            # Update per-track historical accuracy for the predicted track
            if prediction.track_number:
                tg.start_soon(
                    self._update_track_accuracy,
                    station_id,
                    route_id,
                    prediction.track_number,
                    is_correct,
                )

            # Record validation metric
            result = "correct" if is_correct else "incorrect"
            track_predictions_validated.labels(
                station_id=station_id,
                route_id=route_id,
                result=result,
                instance=INSTANCE_ID,
            ).inc()

            # Mark as validated to prevent duplicate processing
            tg.start_soon(
                write_cache,
                self.redis,
                validation_key,
                "validated",
                WEEK,
            )

            logger.info(
                f"Validated prediction for {station_id} {route_id}: {'CORRECT' if is_correct else 'INCORRECT'}"
            )

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                "Failed to validate prediction due to Redis connection issue",
                exc_info=e,
            )
        except ValidationError as e:
            logger.error(
                "Failed to validate prediction due to validation error",
                exc_info=e,
            )

    async def _update_track_accuracy(
        self, station_id: str, route_id: str, track_number: str, is_correct: bool
    ) -> None:
        """Update per-track accuracy counters used by _get_prediction_accuracy.

        Stored format is a simple "correct:total" string per station/route/track.
        """
        try:
            key = f"track_accuracy:{self.normalize_station(station_id)}:{route_id}:{track_number}"
            raw = await self.redis.get(key)
            correct = 0
            total = 0
            if raw:
                try:
                    parts = (
                        raw.decode().split(":")
                        if isinstance(raw, (bytes, bytearray))
                        else str(raw).split(":")
                    )
                    if len(parts) == 2:
                        correct = int(parts[0])
                        total = int(parts[1])
                except (ValueError, TypeError, AttributeError):
                    # Malformed stored accuracy value; reset counters
                    correct = 0
                    total = 0
            total += 1
            if is_correct:
                correct += 1
            await write_cache(self.redis, key, f"{correct}:{total}", 30 * DAY)
        except (ConnectionError, TimeoutError) as e:
            logger.error(
                "Failed to update per-track accuracy due to Redis connection issue",
                exc_info=e,
            )
        except (ConnectionError, TimeoutError, OSError) as e:
            # Limit to expected redis/network related errors; treat others as failures elsewhere
            logger.error("Failed to update per-track accuracy", exc_info=e)

    async def _update_prediction_stats(
        self, station_id: str, route_id: str, is_correct: bool, confidence: float
    ) -> None:
        """Update prediction statistics."""
        try:
            stats_key = f"track_stats:{station_id}:{route_id}"
            stats_data = await check_cache(self.redis, stats_key)

            if stats_data:
                stats = TrackPredictionStats.model_validate_json(stats_data)
            else:
                stats = TrackPredictionStats(
                    station_id=station_id,
                    route_id=route_id,
                    total_predictions=0,
                    correct_predictions=0,
                    accuracy_rate=0.0,
                    last_updated=datetime.now(UTC),
                    prediction_counts_by_track={},
                    average_confidence=0.0,
                )

            # Update stats
            stats.total_predictions += 1
            if is_correct:
                stats.correct_predictions += 1
            stats.accuracy_rate = stats.correct_predictions / stats.total_predictions
            stats.last_updated = datetime.now(UTC)

            # Update average confidence
            old_total = (stats.total_predictions - 1) * stats.average_confidence
            stats.average_confidence = (
                old_total + confidence
            ) / stats.total_predictions

            # Store updated stats
            await write_cache(
                self.redis,
                stats_key,
                stats.model_dump_json(),
                30 * DAY,  # Store for 30 days
            )

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                "Failed to update prediction stats due to Redis connection issue",
                exc_info=e,
            )
        except ValidationError as e:
            logger.error(
                "Failed to update prediction stats due to validation error",
                exc_info=e,
            )

    async def get_prediction_stats(
        self, station_id: str, route_id: str
    ) -> Optional[TrackPredictionStats]:
        """Get prediction statistics for a station and route."""
        try:
            stats_key = f"track_stats:{station_id}:{route_id}"
            stats_data = await check_cache(self.redis, stats_key)

            if stats_data:
                return TrackPredictionStats.model_validate_json(stats_data)
            return None

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                "Failed to get prediction stats due to Redis connection issue",
                exc_info=e,
            )
            return None
        except ValidationError as e:
            logger.error(
                "Failed to get prediction stats due to validation error",
                exc_info=e,
            )
            return None
