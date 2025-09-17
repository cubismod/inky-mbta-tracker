import json
import logging
import math
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
import textdistance
from anyio import open_file, to_thread
from anyio.abc import TaskGroup
from consts import DAY, INSTANCE_ID, MBTA_V3_ENDPOINT, MONTH, WEEK, YEAR
from exceptions import RateLimitExceeded
from huggingface_hub import hf_hub_download
from keras import Model
from mbta_client import MBTAApi, determine_station_id
from mbta_responses import Schedules
from metaphone import doublemetaphone
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
from redis_cache import check_cache, write_cache
from shared_types.shared_types import (
    MLPredictionRequest,
    TrackAssignment,
    TrackPrediction,
    TrackPredictionStats,
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

# Service type patterns
EXPRESS_KEYWORDS: list[str] = ["express", "limited", "direct"]
LOCAL_KEYWORDS: list[str] = ["local", "all stops", "stopping"]

# Station-specific confidence thresholds
STATION_CONFIDENCE_THRESHOLDS: dict[str, float] = {
    "place-sstat": 0.25,  # South Station - high volume, lower threshold
    "place-north": 0.25,  # North Station - high volume, lower threshold
    "place-bbsta": 0.30,  # Back Bay - medium volume
    "default": 0.35,  # Smaller stations need higher confidence
}


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
        self._initialized = False
        self._initialization_lock = anyio.Lock()

    async def _load_child_stations_map(self) -> Dict[str, str]:
        """Load the child stations JSON file and create a mapping from child -> parent."""
        try:
            child_stations_path = Path(__file__).parent.parent / "child_stations.json"
            async with await open_file(child_stations_path) as f:
                content = await f.read()
                data = json.loads(content)

            # Create reverse mapping: child_id -> parent_station_id
            mapping = {}
            for parent_station, children in data.items():
                for child in children:
                    mapping[child["id"]] = parent_station
            return mapping
        except Exception as e:
            logger.warning(f"Failed to load child stations mapping: {e}")
            return {}

    async def initialize(self) -> None:
        """Initialize the TrackPredictor by loading child stations mapping."""
        if self._initialized:
            return

        self._child_stations_map = await self._load_child_stations_map()
        # Pre-compute supported stations set for fast lookups
        self._supported_stations = (
            set(self._child_stations_map.values())
            if self._child_stations_map
            else set()
        )
        self._initialized = True

    async def _ensure_initialized(self) -> None:
        """Ensure the TrackPredictor is initialized, using lazy initialization."""
        if self._initialized:
            return

        async with self._initialization_lock:
            # Double-check after acquiring the lock
            if self._initialized:
                return
            await self.initialize()

    def normalize_station(self, station_id: str) -> str:
        """
        Normalize station/stop identifiers to canonical 'place-...' station ids
        using the child_stations.json lookup table as the source of truth.
        """
        if not station_id:
            return station_id

        sid = str(station_id)

        # Use fallback if not initialized (for synchronous contexts)
        if not self._initialized:
            # Fallback to determine_station_id for compatibility
            try:
                resolved, has = determine_station_id(sid)
                if resolved and has:
                    return resolved
            except Exception:
                pass
            return sid

        # First check if this ID is directly in our child stations mapping
        if sid in self._child_stations_map:
            return self._child_stations_map[sid]

        # If already a place-* ID, return as-is
        if sid.startswith("place-"):
            return sid

        # Fallback to determine_station_id for other cases
        try:
            resolved, has = determine_station_id(sid)
            if resolved and has:
                return resolved
        except Exception:
            pass

        # Final fallback - return the original ID
        return sid

    def supports_track_predictions(self, station_id: str) -> bool:
        """
        Check if a station supports track predictions based on our child_stations.json mapping.
        Returns True if the normalized station ID is in our supported stations list.
        """
        if not self._initialized:
            # Fallback check for known stations when not initialized
            normalized_station = self.normalize_station(station_id)
            return normalized_station in {"place-north", "place-sstat", "place-bbsta"}

        normalized_station = self.normalize_station(station_id)
        # Use pre-computed supported stations set for fast lookup
        return normalized_station in self._supported_stations

    async def store_historical_assignment(
        self, assignment: TrackAssignment, tg: TaskGroup
    ) -> None:
        """
        Store a historical track assignment for future analysis.

        Args:
            assignment: The track assignment data to store
        """
        # Ensure we're initialized before using normalization
        await self._ensure_initialized()

        try:
            # Store in Redis with a key that includes station, route, and timestamp
            key = f"track_history:{self.normalize_station(assignment.station_id)}:{assignment.route_id}:{assignment.trip_id}:{assignment.scheduled_time.date()}"

            # check if the key already exists
            if await check_cache(self.redis, key):
                return

            # Store for 6 months for analysis
            tg.start_soon(
                write_cache,
                self.redis,
                key,
                assignment.model_dump_json(),
                1 * YEAR,  # 1 year
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
                f"Failed to store track assignment due to Redis connection issue: {e}",
                exc_info=True,
            )
        except ValidationError as e:
            logger.error(
                f"Failed to store track assignment due to validation error: {e}",
                exc_info=True,
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
        """Read sharpening gamma from env; default to 1.3."""
        try:
            return float(os.getenv("IMT_ML_CONF_GAMMA", "1.3"))
        except Exception:
            return 1.3

    @staticmethod
    def _conf_hist_weight() -> float:
        """Weight for blending historical accuracy into confidence (0..1)."""
        try:
            w = float(os.getenv("IMT_CONF_HIST_WEIGHT", "0.3"))
            return min(1.0, max(0.0, w))
        except Exception:
            return 0.3

    @staticmethod
    def _bayes_alpha() -> float:
        """Blend weight for pattern vs ML in Bayes fusion (0..1)."""
        try:
            a = float(os.getenv("IMT_BAYES_ALPHA", "0.65"))
            return min(1.0, max(0.0, a))
        except Exception:
            return 0.65

    @staticmethod
    def _ml_sample_prob() -> float:
        """Fraction of successful traditional predictions to enqueue for ML as exploration."""
        try:
            p = float(os.getenv("IMT_ML_SAMPLE_PCT", "0.1"))
            return min(1.0, max(0.0, p))
        except Exception:
            return 0.1

    @staticmethod
    def _ml_replace_delta() -> float:
        """Minimum improvement required for ML to overwrite an existing prediction."""
        try:
            d = float(os.getenv("IMT_ML_REPLACE_DELTA", "0.05"))
            return max(0.0, d)
        except Exception:
            return 0.05

    @staticmethod
    def _ml_compare_enabled() -> bool:
        """
        If enabled, compare the ML ensemble prediction against the traditional
        pattern-based prediction at runtime and choose the result with higher
        final confidence. Controlled by env IMT_ML_COMPARE.
        """
        val = os.getenv("IMT_ML_COMPARE", "").strip().lower()
        return val in {"1", "true", "yes", "on"}

    @staticmethod
    def _ml_compare_wait_ms() -> int:
        """
        Maximum time in milliseconds to wait for a recent ML result when doing a
        live ML-vs-pattern comparison. If ML result isn't available within this
        window, the code will enqueue a request and proceed using the historical
        result. Controlled by env IMT_ML_COMPARE_WAIT_MS (default 200).
        """
        try:
            return max(0, int(os.getenv("IMT_ML_COMPARE_WAIT_MS", "200")))
        except Exception:
            return 200

    @staticmethod
    def _sharpen_probs(
        vec: Optional[NDArray[np.floating[Any]]],
        gamma: float,
    ) -> Optional[NDArray[np.floating[Any]]]:
        """Sharpen a probability vector by exponentiation and renormalization.

        Selection should be based on the original vector; the sharpened vector is for confidence only.
        """
        if vec is None:
            return None
        if gamma <= 0 or not np.isfinite(gamma):
            gamma = 1.0
        v = np.clip(vec.astype(np.float64), 1e-12, 1.0)
        v = np.power(v, gamma)
        s = v.sum()
        return (v / s) if s > 0 else v

    @staticmethod
    def _margin_confidence(
        vec: Optional[NDArray[np.floating[Any]]],
        index: int,
    ) -> float:
        """Compute 0.5*(p1 + margin) where margin = p1 - p2 around a chosen index."""
        if vec is None:
            return 0.0
        n = vec.size
        if n == 0 or index < 0 or index >= n:
            return 0.0
        p1 = float(vec[index])
        if n == 1:
            return p1
        # compute max of others
        mask = np.ones(n, dtype=bool)
        mask[index] = False
        p2 = float(np.max(vec[mask])) if np.any(mask) else 0.0
        margin = max(0.0, p1 - p2)
        return 0.5 * (p1 + margin)

    async def _get_allowed_tracks(self, station_id: str, route_id: str) -> set[str]:
        """Return a set of allowed track numbers for a station/route based on recent history.

        Cached in Redis for 1 day to avoid heavy scans. Falls back to empty set
        meaning "no restriction" if nothing found.
        """
        # Ensure we're initialized before using normalization
        await self._ensure_initialized()
        cache_key = f"allowed_tracks:{self.normalize_station(station_id)}:{route_id}"
        cached = await check_cache(self.redis, cache_key)
        if cached:
            try:
                lst = json.loads(cached)
                return set(str(x) for x in lst)
            except Exception:
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
        except Exception:
            return set()

    @staticmethod
    def _ml_enabled() -> bool:
        val = os.getenv("IMT_ML", "").strip().lower()
        return val in {"1", "true", "yes", "on"}

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
        """Compute cyclical encodings for hour/minute/day_of_week and raw timestamp."""
        # Use local time where scheduled_time is assumed to be timezone-aware
        hour = ts.hour
        minute = ts.minute
        dow = ts.weekday()  # 0..6
        hour_angle = 2 * math.pi * (hour / 24.0)
        minute_angle = 2 * math.pi * (minute / 60.0)
        day_angle = 2 * math.pi * (dow / 7.0)
        return {
            "hour_sin": math.sin(hour_angle),
            "hour_cos": math.cos(hour_angle),
            "minute_sin": math.sin(minute_angle),
            "minute_cos": math.cos(minute_angle),
            "day_sin": math.sin(day_angle),
            "day_cos": math.cos(day_angle),
            "scheduled_timestamp": ts.timestamp(),
        }

    async def _vocab_get_or_add(self, key: str, token: str) -> int:
        """Get a stable integer index for a token from a Redis hash, adding if missing.

        This maintains consistent integer ids for categorical features like station_id
        and route_id across ML predictions.
        """
        idx = await self.redis.hget(key, token)  # pyright: ignore
        redis_commands.labels("hget").inc()
        if idx is not None:
            try:
                return int(idx) if isinstance(idx, (bytes, bytearray)) else int(idx)
            except (TypeError, ValueError):
                pass

        # Assign next index
        next_id = await self.redis.hlen(key)  # pyright: ignore
        redis_commands.labels("hlen").inc()
        await self.redis.hset(key, token, str(next_id))  # pyright: ignore
        redis_commands.labels("hset").inc()
        return int(next_id)

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
            except Exception as e:  # noqa: BLE001 (surface errors to logs)
                logger.error("Failed to load ML model", exc_info=e)
                raise
            return models

        # Load model once in a worker thread
        logger.info("Starting ML prediction worker: loading model…")
        try:
            models = await to_thread.run_sync(load_models_blocking)
        except Exception:
            logger.error("ML worker disabled due to model load failure.")
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
                    except Exception:
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
                        except Exception:
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
                        except Exception:
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
            except Exception as e:  # noqa: BLE001
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
                        f"Unable to parse schedules for {stations_str} on {route_id}: {e}"
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
                f"Error fetching schedules for {stations_str} on {route_id}: {e}"
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
        # Ensure we're initialized before using normalization
        await self._ensure_initialized()

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
                f"Failed to retrieve historical assignments due to Redis connection issue: {e}",
                exc_info=True,
            )
            return []
        except ValidationError as e:
            logger.error(
                f"Failed to retrieve historical assignments due to validation error: {e}",
                exc_info=True,
            )
            return []

    def _enhanced_headsign_similarity(self, headsign1: str, headsign2: str) -> float:
        """
        Enhanced headsign similarity using multiple matching algorithms.

        Returns a score between 0.0 and 1.0.
        """
        if not headsign1 or not headsign2:
            return 0.0

        # Normalize strings
        h1 = headsign1.lower().strip()
        h2 = headsign2.lower().strip()

        if h1 == h2:
            return 1.0

        # Multiple similarity measures with weights
        levenshtein_sim = textdistance.levenshtein.normalized_similarity(h1, h2)
        jaccard_sim = textdistance.jaccard.normalized_similarity(h1.split(), h2.split())

        # Phonetic similarity using metaphone
        try:
            metaphone1 = doublemetaphone(h1)
            metaphone2 = doublemetaphone(h2)
            phonetic_match = 0.0
            if metaphone1[0] and metaphone2[0] and metaphone1[0] == metaphone2[0]:
                phonetic_match = 0.8
            elif metaphone1[1] and metaphone2[1] and metaphone1[1] == metaphone2[1]:
                phonetic_match = 0.6
        except (AttributeError, TypeError, ValueError):
            phonetic_match = 0.0

        # Token-based similarity for destination matching
        token_sim = 0.0
        tokens1 = set(h1.split())
        tokens2 = set(h2.split())
        if tokens1 and tokens2:
            intersection = len(tokens1.intersection(tokens2))
            union = len(tokens1.union(tokens2))
            token_sim = intersection / union if union > 0 else 0.0

        # Weighted combination
        combined_score = (
            0.4 * levenshtein_sim
            + 0.25 * jaccard_sim
            + 0.2 * phonetic_match
            + 0.15 * token_sim
        )

        return min(1.0, combined_score)

    def _detect_service_type(self, headsign: str) -> str:
        """
        Detect service type from headsign (express, local, etc.).
        """
        headsign_lower = headsign.lower()

        for keyword in EXPRESS_KEYWORDS:
            if keyword in headsign_lower:
                return "express"

        for keyword in LOCAL_KEYWORDS:
            if keyword in headsign_lower:
                return "local"

        return "regular"

    def _is_weekend_service(self, day_of_week: int) -> bool:
        """
        Check if the day represents weekend service.
        """
        return day_of_week in [5, 6]  # Saturday, Sunday

    def _get_station_confidence_threshold(self, station_id: str) -> float:
        """
        Get station-specific confidence threshold.
        """
        return STATION_CONFIDENCE_THRESHOLDS.get(
            station_id, STATION_CONFIDENCE_THRESHOLDS["default"]
        )

    async def analyze_patterns(
        self,
        station_id: str,
        route_id: str,
        trip_headsign: str,
        direction_id: int,
        scheduled_time: datetime,
        tg: TaskGroup,
    ) -> Dict[str, float]:
        """
        Analyze historical patterns to determine track assignment probabilities.

        Args:
            station_id: The station ID
            route_id: The route ID
            trip_headsign: The trip headsign
            direction_id: The direction ID
            scheduled_time: The scheduled departure time

        Returns:
            Dictionary mapping track identifiers to probability scores
        """
        # Ensure we're initialized before using normalization
        await self._ensure_initialized()

        try:
            start_time = time.time()

            start_date = scheduled_time - timedelta(days=180)
            end_date = scheduled_time

            # Get assignments from same route and related routes
            assignments = await self.get_historical_assignments(
                station_id, route_id, start_date, end_date, include_related_routes=True
            )

            if not assignments:
                logger.debug(
                    f"No historical assignments found for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}"
                )
                return {}

            # Analyze patterns by different criteria with expanded windows
            patterns: dict[str, dict[str, int]] = {
                "exact_match": defaultdict(int),  # Same headsign, time, direction
                "headsign_match": defaultdict(int),  # Same headsign, direction
                "platform_consistency": defaultdict(
                    int
                ),  # Consistency with station's modal platform
                "time_match_30": defaultdict(int),  # Same time window (±30 min)
                "time_match_60": defaultdict(int),  # Extended time window (±60 min)
                "time_match_120": defaultdict(int),  # Wide time window (±120 min)
                "direction_match": defaultdict(int),  # Same direction
                "day_of_week_match": defaultdict(int),  # Same day of week
                "service_type_match": defaultdict(int),  # Same service type
                "weekend_pattern_match": defaultdict(int),  # Weekend vs weekday pattern
            }

            target_dow = scheduled_time.weekday()
            target_hour = scheduled_time.hour
            target_minute = scheduled_time.minute
            target_service_type = self._detect_service_type(trip_headsign)
            target_is_weekend = self._is_weekend_service(target_dow)

            # Compute modal track (most common historical track) to capture platform consistency
            try:
                modal_counts: dict[str, int] = defaultdict(int)
                for a in assignments:
                    if a.track_number:
                        modal_counts[a.track_number] += 1
                modal_track = None
                if modal_counts:
                    modal_track = max(modal_counts.items(), key=lambda x: x[1])[0]
            except Exception:
                modal_track = None

            for assignment in assignments:
                if not assignment.track_number:
                    continue

                if assignment.track_number:
                    # Use enhanced headsign similarity
                    headsign_similarity = self._enhanced_headsign_similarity(
                        trip_headsign, assignment.headsign
                    )

                    assignment_service_type = self._detect_service_type(
                        assignment.headsign
                    )
                    assignment_is_weekend = self._is_weekend_service(
                        assignment.day_of_week
                    )

                    # Calculate time differences using actual datetimes (minutes)
                    try:
                        time_diff_minutes = int(
                            abs(
                                (
                                    assignment.scheduled_time - scheduled_time
                                ).total_seconds()
                            )
                            / 60
                        )
                    except Exception:
                        # Fallback to hour/minute arithmetic if scheduled_time is missing
                        time_diff_minutes = abs(
                            assignment.hour * 60
                            + assignment.minute
                            - target_hour * 60
                            - target_minute
                        )

                    # Exact match (enhanced similarity + time + direction)
                    # Exact match window: within 60 minutes and same direction
                    if (
                        headsign_similarity > 0.6
                        and assignment.direction_id == direction_id
                        and time_diff_minutes <= 60
                    ):
                        score = 15 * headsign_similarity  # Graduated scoring
                        patterns["exact_match"][assignment.track_number] += int(score)
                        logger.debug(
                            f"Exact match found (score={score}, similarity={headsign_similarity:.2f})"
                        )

                    # Headsign match (lowered threshold)
                    if (
                        headsign_similarity > 0.5  # Lowered from 0.7
                        and assignment.direction_id == direction_id
                    ):
                        score = 8 * headsign_similarity  # Graduated scoring
                        patterns["headsign_match"][assignment.track_number] += int(
                            score
                        )
                        logger.debug(
                            f"Headsign match found (score={score}, similarity={headsign_similarity:.2f})"
                        )

                    # Multiple time windows with different weights
                    if time_diff_minutes <= 30:
                        patterns["time_match_30"][assignment.track_number] += 5
                    elif time_diff_minutes <= 60:
                        patterns["time_match_60"][assignment.track_number] += 3
                    elif time_diff_minutes <= 120:
                        patterns["time_match_120"][assignment.track_number] += 2

                    # Direction match
                    if assignment.direction_id == direction_id:
                        patterns["direction_match"][assignment.track_number] += 2

                    # Service type match (express vs local)
                    if target_service_type == assignment_service_type:
                        patterns["service_type_match"][assignment.track_number] += 3

                    # Weekend vs weekday pattern matching
                    if target_is_weekend == assignment_is_weekend:
                        patterns["weekend_pattern_match"][assignment.track_number] += 2

                    # Day of week match (reduced weight due to weekend pattern)
                    if assignment.day_of_week == target_dow:
                        patterns["day_of_week_match"][assignment.track_number] += 1

                    # Platform / track consistency (bonus if assignment matches modal track)
                    if modal_track and assignment.track_number == modal_track:
                        patterns["platform_consistency"][assignment.track_number] += 4

            # Combine all patterns with weights and time decay
            combined_scores: dict[str, float] = defaultdict(float)
            sample_counts: dict[str, int] = defaultdict(int)

            # Apply time-based weighting to historical data
            for assignment in assignments:
                if not assignment.track_number:
                    continue

                # Time decay factor (recent data weighted more heavily)
                days_old = (scheduled_time - assignment.scheduled_time).days
                time_decay = max(0.1, 1.0 - (days_old / 90.0))  # 90-day decay

                # Calculate enhanced pattern matches
                headsign_similarity = self._enhanced_headsign_similarity(
                    trip_headsign, assignment.headsign
                )

                assignment_service_type = self._detect_service_type(assignment.headsign)
                assignment_is_weekend = self._is_weekend_service(assignment.day_of_week)

                try:
                    time_diff_minutes = int(
                        abs(
                            (assignment.scheduled_time - scheduled_time).total_seconds()
                        )
                        / 60
                    )
                except Exception:
                    time_diff_minutes = abs(
                        assignment.hour * 60
                        + assignment.minute
                        - target_hour * 60
                        - target_minute
                    )

                base_score = 0.0

                # Exact match with enhanced similarity
                if (
                    headsign_similarity > 0.6
                    and assignment.direction_id == direction_id
                    and time_diff_minutes <= 60
                ):
                    base_score = 15 * headsign_similarity
                # Strong headsign match
                elif (
                    headsign_similarity > 0.5
                    and assignment.direction_id == direction_id
                ):
                    base_score = 8 * headsign_similarity
                # Extended time windows
                elif time_diff_minutes <= 30:
                    base_score = 5.0
                elif time_diff_minutes <= 60:
                    base_score = 3.0
                elif time_diff_minutes <= 120:
                    base_score = 2.0
                elif assignment.direction_id == direction_id:
                    base_score = 2.0
                elif target_is_weekend == assignment_is_weekend:
                    base_score = 2.0  # Weekend/weekday pattern match
                elif assignment.day_of_week == target_dow:
                    base_score = 1.0  # Basic day match

                # Bonus for service type match
                if target_service_type == assignment_service_type and base_score > 0:
                    base_score = base_score * 1.2  # 20% bonus

                if base_score > 0:
                    combined_scores[assignment.track_number] += base_score * time_decay
                    sample_counts[assignment.track_number] += 1

                # Convert to probabilities with confidence adjustments
            if combined_scores:
                total_score = sum(combined_scores.values())
                total: dict[str, float] = {}
                for track, score in combined_scores.items():
                    base_prob = score / total_score
                    sample_size = sample_counts[track]

                    # Sample size confidence (more samples = higher confidence)
                    sample_confidence = min(1.0, sample_size / 10.0)

                    # Historical accuracy factor
                    accuracy = await self._get_prediction_accuracy(
                        station_id, route_id, track
                    )

                    # Combined confidence adjustment
                    confidence_factor = 0.3 + 0.4 * sample_confidence + 0.3 * accuracy
                    adjusted_prob = base_prob * confidence_factor
                    total[track] = adjusted_prob

                # Renormalize after confidence adjustments
                adj_total = sum(total.values())
                if adj_total > 0:
                    total = {track: prob / adj_total for track, prob in total.items()}
                logger.debug(
                    f"Calculated pattern for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}"
                )
                logger.debug(f"Total score={total_score}, total={total}")

                # Record analysis duration
                duration = time.time() - start_time
                track_pattern_analysis_duration.labels(
                    station_id=station_id, route_id=route_id, instance=INSTANCE_ID
                ).set(duration)

                return total

            logger.debug(
                f"No patterns found for station_id={station_id}, route_id={route_id}, trip_id={trip_headsign}, direction_id={direction_id}, scheduled_time={scheduled_time}"
            )

            # Record analysis duration even for no patterns
            duration = time.time() - start_time
            track_pattern_analysis_duration.labels(
                station_id=station_id, route_id=route_id, instance=INSTANCE_ID
            ).set(duration)

            return {}

        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to analyze patterns due to Redis connection issue: {e}",
                exc_info=True,
            )
            return {}
        except ValidationError as e:
            logger.error(
                f"Failed to analyze patterns due to validation error: {e}",
                exc_info=True,
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
        # Ensure we're initialized before prediction
        await self._ensure_initialized()
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
                    except Exception as e:
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
                    except Exception as e:
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
                tg,
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
            except Exception:
                logger.debug("Patterns present but failed to log details")

            # Find the most likely track and compute margin-based confidence
            try:
                best_track = max(patterns.keys(), key=lambda x: patterns[x])
            except Exception:
                logger.debug(
                    "Failed to select best_track from patterns",
                    extra={"patterns": patterns},
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
                    except Exception:
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
                                except Exception:
                                    ml_res = None
                                    logger.debug(
                                        "ML result could not be parsed during compare wait",
                                        extra={"waited_ms": waited_ms},
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
                        except Exception:
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
                                except Exception:
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
                                except Exception:
                                    # Metric failures should not break prediction flow
                                    logger.debug(
                                        "Failed to increment ML-wins metric",
                                        exc_info=True,
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
                    except Exception:
                        logger.debug("Exception during Bayes fusion", exc_info=True)
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
                    except Exception:
                        logger.debug(
                            "Failed to enqueue ML request when pattern confidence was low",
                            exc_info=True,
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
            except Exception as e:
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
        # Ensure we're initialized before using normalization
        await self._ensure_initialized()
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
                f"Failed to store prediction due to Redis connection issue: {e}",
                exc_info=True,
            )
        except ValidationError as e:
            logger.error(
                f"Failed to store prediction due to validation error: {e}",
                exc_info=True,
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
        except Exception:
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
                f"Failed to validate prediction due to Redis connection issue: {e}",
                exc_info=True,
            )
        except ValidationError as e:
            logger.error(
                f"Failed to validate prediction due to validation error: {e}",
                exc_info=True,
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
                except Exception:
                    correct = 0
                    total = 0
            total += 1
            if is_correct:
                correct += 1
            await write_cache(self.redis, key, f"{correct}:{total}", 30 * DAY)
        except (ConnectionError, TimeoutError) as e:
            logger.error(
                f"Failed to update per-track accuracy due to Redis connection issue: {e}",
                exc_info=True,
            )
        except Exception as e:  # noqa: BLE001
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
                f"Failed to update prediction stats due to Redis connection issue: {e}",
                exc_info=True,
            )
        except ValidationError as e:
            logger.error(
                f"Failed to update prediction stats due to validation error: {e}",
                exc_info=True,
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
                f"Failed to get prediction stats due to Redis connection issue: {e}",
                exc_info=True,
            )
            return None
        except ValidationError as e:
            logger.error(
                f"Failed to get prediction stats due to validation error: {e}",
                exc_info=True,
            )
            return None
