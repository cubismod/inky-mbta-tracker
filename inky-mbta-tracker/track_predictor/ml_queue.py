"""ML queue and worker module for track prediction system.

This module handles ML prediction request queuing, background processing,
and ensemble model inference for track predictions.
"""

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from typing import Any, Callable, Dict, List, Optional

import anyio
import numpy as np
from anyio import to_thread
from consts import DAY, INSTANCE_ID, MONTH
from huggingface_hub import hf_hub_download
from keras import Model
from prometheus import (
    redis_commands,
    track_prediction_confidence,
    track_predictions_generated,
)
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import ConnectionError, RedisError, TimeoutError
from redis_cache import write_cache
from shared_types.shared_types import MLPredictionRequest, TrackPrediction

from .ml import (
    conf_gamma,
    conf_hist_weight,
    cyclical_time_features,
    margin_confidence,
    ml_enabled,
    ml_replace_delta,
    sharpen_probs,
    vocab_get_or_add,
)
from .redis_ops import (
    choose_top_allowed_track,
    get_allowed_tracks,
    get_prediction_accuracy,
)

logger = logging.getLogger(__name__)


class MLQueue:
    """Manages ML prediction request queueing and processing."""

    # Redis keys for ML prediction queueing
    ML_QUEUE_KEY = "ml:track_predict:requests"
    ML_RESULT_KEY_PREFIX = "ml:track_predict:result:"
    ML_RESULT_LATEST_PREFIX = "ml:track_predict:latest:"
    ML_STATION_VOCAB_KEY = "ml:vocab:station"
    ML_ROUTE_VOCAB_KEY = "ml:vocab:route"

    def __init__(
        self, redis: Redis, normalize_station_fn: Callable[[str], str]
    ) -> None:
        self.redis = redis
        self.normalize_station = normalize_station_fn

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

    async def get_ml_result(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get ML prediction result by request ID."""
        try:
            result_key = f"{self.ML_RESULT_KEY_PREFIX}{request_id}"
            raw_result = await self.redis.get(result_key)  # pyright:ignore
            if raw_result:
                return json.loads(
                    raw_result.decode()
                    if isinstance(raw_result, (bytes, bytearray))
                    else str(raw_result)
                )
            return None
        except (json.JSONDecodeError, TypeError, ConnectionError, TimeoutError) as e:
            logger.debug("Failed to get ML result", exc_info=e)
            return None

    async def get_latest_ml_result(
        self,
        station_id: str,
        route_id: str,
        direction_id: int,
        scheduled_time: datetime,
    ) -> Optional[Dict[str, Any]]:
        """Get the latest ML result for a specific prediction request."""
        try:
            latest_key = f"{self.ML_RESULT_LATEST_PREFIX}{self.normalize_station(station_id)}:{route_id}:{direction_id}:{int(scheduled_time.timestamp())}"
            raw_ml = await self.redis.get(latest_key)  # pyright: ignore
            if raw_ml:
                return json.loads(
                    raw_ml.decode()
                    if isinstance(raw_ml, (bytes, bytearray))
                    else str(raw_ml)
                )
            return None
        except (json.JSONDecodeError, TypeError, ConnectionError, TimeoutError) as e:
            logger.debug("Failed to get latest ML result", exc_info=e)
            return None

    async def vocab_get_or_add(self, key: str, token: str) -> int:
        """Get a stable integer index for a token from a Redis hash, adding if missing."""
        return await vocab_get_or_add(self.redis, key, token)

    async def ml_prediction_worker(self) -> None:
        """
        Background worker that consumes prediction requests from Redis and
        generates predictions using the Keras ensemble model.

        Runs blocking model load/inference in an anyio thread.
        """
        if not ml_enabled():
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
            except (OSError, ImportError, RuntimeError) as e:  # noqa: BLE001 (log error before re-raising)
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

                await self._process_ml_request(payload, models)

            except ValidationError as e:
                logger.error("Unable to validate ML request", exc_info=e)
            except (
                RuntimeError,
                OSError,
            ) as e:  # keep worker alive for recoverable runtime errors
                logger.error("Error in ML prediction worker loop", exc_info=e)
                await anyio.sleep(0.5)

    async def _process_ml_request(
        self, payload: MLPredictionRequest, models: List[Model]
    ) -> None:
        """Process a single ML prediction request."""
        req_id = payload.id
        station_id = payload.station_id
        route_id = payload.route_id
        direction_id = payload.direction_id
        scheduled_time = payload.scheduled_time
        trip_id = payload.trip_id
        headsign = payload.headsign

        # Vocab lookups
        station_idx = await self.vocab_get_or_add(self.ML_STATION_VOCAB_KEY, station_id)
        route_idx = await self.vocab_get_or_add(self.ML_ROUTE_VOCAB_KEY, route_id)

        feats = cyclical_time_features(scheduled_time)

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

        # Process ensemble predictions
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

        if not model_top_classes:
            logger.warning(f"No valid model predictions for {station_id} {route_id}")
            return

        # Majority vote; ties resolve to smallest index via argmax
        predicted_track = int(np.bincount(model_top_classes).argmax())
        # Mean probabilities across models for confidence
        mean_probs = (
            np.mean(np.stack(flat_probs, axis=0), axis=0) if flat_probs else None
        )

        # Apply allowed-tracks mask from history
        if mean_probs is not None:
            allowed = await get_allowed_tracks(self.redis, station_id, route_id)
            if allowed:
                masked = mean_probs.copy()
                for idx in range(masked.shape[0]):
                    if str(idx + 1) not in allowed:
                        masked[idx] = 0.0
                total = float(np.sum(masked))

                if total > 0:
                    mean_probs = masked / total
                    predicted_track = int(np.argmax(mean_probs))
                else:
                    # Try to pick top allowed track from history as fallback
                    best_allowed = await choose_top_allowed_track(
                        self.redis, station_id, route_id, allowed
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
        model_confidence = (
            margin_confidence(mean_probs, predicted_track)
            if mean_probs is not None
            else 0.0
        )

        # Sharpened margin-based display confidence
        probs_for_conf = (
            sharpen_probs(mean_probs, conf_gamma()) if mean_probs is not None else None
        )
        confidence = (
            margin_confidence(probs_for_conf, predicted_track)
            if probs_for_conf is not None
            else 0.0
        )

        track_number = str(predicted_track + 1)  # 1-based for MBTA tracks

        # Drop very low-confidence ML predictions
        if confidence < 0.25:
            logger.debug(
                f"Dropping low-confidence ML prediction for {station_id} {route_id} at {scheduled_time}: track={track_number}, confidence={confidence:.3f}"
            )
            return

        # Historical accuracy for predicted track number
        hist_acc = await get_prediction_accuracy(
            self.redis, station_id, route_id, track_number
        )

        # Blend confidence with historical accuracy
        w_hist = conf_hist_weight()
        confidence = (1.0 - w_hist) * confidence + w_hist * hist_acc

        # Store ML result
        await self._store_ml_result(
            req_id,
            station_id,
            route_id,
            direction_id,
            scheduled_time,
            trip_id,
            headsign,
            predicted_track,
            track_number,
            confidence,
            model_confidence,
            hist_acc,
            mean_probs,
            station_idx,
            route_idx,
        )

    async def _store_ml_result(
        self,
        req_id: str,
        station_id: str,
        route_id: str,
        direction_id: int,
        scheduled_time: datetime,
        trip_id: str,
        headsign: str,
        predicted_track: int,
        track_number: str,
        confidence: float,
        model_confidence: float,
        hist_acc: float,
        mean_probs: Optional[np.ndarray[Any, np.dtype[Any]]],
        station_idx: int,
        route_idx: int,
    ) -> None:
        """Store ML prediction result in Redis."""
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
            "probabilities": mean_probs.tolist() if mean_probs is not None else None,
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

        # Conditionally store as TrackPrediction if it beats existing result
        await self._maybe_store_track_prediction(
            station_id,
            route_id,
            trip_id,
            headsign,
            direction_id,
            scheduled_time,
            track_number,
            confidence,
            model_confidence,
            hist_acc,
        )

    async def _maybe_store_track_prediction(
        self,
        station_id: str,
        route_id: str,
        trip_id: str,
        headsign: str,
        direction_id: int,
        scheduled_time: datetime,
        track_number: str,
        confidence: float,
        model_confidence: float,
        hist_acc: float,
    ) -> None:
        """Store TrackPrediction if ML result is better than existing prediction."""
        from redis_cache import check_cache

        cache_key = f"track_prediction:{self.normalize_station(station_id)}:{route_id}:{trip_id}:{scheduled_time.date()}"
        do_write = False
        existing_json = await check_cache(self.redis, cache_key)

        if existing_json:
            try:
                existing = TrackPrediction.model_validate_json(existing_json)
                delta = ml_replace_delta()
                if existing.prediction_method != "ml_ensemble":
                    if confidence > (existing.confidence_score + delta):
                        do_write = True
                else:
                    if confidence > existing.confidence_score:
                        do_write = True
            except (ValidationError, ValueError, TypeError):
                # If parsing fails, decide conservatively by confidence
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

            # Clean up negative cache
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


# Factory function for creating ML queue instances
def create_ml_queue(
    redis: Redis, normalize_station_fn: Callable[[str], str]
) -> MLQueue:
    """Create an ML queue instance with the given Redis client and normalization function."""
    return MLQueue(redis, normalize_station_fn)
