"""
Machine-learning related helper utilities for the track predictor.

This module contains pure helpers and small integration helpers that encapsulate
the ML-specific logic previously embedded in the large TrackPredictor class.
"""

from __future__ import annotations

import math
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
from numpy.typing import NDArray


# -----------------------------------------------------------------------------
# Environment-backed configuration helpers
# -----------------------------------------------------------------------------
def conf_gamma() -> float:
    """Sharpening gamma; read from IMT_ML_CONF_GAMMA, default 1.3."""
    try:
        return float(os.getenv("IMT_ML_CONF_GAMMA", "1.3"))
    except (ValueError, TypeError):
        return 1.3


def conf_hist_weight() -> float:
    """Weight to blend historical accuracy into displayed confidence (0..1)."""
    try:
        w = float(os.getenv("IMT_CONF_HIST_WEIGHT", "0.3"))
        return min(1.0, max(0.0, w))
    except (ValueError, TypeError):
        return 0.3


def bayes_alpha() -> float:
    """Blend weight for pattern vs ML in Bayes fusion (0..1)."""
    try:
        a = float(os.getenv("IMT_BAYES_ALPHA", "0.65"))
        return min(1.0, max(0.0, a))
    except (ValueError, TypeError):
        return 0.65


def ml_sample_prob() -> float:
    """Fraction of successful traditional predictions to enqueue for ML exploration."""
    try:
        p = float(os.getenv("IMT_ML_SAMPLE_PCT", "0.1"))
        return min(1.0, max(0.0, p))
    except (ValueError, TypeError):
        return 0.1


def ml_replace_delta() -> float:
    """Minimum improvement required for ML to overwrite an existing prediction."""
    try:
        d = float(os.getenv("IMT_ML_REPLACE_DELTA", "0.05"))
        return max(0.0, d)
    except (ValueError, TypeError):
        return 0.05


def ml_compare_enabled() -> bool:
    """Return True if IMT_ML_COMPARE environment flag is enabled."""
    val = os.getenv("IMT_ML_COMPARE", "").strip().lower()
    return val in {"1", "true", "yes", "on"}


def ml_compare_wait_ms() -> int:
    """Maximum time (ms) to wait for an ML result when comparing live (default 200)."""
    try:
        return max(0, int(os.getenv("IMT_ML_COMPARE_WAIT_MS", "200")))
    except (ValueError, TypeError):
        return 200


def ml_enabled() -> bool:
    """Return True if ML features are enabled via IMT_ML env var."""
    val = os.getenv("IMT_ML", "").strip().lower()
    return val in {"1", "true", "yes", "on"}


# -----------------------------------------------------------------------------
# Probability & confidence helpers
# -----------------------------------------------------------------------------
def sharpen_probs(
    vec: NDArray[np.floating[Any]], gamma: float
) -> NDArray[np.floating[Any]]:
    """
    Sharpen a probability vector by exponentiation then renormalization.

    - gamma <= 0 or non-finite behaves like gamma == 1 (no-op).
    - clips tiny values to avoid numerical underflow.

    Returns a new numpy array of the same shape.
    """
    if gamma <= 0 or not np.isfinite(gamma):
        gamma = 1.0
    v = np.clip(vec.astype(np.float64), 1e-12, 1.0)
    v = np.power(v, gamma)
    s = v.sum()
    return (v / s) if s > 0 else v


def margin_confidence(vec: NDArray[np.floating[Any]], index: int) -> float:
    """
    Compute a margin-based confidence for a selected index.

    The formula used is: 0.5 * (p1 + margin) where margin = p1 - p2, and p2 is the
    largest probability among the other classes. This yields a score in [0,1].
    """
    n = vec.size
    if n == 0 or index < 0 or index >= n:
        return 0.0
    p1 = float(vec[index])
    if n == 1:
        return p1
    mask = np.ones(n, dtype=bool)
    mask[index] = False
    p2 = float(np.max(vec[mask])) if np.any(mask) else 0.0
    margin = max(0.0, p1 - p2)
    return 0.5 * (p1 + margin)


# -----------------------------------------------------------------------------
# Feature preparation
# -----------------------------------------------------------------------------
def cyclical_time_features(ts: datetime) -> Dict[str, float]:
    """
    Compute cyclical encodings for hour/minute/day_of_week and provide raw timestamp.

    Output dictionary keys match the previous implementation's expected names.
    """
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


def build_model_input(
    station_idx: int,
    route_idx: int,
    direction_id: int,
    cyclical_feats: Dict[str, float],
) -> Dict[str, NDArray[Any]]:
    """
    Build the feature dict consumed by the Keras model(s).

    Returns numpy arrays with appropriate dtypes and batch dimension (1, ...).
    """
    return {
        "station_id": np.array([station_idx], dtype=np.int64),
        "route_id": np.array([route_idx], dtype=np.int64),
        "direction_id": np.array([int(direction_id)], dtype=np.int64),
        "hour_sin": np.array([cyclical_feats["hour_sin"]], dtype=np.float32),
        "hour_cos": np.array([cyclical_feats["hour_cos"]], dtype=np.float32),
        "minute_sin": np.array([cyclical_feats["minute_sin"]], dtype=np.float32),
        "minute_cos": np.array([cyclical_feats["minute_cos"]], dtype=np.float32),
        "day_sin": np.array([cyclical_feats["day_sin"]], dtype=np.float32),
        "day_cos": np.array([cyclical_feats["day_cos"]], dtype=np.float32),
        "scheduled_timestamp": np.array(
            [cyclical_feats["scheduled_timestamp"]], dtype=np.float32
        ),
    }


# -----------------------------------------------------------------------------
# Ensemble model loading + aggregation helpers
# -----------------------------------------------------------------------------
def load_ensemble_model_paths(
    ensemble_size: int = 5,
    filename_template: str = "track_prediction_ensemble_model_{i}_best.keras",
) -> List[str]:
    """
    Generate the expected filenames for an ensemble. Does not perform network IO.

    The caller can use these names with huggingface_hub.hf_hub_download or other means
    to obtain the actual file paths.
    """
    return [filename_template.format(i=i) for i in range(ensemble_size)]


def aggregate_model_outputs(
    probs_list: Sequence[NDArray[np.floating[Any]]],
) -> Tuple[Optional[int], Optional[NDArray[np.floating[Any]]]]:
    """
    Aggregate a list of model output probability arrays.

    Each entry in probs_list is expected to be a 1-D array (or 2-D with batch dim=1).
    Returns a tuple of (predicted_class_index, mean_probabilities) or (None, None)
    if aggregation is not possible.
    """
    model_top_classes: List[int] = []
    flat_probs: List[NDArray[np.floating[Any]]] = []

    for p in probs_list:
        arr = np.array(p)
        try:
            if arr.ndim == 2 and arr.shape[0] == 1:
                flat = arr[0]
            elif arr.ndim == 1:
                flat = arr
            else:
                flat = arr.reshape(-1)
            if flat.size == 0:
                continue
            idx = int(np.argmax(flat))
            model_top_classes.append(idx)
            flat_probs.append(flat.astype(np.float32))
        except (ValueError, IndexError, TypeError):
            # Skip malformed outputs
            continue

    if not model_top_classes:
        return None, None

    # Majority vote for final class, ties favor smallest index via argmax over bincount
    predicted = int(np.bincount(model_top_classes).argmax())

    # Mean probabilities across models
    mean_probs = np.mean(np.stack(flat_probs, axis=0), axis=0) if flat_probs else None

    return predicted, mean_probs


def apply_allowed_mask(
    mean_probs: NDArray[np.floating[Any]], allowed: Optional[Iterable[str]]
) -> Tuple[NDArray[np.floating[Any]], bool]:
    """
    Apply an allowed-tracks mask to a mean probability vector.

    - mean_probs: 1D numpy array for classes where index 0 == track '1'.
    - allowed: iterable of string track numbers (e.g., {'1', '2'}) or None/empty to skip masking.

    Returns (masked_probs, all_removed_flag) where all_removed_flag is True if
    the mask removed all probability mass (i.e., sum == 0).
    """
    if allowed is None:
        return mean_probs, False

    masked = mean_probs.copy()
    n = masked.shape[0]
    for idx in range(n):
        if str(idx + 1) not in allowed:
            masked[idx] = 0.0
    total = float(np.sum(masked))
    if total > 0.0:
        masked = masked / total
        return masked, False
    return mean_probs, True  # indicate masking removed all mass


def bayes_fusion(
    pattern_vec: NDArray[np.floating[Any]],
    ml_probs: NDArray[np.floating[Any]],
    alpha: float,
) -> Optional[NDArray[np.floating[Any]]]:
    """
    Fuse a pattern-derived distribution with ML probabilities using a simple
    log-space-like power weighting: posterior ∝ pattern^alpha * ml^(1-alpha).

    Returns a normalized fused numpy array or None if fusion not possible.
    """
    if pattern_vec is None or ml_probs is None:
        return None
    try:
        n = max(pattern_vec.size, ml_probs.size)
        # Broadcast/reshape as necessary
        p = np.array(pattern_vec, dtype=np.float64)
        m = np.array(ml_probs, dtype=np.float64)
        # Ensure same length by padding smaller vector with zeros (conservative)
        if p.size < n:
            p = np.pad(p, (0, n - p.size), constant_values=0.0)
        if m.size < n:
            m = np.pad(m, (0, n - m.size), constant_values=0.0)

        eps = 1e-9
        fused = np.power(np.clip(p, eps, 1.0), alpha) * np.power(
            np.clip(m, eps, 1.0), 1.0 - alpha
        )
        total = fused.sum()
        if total > 0:
            return fused / total
        return None
    except (ValueError, TypeError):
        return None


# -----------------------------------------------------------------------------
# Redis-backed vocabulary helper (async)
# -----------------------------------------------------------------------------
async def vocab_get_or_add(redis_client: Any, key: str, token: str) -> int:
    """
    Get a stable integer index for a token from a Redis hash, adding if missing.

    - redis_client must implement hget, hlen, hset as awaitable methods.
    - Returns an int index for the token.
    """
    idx = await redis_client.hget(key, token)  # pyright: ignore
    if idx is not None:
        try:
            return int(idx) if isinstance(idx, (bytes, bytearray)) else int(idx)
        except (TypeError, ValueError):
            pass

    next_id = await redis_client.hlen(key)  # pyright: ignore
    await redis_client.hset(key, token, str(next_id))  # pyright: ignore
    return int(next_id)
