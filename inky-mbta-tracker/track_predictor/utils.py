"""
Utility helpers for the track_predictor package.

This module contains small, pure helpers and configuration constants that are
used by the TrackPredictor. Keeping these utilities separate makes them easier
to unit test and reason about.
"""

from __future__ import annotations

import textdistance

# Service type keywords used to infer express/local/regular service from headsigns.
EXPRESS_KEYWORDS: list[str] = ["express", "limited", "direct"]
LOCAL_KEYWORDS: list[str] = ["local", "all stops", "stopping"]

# Station-specific confidence thresholds. Values represent the minimum required
# confidence before a historical/pattern-derived prediction is accepted.
STATION_CONFIDENCE_THRESHOLDS: dict[str, float] = {
    "place-sstat": 0.25,  # South Station - high volume, lower threshold
    "place-north": 0.25,  # North Station - high volume, lower threshold
    "place-bbsta": 0.30,  # Back Bay - medium volume
    "default": 0.35,  # Smaller stations need higher confidence
}


def detect_service_type(headsign: str) -> str:
    """
    Detect service type from a headsign string.

    Returns one of: "express", "local", "regular".
    The detection is simple keyword-based and intentionally conservative.
    """
    if not headsign:
        return "regular"

    hl = headsign.lower()
    for kw in EXPRESS_KEYWORDS:
        if kw in hl:
            return "express"
    for kw in LOCAL_KEYWORDS:
        if kw in hl:
            return "local"
    return "regular"


def is_weekend_service(day_of_week: int) -> bool:
    """
    Return True if the provided day_of_week (0=Mon .. 6=Sun) is a weekend.
    """
    return day_of_week in (5, 6)


def get_station_confidence_threshold(station_id: str) -> float:
    """
    Return a station-specific confidence threshold or a sensible default.
    """
    return STATION_CONFIDENCE_THRESHOLDS.get(
        station_id, STATION_CONFIDENCE_THRESHOLDS["default"]
    )


def enhanced_headsign_similarity(headsign1: str, headsign2: str) -> float:
    """
    Compute an enhanced similarity score between two headsigns in [0.0, 1.0].
    """
    if not headsign1 or not headsign2:
        return 0.0

    h1 = headsign1.lower().strip()
    h2 = headsign2.lower().strip()

    if h1 == h2:
        return 1.0

    # Base Levenshtein (normalized)
    try:
        lev = textdistance.levenshtein.normalized_similarity(h1, h2)
    except (TypeError, ValueError):
        lev = 0.0

    # Token overlap bonus (handles reordering like "South Station" vs "Station South")
    toks1 = [t for t in h1.split() if t]
    toks2 = [t for t in h2.split() if t]
    if toks1 and toks2:
        set1, set2 = set(toks1), set(toks2)
        token_overlap = len(set1 & set2) / max(len(set1 | set2), 1)
    else:
        token_overlap = 0.0

    # Blend the scores: give some weight to token overlap so reordered tokens score higher
    score = 0.7 * lev + 0.3 * token_overlap
    # Ensure within bounds
    if score < 0.0:
        score = 0.0
    if score > 1.0:
        score = 1.0
    return score
