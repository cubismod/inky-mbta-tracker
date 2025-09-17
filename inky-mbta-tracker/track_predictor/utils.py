# inky-mbta-tracker/inky-mbta-tracker/track_predictor/utils.py
"""
Utility helpers for the track_predictor package.

This module contains small, pure helpers and configuration constants that are
used by the TrackPredictor. Keeping these utilities separate makes them easier
to unit test and reason about.
"""

from __future__ import annotations

import textdistance
from metaphone import doublemetaphone

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

    This function combines multiple signals:
    - normalized Levenshtein similarity
    - Jaccard token similarity
    - phonetic (Double Metaphone) match heuristics
    - simple token overlap ratio

    The weights and thresholds are tuned to favor strong matches while still
    being forgiving of minor differences (abbreviations, punctuation, etc).
    """
    if not headsign1 or not headsign2:
        return 0.0

    h1 = headsign1.lower().strip()
    h2 = headsign2.lower().strip()

    if h1 == h2:
        return 1.0

    # Levenshtein (normalized)
    try:
        lev = textdistance.levenshtein.normalized_similarity(h1, h2)
    except Exception:
        lev = 0.0

    # Jaccard on token sets
    try:
        toks1 = h1.split()
        toks2 = h2.split()
        jacc = textdistance.jaccard.normalized_similarity(toks1, toks2)
    except Exception:
        jacc = 0.0

    # Phonetic similarity via Double Metaphone
    phonetic_match = 0.0
    try:
        dm1 = doublemetaphone(h1)
        dm2 = doublemetaphone(h2)
        # doublemetaphone returns a tuple (primary, alternate)
        if dm1 and dm2:
            if dm1[0] and dm2[0] and dm1[0] == dm2[0]:
                phonetic_match = 0.8
            elif dm1[1] and dm2[1] and dm1[1] == dm2[1]:
                phonetic_match = 0.6
    except Exception:
        phonetic_match = 0.0

    # Token overlap ratio
    try:
        s1 = set(toks1)
        s2 = set(toks2)
        inter = len(s1.intersection(s2))
        union = len(s1.union(s2))
        token_sim = (inter / union) if union > 0 else 0.0
    except Exception:
        token_sim = 0.0

    # Weighted combination (weights chosen to balance edit/phonetic/token signals)
    combined = 0.4 * lev + 0.25 * jacc + 0.2 * phonetic_match + 0.15 * token_sim
    if combined > 1.0:
        combined = 1.0
    if combined < 0.0:
        combined = 0.0
    return combined
