# inky-mbta-tracker/inky-mbta-tracker/track_predictor/pattern.py
"""
Pattern scoring helpers extracted from TrackPredictor.analyze_patterns.

This module provides pure (or nearly-pure) helpers that encapsulate the
assignment scoring and probability computation logic used to build
pattern-derived track assignment distributions.

Primary helpers:
- compute_modal_track(assignments)
- compute_assignment_scores(assignments, trip_headsign, direction_id, scheduled_time)
- compute_final_probabilities(combined_scores, sample_counts, accuracy_lookup)

The `compute_final_probabilities` function is async because historical
accuracy is typically obtained via an async Redis lookup. To keep the
scoring logic testable, the module accepts an injected async `accuracy_lookup`
callable that maps a track identifier (string) to a float accuracy score.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Optional,
    Tuple,
)

from .utils import (
    detect_service_type,
    enhanced_headsign_similarity,
    is_weekend_service,
)


def compute_modal_track(assignments: Iterable[Any]) -> Optional[str]:
    """
    Compute the modal (most frequent) track among assignments.
    Returns the track string or None when no modal track exists.
    """
    counts: Dict[str, int] = {}
    for a in assignments:
        tn = getattr(a, "track_number", None)
        if tn:
            counts[tn] = counts.get(tn, 0) + 1
    if not counts:
        return None
    # Tie-break by smallest track string via sorting by track then by count
    best = max(sorted(counts.items(), key=lambda x: x[0]), key=lambda x: x[1])
    return best[0] if best else None


def compute_assignment_scores(
    assignments: Iterable[Any],
    trip_headsign: str,
    direction_id: int,
    scheduled_time: datetime,
) -> Tuple[Dict[str, float], Dict[str, int], Dict[str, Dict[str, int]]]:
    """
    Score historical assignments for a target trip.

    Returns a tuple:
      (combined_scores, sample_counts, patterns)

    - combined_scores: per-track aggregated (pre-normalization) float scores
    - sample_counts: per-track integer sample counts (how many assignments contributed)
    - patterns: dict of individual pattern-count buckets (useful for debugging/metrics)

    The scoring function mirrors the original TrackPredictor logic:
      - uses enhanced headsign similarity
      - multiple time windows (±30/60/120 minutes)
      - direction, day-of-week, weekend pattern, service type, and platform/modal consistency
      - a time-decay factor favors recent assignments
    """
    # Initialize pattern buckets for debugging/metrics
    patterns: Dict[str, Dict[str, int]] = {
        "exact_match": defaultdict(int),  # Same headsign, time, direction
        "headsign_match": defaultdict(int),
        "platform_consistency": defaultdict(int),
        "time_match_30": defaultdict(int),
        "time_match_60": defaultdict(int),
        "time_match_120": defaultdict(int),
        "direction_match": defaultdict(int),
        "day_of_week_match": defaultdict(int),
        "service_type_match": defaultdict(int),
        "weekend_pattern_match": defaultdict(int),
    }

    # Materialize assignments as list to iterate multiple times
    assignment_list = list(assignments)

    if not assignment_list:
        return {}, {}, patterns

    # Precompute modal track for platform consistency bonus
    try:
        modal_counts: Dict[str, int] = {}
        for a in assignment_list:
            if getattr(a, "track_number", None):
                modal_counts[a.track_number] = modal_counts.get(a.track_number, 0) + 1
        modal_track: Optional[str] = None
        if modal_counts:
            modal_track = max(modal_counts.items(), key=lambda x: x[1])[0]
    except Exception:
        modal_track = None

    target_dow = scheduled_time.weekday()
    target_hour = scheduled_time.hour
    target_minute = scheduled_time.minute
    target_service_type = detect_service_type(trip_headsign)
    target_is_weekend = is_weekend_service(target_dow)

    combined_scores: Dict[str, float] = defaultdict(float)
    sample_counts: Dict[str, int] = defaultdict(int)

    # First pass: accumulate pattern buckets (kept similar to prior code for debug)
    for assignment in assignment_list:
        if not getattr(assignment, "track_number", None):
            continue
        try:
            headsign_similarity = enhanced_headsign_similarity(
                trip_headsign, getattr(assignment, "headsign", "") or ""
            )
        except Exception:
            headsign_similarity = 0.0

        assignment_service_type = detect_service_type(
            getattr(assignment, "headsign", "") or ""
        )
        assignment_is_weekend = is_weekend_service(
            getattr(assignment, "day_of_week", 0)
        )

        # time difference in minutes
        try:
            time_diff_minutes = int(
                abs((assignment.scheduled_time - scheduled_time).total_seconds()) / 60
            )
        except Exception:
            # Fallback to hour/minute if scheduled_time missing
            try:
                time_diff_minutes = abs(
                    getattr(assignment, "hour", 0) * 60
                    + getattr(assignment, "minute", 0)
                    - target_hour * 60
                    - target_minute
                )
            except Exception:
                time_diff_minutes = 9999

        # Exact-ish match (enhanced similarity + time + direction)
        if (
            headsign_similarity > 0.6
            and getattr(assignment, "direction_id", None) == direction_id
            and time_diff_minutes <= 60
        ):
            score = int(15 * headsign_similarity)
            patterns["exact_match"][assignment.track_number] += int(score)

        # Headsign match (lower threshold)
        if (
            headsign_similarity > 0.5
            and getattr(assignment, "direction_id", None) == direction_id
        ):
            score = int(8 * headsign_similarity)
            patterns["headsign_match"][assignment.track_number] += int(score)

        if time_diff_minutes <= 30:
            patterns["time_match_30"][assignment.track_number] += 5
        elif time_diff_minutes <= 60:
            patterns["time_match_60"][assignment.track_number] += 3
        elif time_diff_minutes <= 120:
            patterns["time_match_120"][assignment.track_number] += 2

        if getattr(assignment, "direction_id", None) == direction_id:
            patterns["direction_match"][assignment.track_number] += 2

        if target_service_type == assignment_service_type:
            patterns["service_type_match"][assignment.track_number] += 3

        if target_is_weekend == assignment_is_weekend:
            patterns["weekend_pattern_match"][assignment.track_number] += 2

        if getattr(assignment, "day_of_week", None) == target_dow:
            patterns["day_of_week_match"][assignment.track_number] += 1

        if modal_track and getattr(assignment, "track_number", None) == modal_track:
            patterns["platform_consistency"][assignment.track_number] += 4

    # Second pass: compute combined_scores with time decay and graduated scoring
    for assignment in assignment_list:
        if not getattr(assignment, "track_number", None):
            continue

        try:
            days_old = (scheduled_time - assignment.scheduled_time).days
            time_decay = max(0.1, 1.0 - (days_old / 90.0))
        except Exception:
            time_decay = 0.1

        try:
            headsign_similarity = enhanced_headsign_similarity(
                trip_headsign, getattr(assignment, "headsign", "") or ""
            )
        except Exception:
            headsign_similarity = 0.0

        assignment_service_type = detect_service_type(
            getattr(assignment, "headsign", "") or ""
        )
        assignment_is_weekend = is_weekend_service(
            getattr(assignment, "day_of_week", 0)
        )

        try:
            time_diff_minutes = int(
                abs((assignment.scheduled_time - scheduled_time).total_seconds()) / 60
            )
        except Exception:
            try:
                time_diff_minutes = abs(
                    getattr(assignment, "hour", 0) * 60
                    + getattr(assignment, "minute", 0)
                    - target_hour * 60
                    - target_minute
                )
            except Exception:
                time_diff_minutes = 9999

        base_score = 0.0

        if (
            headsign_similarity > 0.6
            and getattr(assignment, "direction_id", None) == direction_id
            and time_diff_minutes <= 60
        ):
            base_score = 15 * headsign_similarity
        elif (
            headsign_similarity > 0.5
            and getattr(assignment, "direction_id", None) == direction_id
        ):
            base_score = 8 * headsign_similarity
        elif time_diff_minutes <= 30:
            base_score = 5.0
        elif time_diff_minutes <= 60:
            base_score = 3.0
        elif time_diff_minutes <= 120:
            base_score = 2.0
        elif getattr(assignment, "direction_id", None) == direction_id:
            base_score = 2.0
        elif target_is_weekend == assignment_is_weekend:
            base_score = 2.0
        elif getattr(assignment, "day_of_week", None) == target_dow:
            base_score = 1.0

        # Service type bonus
        if target_service_type == assignment_service_type and base_score > 0:
            base_score = base_score * 1.2

        if base_score > 0:
            t = assignment.track_number
            combined_scores[t] += base_score * time_decay
            sample_counts[t] += 1

    return (
        dict(combined_scores),
        dict(sample_counts),
        {k: dict(v) for k, v in patterns.items()},
    )


async def compute_final_probabilities(
    combined_scores: Dict[str, float],
    sample_counts: Dict[str, int],
    accuracy_lookup: Callable[[str], Awaitable[float]],
) -> Dict[str, float]:
    """
    Convert aggregated combined_scores and sample_counts into a normalized
    probability distribution that accounts for sample-size confidence and
    historical accuracy.

    - combined_scores: pre-normalized float scores per track
    - sample_counts: number of historical samples contributing to each track
    - accuracy_lookup: async callable track_str -> float (0..1), used to fetch historical accuracy

    Returns a dict mapping track string to final probability (sums to 1.0).
    """
    if not combined_scores:
        return {}

    total_score = sum(combined_scores.values())
    if total_score <= 0:
        return {}

    adjusted: Dict[str, float] = {}
    # For each track, compute base_prob and an adjustment factor from sample size + accuracy
    for track, score in combined_scores.items():
        base_prob = score / total_score
        sample_size = sample_counts.get(track, 0)
        sample_confidence = min(1.0, sample_size / 10.0)
        # obtain historical accuracy (async)
        try:
            accuracy = float(await accuracy_lookup(track))
        except Exception:
            accuracy = 0.7  # conservative default if lookup fails

        confidence_factor = 0.3 + 0.4 * sample_confidence + 0.3 * accuracy
        adjusted_prob = base_prob * confidence_factor
        adjusted[track] = adjusted_prob

    adj_total = sum(adjusted.values())
    if adj_total <= 0:
        return {}

    # Normalize
    normalized = {track: prob / adj_total for track, prob in adjusted.items()}
    return normalized
