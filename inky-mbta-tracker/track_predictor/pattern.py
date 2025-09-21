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
    List,
    Optional,
)

from shared_types.shared_types import TrackAssignment

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
    assignments: List[TrackAssignment],
    route_id: str,
    trip_headsign: str,
    direction_id: int,
    scheduled_time: datetime,
):
    """
    Score historical assignments for a target trip. Scoring is based on similarities between
    TrackAssignment attributes and the target trip's attributes.

    Returns a dict of TrackAssignment IDs and scorings
    """
    target_dow = scheduled_time.weekday()
    target_service_type = detect_service_type(trip_headsign)
    target_is_weekend = is_weekend_service(target_dow)

    combined_scores: Dict[str, float] = defaultdict(float)
    sample_counts: Dict[str, int] = defaultdict(int)

    for assignment in assignments:
        track = assignment.track_number
        if not track:
            continue

        headsign_similarity = float(
            enhanced_headsign_similarity(trip_headsign, assignment.headsign or "")
        )

        time_diff_min = int(
            abs((assignment.scheduled_time - scheduled_time).total_seconds()) / 60
        )

        # Map time diff to a 0..1 score (closer is better)
        if time_diff_min <= 15:
            time_score = 1.0
        elif time_diff_min <= 30:
            time_score = 0.8
        elif time_diff_min <= 60:
            time_score = 0.6
        elif time_diff_min <= 120:
            time_score = 0.3
        else:
            time_score = 0.0

        # Direction and route matches (binary)
        direction_match = 1.0 if assignment.direction_id == direction_id else 0.0
        route_match = 1.0 if assignment.route_id == route_id else 0.0

        # Service type and weekend match bonuses (binary)
        assignment_service_type = detect_service_type(assignment.headsign)
        service_type_match = (
            1.0 if assignment_service_type == target_service_type else 0.0
        )
        assignment_is_weekend = is_weekend_service(assignment.day_of_week)
        weekend_match = 1.0 if assignment_is_weekend == target_is_weekend else 0.0

        # Weighted combination (weights sum to 1.0)
        score = (
            0.5 * headsign_similarity
            + 0.3 * time_score
            + 0.1 * direction_match
            + 0.05 * route_match
            + 0.05 * (0.7 * service_type_match + 0.3 * weekend_match)
        )

        # Clamp to [0,1]
        score = max(0.0, min(1.0, score))

        combined_scores[track] += score
        sample_counts[track] += 1

    return dict(combined_scores), dict(sample_counts)


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
        except (TypeError, ValueError, RuntimeError, TimeoutError):
            # If lookup returns non-numeric data or fails in expected runtime ways,
            # fall back to a conservative default accuracy.
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
