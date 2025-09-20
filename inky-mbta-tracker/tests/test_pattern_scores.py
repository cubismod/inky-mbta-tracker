from datetime import UTC, datetime

import pytest
from shared_types.shared_types import TrackAssignment, TrackAssignmentType
from track_predictor.pattern import compute_assignment_scores


def make_assignment(
    track: str | None,
    route_id: str,
    headsign: str,
    direction_id: int,
    scheduled_time: datetime,
    day_of_week: int,
) -> TrackAssignment:
    return TrackAssignment(
        station_id="place-sstat",
        route_id=route_id,
        trip_id="test-trip",
        headsign=headsign,
        direction_id=direction_id,
        assignment_type=TrackAssignmentType.HISTORICAL,
        track_number=track,
        scheduled_time=scheduled_time,
        recorded_time=scheduled_time,
        hour=scheduled_time.hour,
        minute=scheduled_time.minute,
        day_of_week=day_of_week,
    )


def test_single_exact_match_score() -> None:
    """A single identical assignment should produce score ~1.0 and count 1."""
    scheduled = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)
    a = make_assignment(
        track="3",
        route_id="CR-Providence",
        headsign="Providence",
        direction_id=0,
        scheduled_time=scheduled,
        day_of_week=0,
    )

    combined, counts = compute_assignment_scores(
        [a], "CR-Providence", "Providence", 0, scheduled
    )

    assert "3" in combined
    assert combined["3"] == pytest.approx(1.0)
    assert counts["3"] == 1


def test_aggregation_and_skip_missing_track() -> None:
    """Assignments without a track_number should be ignored; multiple same-track assignments aggregate."""
    scheduled = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)
    a1 = make_assignment(
        track="3",
        route_id="CR-Providence",
        headsign="Providence",
        direction_id=0,
        scheduled_time=scheduled,
        day_of_week=0,
    )
    a2 = make_assignment(
        track=None,
        route_id="CR-Providence",
        headsign="Providence",
        direction_id=0,
        scheduled_time=scheduled,
        day_of_week=0,
    )

    combined, counts = compute_assignment_scores(
        [a1, a2], "CR-Providence", "Providence", 0, scheduled
    )

    assert "3" in combined
    assert combined["3"] == pytest.approx(1.0)
    assert counts["3"] == 1
    # missing track should not produce a key
    assert None not in combined


def test_preference_for_better_headsign_match() -> None:
    """An assignment with an exact headsign should score higher than one with a mismatched headsign."""
    scheduled = datetime(2024, 1, 15, 9, 30, tzinfo=UTC)
    match = make_assignment(
        track="3",
        route_id="CR-Providence",
        headsign="Providence",
        direction_id=0,
        scheduled_time=scheduled,
        day_of_week=0,
    )
    # different headsign and different route to reduce score
    other = make_assignment(
        track="4",
        route_id="CR-Other",
        headsign="Lowell",
        direction_id=0,
        scheduled_time=scheduled,
        day_of_week=0,
    )

    combined, counts = compute_assignment_scores(
        [match, other], "CR-Providence", "Providence", 0, scheduled
    )

    assert "3" in combined and "4" in combined
    assert combined["3"] > combined["4"]
    assert counts["3"] == 1
    assert counts["4"] == 1
