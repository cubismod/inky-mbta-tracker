from datetime import UTC, datetime, timedelta

from api.endpoints.schedules import StopStreamConfig, _filter_events
from shared_types.shared_types import ScheduleEvent


def make_event(
    *,
    stop: str,
    route_id: str,
    event_id: str,
    minutes_from_now: int = 30,
    transit_time_min: int = 5,
    show_on_display: bool = True,
) -> ScheduleEvent:
    return ScheduleEvent(
        action="add",
        time=datetime.now(UTC) + timedelta(minutes=minutes_from_now),
        route_id=route_id,
        route_type=1,
        headsign="Test",
        stop=stop,
        id=event_id,
        transit_time_min=transit_time_min,
        trip_id="trip-1",
        alerting=False,
        bikes_allowed=False,
        show_on_display=show_on_display,
    )


def test_filter_events_by_stop_and_route():
    configs = [
        StopStreamConfig(
            stop_name="Place A",
            route_filter="Red",
            direction_filter=0,
            transit_time_min=5,
            schedule_only=False,
            show_on_display=True,
            route_substring_filter=None,
        ),
        StopStreamConfig(
            stop_name="Place B",
            route_filter="CR-Providence",
            direction_filter=1,
            transit_time_min=10,
            schedule_only=True,
            show_on_display=True,
            route_substring_filter="CR",
        ),
    ]

    events = [
        make_event(stop="Place A", route_id="Red", event_id="prediction-1"),
        make_event(stop="Place B", route_id="CR-Providence", event_id="prediction-2"),
        make_event(stop="Place C", route_id="Green", event_id="prediction-3"),
    ]

    filtered = _filter_events(events, configs)

    # Place A should remain (route match, schedule_only False)
    assert any(e.id == "prediction-1" for e in filtered)
    # Place B should be filtered out because schedule_only=True but id is prediction
    assert all(e.id != "prediction-2" for e in filtered)
    # Place C not configured
    assert all(e.stop != "Place C" for e in filtered)


def test_filter_events_respects_transit_buffer():
    configs = [
        StopStreamConfig(
            stop_name="Place A",
            route_filter="Red",
            direction_filter=0,
            transit_time_min=15,
            schedule_only=False,
            show_on_display=True,
            route_substring_filter=None,
        )
    ]

    # Event inside transit buffer should be dropped
    events = [
        make_event(
            stop="Place A",
            route_id="Red",
            event_id="prediction-4",
            minutes_from_now=5,
            transit_time_min=15,
        )
    ]

    assert _filter_events(events, configs) == []

    # Event outside buffer should remain
    far_event = make_event(
        stop="Place A",
        route_id="Red",
        event_id="prediction-5",
        minutes_from_now=25,
        transit_time_min=15,
    )

    assert _filter_events([far_event], configs) == [far_event]
