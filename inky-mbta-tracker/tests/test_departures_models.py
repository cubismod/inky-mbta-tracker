from datetime import datetime

from api.models import Departure, DeparturesResponse, DepartureStop


def test_departures_response_serializes_expected_shape() -> None:
    resp = DeparturesResponse(
        stop=DepartureStop(id="place-pktrm", name=None),
        departures=[
            Departure(
                trip_id="trip-1",
                route_id="Red",
                route_type=1,
                direction_id=0,
                headsign="Ashmont",
                arrival_time=datetime.fromisoformat("2026-06-06T12:00:00-04:00"),
                departure_time=datetime.fromisoformat("2026-06-06T12:01:00-04:00"),
                status=None,
                alerting=False,
                bikes_allowed=True,
            )
        ],
    )

    body = resp.model_dump()
    assert body["stop"] == {"id": "place-pktrm", "name": None}
    assert body["departures"][0]["route_type"] == 1
    assert body["departures"][0]["alerting"] is False
