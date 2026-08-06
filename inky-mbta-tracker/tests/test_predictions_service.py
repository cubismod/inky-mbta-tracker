from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, cast

import pytest
from aiohttp import ClientSession
from api.services import predictions
from redis.asyncio import Redis

PREDICTION_BODY = """
{
  "data": [
    {
      "type": "prediction",
      "id": "prediction-1",
      "relationships": {
        "vehicle": null,
        "stop": {"data": {"type": "stop", "id": "stop-1"}},
        "trip": {"data": {"type": "trip", "id": "trip-1"}},
        "route": {"data": {"type": "route", "id": "Red"}}
      },
      "attributes": {
        "arrival_time": "2026-06-06T12:00:00-04:00",
        "revenue": "REVENUE"
      }
    }
  ]
}
"""

DEPARTURES_BODY = """
{
  "data": [
    {
      "type": "prediction",
      "id": "prediction-1",
      "relationships": {
        "vehicle": null,
        "trip": {"data": {"type": "trip", "id": "trip-1"}},
        "stop": {"data": {"type": "stop", "id": "70070"}},
        "route": {"data": {"type": "route", "id": "Red"}}
      },
      "attributes": {
        "arrival_time": "2026-06-06T12:00:00-04:00",
        "departure_time": "2026-06-06T12:01:00-04:00",
        "direction_id": 0,
        "status": null,
        "revenue": "REVENUE"
      }
    }
  ],
  "included": [
    {
      "type": "trip",
      "id": "trip-1",
      "relationships": {},
      "attributes": {
        "wheelchair_accessible": 1,
        "name": "",
        "headsign": "Ashmont",
        "direction_id": 0,
        "bikes_allowed": 1
      }
    },
    {
      "type": "stop",
      "id": "70070",
      "relationships": {"parent_station": {"data": null}},
      "attributes": {
        "name": "Park Street",
        "location_type": 0,
        "wheelchair_boarding": 1
      }
    },
    {
      "type": "route",
      "id": "Red",
      "relationships": {},
      "links": {"self": "/routes/Red"},
      "attributes": {
        "color": "DA291C",
        "fare_class": "Rapid Transit",
        "sort_order": 10010,
        "short_name": "",
        "long_name": "Red Line",
        "text_color": "FFFFFF",
        "type": 1,
        "description": "Rapid Transit"
      }
    }
  ]
}
"""


@pytest.mark.anyio
async def test_predictions_model_parses_included_resources() -> None:
    from mbta_responses import Predictions, RouteResource, StopResource, TripResource

    parsed = Predictions.model_validate_json(DEPARTURES_BODY, strict=False)

    assert parsed.included is not None
    assert isinstance(parsed.included[0], TripResource)
    assert isinstance(parsed.included[1], StopResource)
    assert isinstance(parsed.included[2], RouteResource)
    assert parsed.included[0].attributes.headsign == "Ashmont"
    assert parsed.included[2].attributes.type == 1


DEPARTURES_UNSORTED_BODY = """
{
  "data": [
    {
      "type": "prediction",
      "id": "prediction-late",
      "relationships": {
        "vehicle": null,
        "trip": {"data": {"type": "trip", "id": "trip-1"}},
        "stop": {"data": {"type": "stop", "id": "70070"}},
        "route": {"data": {"type": "route", "id": "Red"}}
      },
      "attributes": {
        "arrival_time": "2026-06-06T12:10:00-04:00",
        "departure_time": "2026-06-06T12:11:00-04:00",
        "direction_id": 0,
        "revenue": "REVENUE"
      }
    },
    {
      "type": "prediction",
      "id": "prediction-early",
      "relationships": {
        "vehicle": null,
        "trip": {"data": {"type": "trip", "id": "trip-2"}},
        "stop": {"data": {"type": "stop", "id": "70070"}},
        "route": {"data": {"type": "route", "id": "Red"}}
      },
      "attributes": {
        "arrival_time": null,
        "departure_time": "2026-06-06T12:05:00-04:00",
        "direction_id": 0,
        "revenue": "REVENUE"
      }
    }
  ],
  "included": []
}
"""


async def _no_alerts(routes: str, session: Any, r_client: Any) -> None:
    return None


@pytest.mark.anyio
async def test_fetch_stop_departures_maps_included_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        params = kwargs.pop("params", None)
        if params:
            from urllib.parse import urlencode

            url = f"{url}?{urlencode(params)}"
        calls.append(url)
        yield FakeResponse(200, DEPARTURES_BODY)

    monkeypatch.setattr(predictions, "MBTA_AUTH", None)
    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)
    monkeypatch.setattr(predictions, "light_get_alerts_batch", _no_alerts)

    result = await predictions.fetch_stop_departures(
        cast(ClientSession, FakeSession()),
        cast(Redis, None),
        stop="place-pktrm",
        route="Red",
        direction=0,
        limit=5,
    )

    assert result.stop.id == "place-pktrm"
    assert len(result.departures) == 1
    dep = result.departures[0]
    assert dep.trip_id == "trip-1"
    assert dep.route_id == "Red"
    assert dep.route_type == 1
    assert dep.direction_id == 0
    assert dep.headsign == "Ashmont"
    assert dep.bikes_allowed is True
    assert dep.alerting is False
    assert dep.arrival_time == datetime.fromisoformat("2026-06-06T12:00:00-04:00")
    assert dep.departure_time == datetime.fromisoformat("2026-06-06T12:01:00-04:00")
    assert "filter%5Bstop%5D=place-pktrm" in calls[0]
    assert "filter%5Broute%5D=Red" in calls[0]
    assert "filter%5Bdirection_id%5D=0" in calls[0]
    assert "page%5Blimit%5D=5" in calls[0]
    assert "include=trip%2Cstop%2Croute" in calls[0]


@pytest.mark.anyio
async def test_fetch_stop_departures_sorts_by_effective_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        yield FakeResponse(200, DEPARTURES_UNSORTED_BODY)

    monkeypatch.setattr(predictions, "MBTA_AUTH", None)
    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)
    monkeypatch.setattr(predictions, "light_get_alerts_batch", _no_alerts)

    result = await predictions.fetch_stop_departures(
        cast(ClientSession, FakeSession()), cast(Redis, None), stop="place-pktrm"
    )

    assert [d.trip_id for d in result.departures] == ["trip-2", "trip-1"]


@pytest.mark.anyio
async def test_fetch_stop_departures_raises_for_non_200(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        yield FakeResponse(500, "{}")

    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)

    with pytest.raises(predictions.MBTAUpstreamError) as err:
        await predictions.fetch_stop_departures(
            cast(ClientSession, FakeSession()), cast(Redis, None), stop="place-pktrm"
        )

    assert err.value.status_code == 500


def _alert(alert_id: str, entities: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "type": "alert",
        "id": alert_id,
        "attributes": {
            "cause": "MAINTENANCE",
            "created_at": "2026-06-06T10:00:00-04:00",
            "header": f"header-{alert_id}",
            "short_header": f"short-{alert_id}",
            "updated_at": "2026-06-06T10:00:00-04:00",
            "active_period": [],
            "severity": 3,
            "informed_entity": entities,
        },
    }


def _departures_patch(
    monkeypatch: pytest.MonkeyPatch,
    alerts: list[dict[str, Any]] | None,
    captured_routes: list[str] | None = None,
) -> None:
    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        yield FakeResponse(200, DEPARTURES_BODY)

    async def fake_alerts(routes: str, session: Any, r_client: Any) -> Any:
        if captured_routes is not None:
            captured_routes.append(routes)
        return alerts

    monkeypatch.setattr(predictions, "MBTA_AUTH", None)
    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)
    monkeypatch.setattr(predictions, "light_get_alerts_batch", fake_alerts)


@pytest.mark.anyio
async def test_fetch_stop_departures_marks_alerting_for_matching_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[str] = []
    _departures_patch(monkeypatch, [_alert("a1", [{"route": "Red"}])], captured)

    result = await predictions.fetch_stop_departures(
        cast(ClientSession, FakeSession()), cast(Redis, None), stop="place-pktrm"
    )

    assert result.departures[0].alerting is True
    assert captured == ["Red"]


@pytest.mark.anyio
async def test_fetch_stop_departures_marks_alerting_for_matching_trip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _departures_patch(monkeypatch, [_alert("a1", [{"route": "Red", "trip": "trip-1"}])])

    result = await predictions.fetch_stop_departures(
        cast(ClientSession, FakeSession()), cast(Redis, None), stop="place-pktrm"
    )

    assert result.departures[0].alerting is True


@pytest.mark.anyio
async def test_fetch_stop_departures_skips_direction_specific_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _departures_patch(
        monkeypatch, [_alert("a1", [{"route": "Red", "direction_id": 1}])]
    )

    result = await predictions.fetch_stop_departures(
        cast(ClientSession, FakeSession()), cast(Redis, None), stop="place-pktrm"
    )

    assert result.departures[0].alerting is False


@pytest.mark.anyio
async def test_fetch_stop_departures_skips_other_route_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _departures_patch(monkeypatch, [_alert("a1", [{"route": "Blue"}])])

    result = await predictions.fetch_stop_departures(
        cast(ClientSession, FakeSession()), cast(Redis, None), stop="place-pktrm"
    )

    assert result.departures[0].alerting is False


@pytest.mark.anyio
async def test_fetch_stop_departures_survives_alerts_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _departures_patch(monkeypatch, None)

    result = await predictions.fetch_stop_departures(
        cast(ClientSession, FakeSession()), cast(Redis, None), stop="place-pktrm"
    )

    assert len(result.departures) == 1
    assert result.departures[0].alerting is False


class FakeResponse:
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self._body = body

    async def text(self) -> str:
        return self._body


class FakeSession:
    closed = False


async def _null_cache(*args: Any, **kwargs: Any) -> None:
    return None


@pytest.mark.anyio
async def test_batch_fetch_returns_predicted_arrival_times(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api_calls: list[str] = []

    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        params = kwargs.pop("params", None)
        if params:
            from urllib.parse import urlencode

            url = f"{url}?{urlencode(params)}"
        api_calls.append(url)
        yield FakeResponse(200, PREDICTION_BODY)

    monkeypatch.setattr(predictions, "get_cache", _null_cache)
    monkeypatch.setattr(predictions, "write_cache", _null_cache)
    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)

    result = await predictions.batch_fetch_trip_predictions(
        cast(ClientSession, None), cast(Redis, None), ["stop-1"]
    )

    assert len(result) == 1
    assert ("trip-1", "stop-1") in result
    assert isinstance(result[("trip-1", "stop-1")], datetime)
    assert result[("trip-1", "stop-1")] == datetime.fromisoformat(
        "2026-06-06T12:00:00-04:00"
    )
    assert len(api_calls) == 1
    assert "filter%5Bstop%5D" in api_calls[0]
    assert "stop-1" in api_calls[0]


@pytest.mark.anyio
async def test_batch_fetch_handles_api_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        yield FakeResponse(500, "{}")

    monkeypatch.setattr(predictions, "get_cache", _null_cache)
    monkeypatch.setattr(predictions, "write_cache", _null_cache)
    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)

    result = await predictions.batch_fetch_trip_predictions(
        cast(ClientSession, None), cast(Redis, None), ["stop-1"]
    )

    assert result == {}


@pytest.mark.anyio
async def test_batch_fetch_uses_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api_calls: list[str] = []

    @asynccontextmanager
    async def fake_rate_limited_get(
        session: ClientSession, redis: Redis, url: str, **kwargs: Any
    ) -> AsyncIterator[FakeResponse]:
        api_calls.append(url)
        yield FakeResponse(200, PREDICTION_BODY)

    monkeypatch.setattr(predictions, "get_cache", _null_cache)
    monkeypatch.setattr(predictions, "rate_limited_get", fake_rate_limited_get)

    async def fake_get_cache(redis: Redis, key: str) -> str | None:
        return PREDICTION_BODY

    monkeypatch.setattr(predictions, "get_cache", fake_get_cache)

    result = await predictions.batch_fetch_trip_predictions(
        cast(ClientSession, None), cast(Redis, None), ["stop-1", "stop-1"]
    )

    assert ("trip-1", "stop-1") in result
    assert result[("trip-1", "stop-1")] == datetime.fromisoformat(
        "2026-06-06T12:00:00-04:00"
    )
    assert len(api_calls) == 0
