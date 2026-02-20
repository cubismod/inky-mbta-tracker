import json
from datetime import datetime
from types import TracebackType
from zoneinfo import ZoneInfo

import pytest
from healthcheck import check_vehicles_endpoint, is_service_hours


class DummyResponse:
    def __init__(self, payload: str) -> None:
        self._payload = payload.encode("utf-8")

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


def test_is_service_hours_weekday_window() -> None:
    ny_tz = ZoneInfo("America/New_York")
    assert is_service_hours(datetime(2026, 2, 16, 5, 0, tzinfo=ny_tz))
    assert not is_service_hours(datetime(2026, 2, 16, 4, 59, tzinfo=ny_tz))
    assert is_service_hours(datetime(2026, 2, 16, 0, 10, tzinfo=ny_tz))
    assert not is_service_hours(datetime(2026, 2, 16, 0, 40, tzinfo=ny_tz))


def test_is_service_hours_friday_saturday_extension() -> None:
    ny_tz = ZoneInfo("America/New_York")
    assert is_service_hours(datetime(2026, 2, 14, 1, 30, tzinfo=ny_tz))
    assert is_service_hours(datetime(2026, 2, 14, 2, 0, tzinfo=ny_tz))
    assert not is_service_hours(datetime(2026, 2, 14, 2, 1, tzinfo=ny_tz))


def test_check_vehicles_endpoint_empty_features_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = json.dumps({"features": []})

    def fake_urlopen(url: str, timeout: float = 5):
        return DummyResponse(payload)

    monkeypatch.setattr("healthcheck.urlopen", fake_urlopen)

    now = datetime(2026, 2, 16, 10, 0, tzinfo=ZoneInfo("America/New_York"))
    assert not check_vehicles_endpoint("http://example.com/vehicles", now=now)


def test_check_vehicles_endpoint_non_empty_features_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = json.dumps({"features": [{"id": 1}]})

    def fake_urlopen(url: str, timeout: float = 5):
        return DummyResponse(payload)

    monkeypatch.setattr("healthcheck.urlopen", fake_urlopen)

    now = datetime(2026, 2, 16, 10, 0, tzinfo=ZoneInfo("America/New_York"))
    assert check_vehicles_endpoint("http://example.com/vehicles", now=now)


def test_check_vehicles_endpoint_skips_outside_service_hours(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(url: str, timeout: float = 5):
        raise AssertionError("urlopen should not be called outside service hours")

    monkeypatch.setattr("healthcheck.urlopen", fake_urlopen)

    now = datetime(2026, 2, 16, 3, 0, tzinfo=ZoneInfo("America/New_York"))
    assert check_vehicles_endpoint("http://example.com/vehicles", now=now)
