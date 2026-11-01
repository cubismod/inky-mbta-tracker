import pytest
from config import Config
from mbta_responses import ActivePeriod, AlertAttributes, AlertResource, InformedEntity
from webhook.discord_webhook import create_webhook_object
from webhook.helpers import (
    PendingBatchItem,
    build_grouped_webhook,
    display_route_label,
    display_route_labels,
)


def _alert(routes: list[str]) -> AlertResource:
    return AlertResource(
        type="alert",
        id="a1",
        attributes=AlertAttributes(
            cause="UNKNOWN_CAUSE",
            created_at="2026-06-27T12:00:00-04:00",
            header="Delay",
            short_header="Delay",
            updated_at="2026-06-27T12:00:00-04:00",
            active_period=[ActivePeriod(start=None, end=None)],
            informed_entity=[
                InformedEntity(route=route, route_type=3) for route in routes
            ],
            severity=5,
        ),
    )


@pytest.mark.parametrize(
    ("route", "expected"),
    [
        ("Orange", "Orange Line"),
        ("Red", "Red Line"),
        ("Blue", "Blue Line"),
        ("Mattapan", "Mattapan Line"),
        ("Green-B", "Green Line"),
        ("Green-E", "Green Line"),
        ("741", "Silver Line SL1"),
        ("743", "Silver Line SL3"),
        ("746", "Silver Line SLW"),
        ("749", "Silver Line SL5"),
        ("751", "Silver Line SL4"),
        ("CR-Providence", "Providence Line"),
        ("39", "39"),
        ("", ""),
    ],
)
def test_display_route_label(route: str, expected: str) -> None:
    assert display_route_label(route) == expected


def test_display_route_labels_converts_every_silver_route() -> None:
    # Comma-joined route ids used to break the lookup and leak the raw number.
    assert display_route_labels(["741", "751"]) == "Silver Line SL1, Silver Line SL4"


def test_display_route_labels_dedupes_and_handles_empty() -> None:
    assert display_route_labels(["Green-B", "Green-C"]) == "Green Line"
    assert display_route_labels([]) == ""


@pytest.mark.parametrize(
    ("route", "expected"),
    [
        ("Orange", "Orange Line Alert"),
        ("751", "Silver Line SL4 Alert"),
        ("39", "39 Alert"),
    ],
)
def test_alert_title_names_the_line(route: str, expected: str) -> None:
    webhook = create_webhook_object(
        _alert([route]), [route], 0xFFFFFF, Config(stops=[])
    )
    assert webhook.embeds
    author = webhook.embeds[0].author
    assert author is not None
    assert author.name == expected


def test_grouped_rows_use_converted_labels() -> None:
    alert = _alert(["751"])
    item = PendingBatchItem(
        webhook_id="a1",
        webhook_json=create_webhook_object(
            alert, ["751"], 0xFFFFFF, Config(stops=[])
        ).model_dump_json(),
        message_hash="m1",
        updated_at=1_700_000_000,
        created_at="2026-06-27T12:00:00-04:00",
        routes=["751"],
    )
    grouped = build_grouped_webhook([item], Config(stops=[]))
    assert grouped.embeds
    fields = grouped.embeds[0].fields
    assert fields
    assert "Silver Line SL4" in fields[0].name
