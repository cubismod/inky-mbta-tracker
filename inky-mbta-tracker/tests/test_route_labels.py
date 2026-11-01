import pytest
from config import Config
from consts import (
    SILVER_LINE_ROUTES,
    is_silver_line_route,
    silver_line_label,
    silver_line_route_id,
)
from geojson_utils import lookup_route_color
from mbta_responses import ActivePeriod, AlertAttributes, AlertResource, InformedEntity
from vehicle_counting import _classify_route, count_routes
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


@pytest.mark.parametrize("route_id", sorted(SILVER_LINE_ROUTES))
def test_no_silver_route_reaches_a_reader_as_a_number(route_id: str) -> None:
    """Guard the whole 74x range: every id renders as its SL short name."""
    label = display_route_label(route_id)
    assert label == f"Silver Line {SILVER_LINE_ROUTES[route_id]}"
    assert route_id not in label


@pytest.mark.parametrize("route_id", sorted(SILVER_LINE_ROUTES))
def test_already_converted_sl_code_stays_readable(route_id: str) -> None:
    sl = SILVER_LINE_ROUTES[route_id]
    assert display_route_label(sl) == f"Silver Line {sl}"


def test_silver_line_predicate_is_exact_not_prefix_based() -> None:
    # MBTA bus routes 74 and 75 are not Silver Line, so the old
    # startswith("74")/("75") sniffing was wrong in principle even when harmless.
    assert is_silver_line_route("751")
    assert is_silver_line_route("SL4")
    assert not is_silver_line_route("74")
    assert not is_silver_line_route("75")
    assert not is_silver_line_route("7501")
    assert not is_silver_line_route("")


@pytest.mark.parametrize("route_id", sorted(SILVER_LINE_ROUTES))
def test_sl_labels_resolve_back_to_numeric_ids(route_id: str) -> None:
    """A reader holding the published label can round-trip to the MBTA id."""
    sl = SILVER_LINE_ROUTES[route_id]
    assert silver_line_route_id(sl) == route_id
    assert silver_line_label(route_id) == sl


def test_unknown_label_has_no_route_id() -> None:
    assert silver_line_route_id("SL9") is None
    assert silver_line_route_id("Red") is None
    assert silver_line_route_id("") is None


@pytest.mark.parametrize("route_id", sorted(SILVER_LINE_ROUTES))
def test_counted_silver_routes_land_in_the_sl_bucket(route_id: str) -> None:
    """Vehicle counting resolves 74x ids to the SL line, not an unknown bucket."""
    counts, totals = count_routes([route_id])
    assert totals.SL == 1
    assert totals.total == 1
    assert counts.bus.SL == 1
    assert _classify_route(route_id) == ("SL", "bus")


@pytest.mark.parametrize("route_id", sorted(SILVER_LINE_ROUTES))
def test_silver_routes_use_the_silver_line_color(route_id: str) -> None:
    """The old color check tested startswith(\"74\") with no \"75\" branch.

    751 (SL4) therefore fell through to the numeric-bus branch and rendered
    bus yellow, while 749 matched \"74\" and happened to come out right.
    """
    assert lookup_route_color(route_id) == "#9A9C9D"
    assert lookup_route_color(SILVER_LINE_ROUTES[route_id]) == "#9A9C9D"


@pytest.mark.parametrize("bus", ["7", "71", "74", "75", "77", "79"])
def test_buses_around_the_silver_range_keep_the_bus_color(bus: str) -> None:
    """Bus 74 and 75 were mis-colored gray by the old prefix check."""
    assert lookup_route_color(bus) == "#FFFF00"
