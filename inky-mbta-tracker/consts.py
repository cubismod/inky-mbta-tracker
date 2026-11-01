MBTA_V3_ENDPOINT = "https://api-v3.mbta.com"
TWO_MONTHS = 5256000
DAY = 86400
HOUR = 3600
YEAR = 31536000
WEEK = 604800
MINUTE = 60
# SSE connections that deliver no events for this long are presumed half-open and
# reconnected; MBTA streams emit events/keepalives far more often than this
SSE_INACTIVITY_TIMEOUT = 30 * MINUTE
ALERTS_SET_KEY = "alerts:stats"
LIVE_NEGATIVE_CACHE_KEY = "live_negative_cache"
VEHICLE_STREAM_KEY = "vehicle_stream_diff"
HISTORICAL_V_DATA = "historical_vehicle_data"

# The MBTA addresses its Silver Line bus routes by numeric route id, but every
# human-facing surface ("Silver Line SL4 Alert", map labels) reads the SL short
# name. This dict is the single source of truth for that mapping so no surface
# can drift and leak a bare number. Grouping by MBTA line id, per
# https://api-v3.mbta.com/routes/<id> relationships.line:
#   741/742/743/746 -> line-SLWaterfront
#   749/751         -> line-SLWashington
SILVER_LINE_ROUTES: dict[str, str] = {
    "741": "SL1",
    "742": "SL2",
    "743": "SL3",
    "746": "SLW",
    "749": "SL5",
    "751": "SL4",
}
# Reverse membership: an id that is already an SL short name.
SILVER_LINE_LABELS = frozenset(SILVER_LINE_ROUTES.values())
# SL short name -> numeric route id, for callers that only know the readable form.
SILVER_LINE_IDS: dict[str, str] = {
    label: route_id for route_id, label in SILVER_LINE_ROUTES.items()
}


def is_silver_line_route(route_id: str) -> bool:
    """True when *route_id* is a numeric Silver Line route id or an SL short name.

    Use this instead of prefix sniffing (``startswith("74")``): MBTA bus routes
    74 and 75 exist and are not Silver Line, while all six Silver routes are
    addressed by exact ids.
    """
    if not route_id:
        return False
    key = route_id.strip().upper()
    return key in SILVER_LINE_ROUTES or key in SILVER_LINE_LABELS


def silver_line_label(route_id: str) -> str:
    """Return the SL short name for a Silver Line route id, else *route_id* as given."""
    return SILVER_LINE_ROUTES.get(route_id.strip().upper(), route_id)


def silver_line_route_id(label: str) -> str | None:
    """Return the numeric MBTA route id for an SL short name ("SL4" -> "751").

    None when *label* is not a known Silver Line short name, so callers can tell
    "unknown" apart from "pass through unchanged".
    """
    return SILVER_LINE_IDS.get(label.strip().upper())
