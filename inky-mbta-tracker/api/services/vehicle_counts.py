import logging
from datetime import datetime
from typing import Optional

import orjson
from api.models import TotalsByLine, VehicleCountsByType, VehicleLineTotals
from config import Config
from mbta_client_extended import silver_line_lookup
from prometheus import redis_commands
from redis.asyncio import Redis
from shared_types.shared_types import VehicleRedisSchema

logger = logging.getLogger(__name__)

LINES = ("RL", "GL", "BL", "OL", "SL", "CR")

_SILVER_NUMERIC_PREFIXES = ("741", "742", "743", "746", "749", "751")


def _classify_route(route: str) -> tuple[Optional[str], Optional[str]]:
    """Classify a route id into a (line, vehicle_type) bucket.

    Returns (None, None) for routes that are not part of a tracked line.
    """
    route_lower = (route or "").strip().lower()

    if route_lower.startswith("mattapan") or route_lower.startswith("red"):
        line: Optional[str] = "RL"
    elif route_lower.startswith("green"):
        line = "GL"
    elif route_lower.startswith("blue"):
        line = "BL"
    elif route_lower.startswith("orange"):
        line = "OL"
    elif (
        route_lower.startswith("sl")
        or "silver" in route_lower
        or any(route_lower.startswith(p) for p in _SILVER_NUMERIC_PREFIXES)
    ):
        line = "SL"
    elif (
        route_lower.startswith("cr")
        or route_lower.startswith("commuter")
        or route_lower == "commuter rail"
    ):
        line = "CR"
    else:
        return None, None

    if route_lower.startswith("mattapan") or route_lower.startswith("green"):
        vtype: Optional[str] = "light_rail"
    elif (
        route_lower.startswith("cr")
        or route_lower.startswith("commuter")
        or route_lower == "commuter rail"
    ):
        vtype = "regional_rail"
    elif (
        route_lower.startswith("sl")
        or "silver" in route_lower
        or any(route_lower.startswith(p) for p in _SILVER_NUMERIC_PREFIXES)
        or route_lower.isdecimal()
        or route_lower.startswith("7")
    ):
        vtype = "bus"
    elif line in ("RL", "BL", "OL"):
        vtype = "heavy_rail"
    else:
        vtype = None

    return line, vtype


def _empty_counts() -> VehicleCountsByType:
    return VehicleCountsByType(
        light_rail=VehicleLineTotals(),
        heavy_rail=VehicleLineTotals(),
        regional_rail=VehicleLineTotals(),
        bus=VehicleLineTotals(),
    )


def _totals_by_line(counts: VehicleCountsByType) -> TotalsByLine:
    rows = (
        counts.light_rail,
        counts.heavy_rail,
        counts.regional_rail,
        counts.bus,
    )
    totals = TotalsByLine()
    for row in rows:
        for col in LINES:
            setattr(totals, col, getattr(totals, col) + getattr(row, col))
        totals.total += row.total
    return totals


async def get_vehicle_route_counts(
    r_client: Redis, config: Config, frequent_buses: bool = False
) -> tuple[VehicleCountsByType, TotalsByLine]:
    """Tally vehicles by line and vehicle type directly from Redis, without
    building GeoJSON features.

    Pipeline-GETs every member of the ``pos-data`` set, validates each payload
    into a ``VehicleRedisSchema`` exactly once, applies the frequent-bus filter,
    and classifies routes into the RL/GL/BL/OL/SL/CR line columns and the
    light/heavy/regional-rail/bus type rows.

    Stale keys (expired GETs) are pruned from the ``pos-data`` set, mirroring
    ``get_vehicle_features``.
    """
    counts = _empty_counts()

    vehicle_keys: list[bytes] = list(await r_client.smembers("pos-data"))  # type: ignore[misc]
    redis_commands.labels("smembers").inc()
    if not vehicle_keys:
        return counts, _totals_by_line(counts)

    results: list[bytes | None] = await r_client.mget(*vehicle_keys)
    redis_commands.labels("mget").inc()

    frequent_lines = config.frequent_bus_lines
    for result in results:
        if not result:
            continue
        try:
            raw = orjson.loads(result)
        except orjson.JSONDecodeError:
            continue
        raw["update_time"] = datetime.fromisoformat(raw["update_time"])
        vehicle_info = VehicleRedisSchema.model_construct(**raw)
        route = vehicle_info.route
        if frequent_lines:
            if frequent_buses and route not in frequent_lines:
                continue
            if not frequent_buses and route in frequent_lines:
                continue
        if route.startswith("74") or route.startswith("75"):
            route = silver_line_lookup(route)
        line, vtype = _classify_route(route)
        if not line or not vtype:
            continue
        row = getattr(counts, vtype)
        setattr(row, line, getattr(row, line) + 1)
        row.total += 1

    return counts, _totals_by_line(counts)
