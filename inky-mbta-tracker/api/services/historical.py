import logging
from datetime import UTC, datetime
from statistics import fmean
from typing import Any

import orjson
from api.models import (
    HistoricalSnapshot,
    HistoricalVehicleCountSnapshot,
    HistoricalVehicleCountsResponse,
    HistoricalVehicleSpeedSnapshot,
    HistoricalVehicleSpeedsResponse,
    HistoricalVehiclesResponse,
    LineSpeedStats,
)
from consts import HISTORICAL_V_DATA
from mbta_client_extended import silver_line_lookup
from opentelemetry import trace
from otel_utils import add_span_attributes, add_transaction_ids_to_span
from redis.asyncio import Redis
from vehicle_counting import _classify_route, count_routes

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


def _format_timestamp(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


async def fetch_historical_snapshots(r_client: Redis) -> HistoricalVehiclesResponse:
    with tracer.start_as_current_span(
        "api.services.fetch_historical_snapshots"
    ) as span:
        add_transaction_ids_to_span(span)
        raw = await r_client.hgetall(HISTORICAL_V_DATA)  # type: ignore[misc]
        snapshots: list[HistoricalSnapshot] = []
        for ts_bytes, body_bytes in raw.items():
            snapshots.append(
                HistoricalSnapshot(
                    timestamp=_format_timestamp(float(ts_bytes)),
                    vehicles=orjson.loads(body_bytes),
                )
            )
        snapshots.sort(key=lambda snapshot: snapshot.timestamp)
        add_span_attributes(
            span,
            {
                "historical.snapshots.count": len(snapshots),
                "historical.fetch.status": "success",
            },
        )
        return HistoricalVehiclesResponse(snapshots=snapshots)


async def fetch_historical_vehicle_counts(
    r_client: Redis,
) -> HistoricalVehicleCountsResponse:
    with tracer.start_as_current_span(
        "api.services.fetch_historical_vehicle_counts"
    ) as span:
        add_transaction_ids_to_span(span)
        raw = await r_client.hgetall(HISTORICAL_V_DATA)  # type: ignore[misc]
        snapshots: list[HistoricalVehicleCountSnapshot] = []
        for ts_bytes, body_bytes in raw.items():
            vehicles: dict[str, dict] = orjson.loads(body_bytes)
            routes = [
                str(vehicle.get("properties", {}).get("route") or "")
                for vehicle in vehicles.values()
            ]
            counts, totals_by_line = count_routes(routes)
            snapshots.append(
                HistoricalVehicleCountSnapshot(
                    timestamp=_format_timestamp(float(ts_bytes)),
                    counts=counts,
                    totals_by_line=totals_by_line,
                )
            )
        snapshots.sort(key=lambda snapshot: snapshot.timestamp)
        add_span_attributes(
            span,
            {
                "historical.snapshots.count": len(snapshots),
                "historical.fetch.status": "success",
            },
        )
        return HistoricalVehicleCountsResponse(snapshots=snapshots)


def _speed_stats_by_line(
    vehicles: dict[str, dict[str, Any]],
) -> dict[str, LineSpeedStats]:
    speeds_by_line: dict[str, list[float]] = {}
    for vehicle in vehicles.values():
        properties: dict[str, Any] = vehicle.get("properties", {})
        speed = properties.get("speed")
        if not isinstance(speed, (int, float)) or speed <= 0:
            continue
        route = str(properties.get("route") or "")
        if route.startswith("74") or route.startswith("75"):
            route = silver_line_lookup(route)
        line, _ = _classify_route(route)
        if not line:
            continue
        speeds_by_line.setdefault(line, []).append(float(speed))
    return {
        line: LineSpeedStats(
            avg_speed=fmean(speeds),
            min_speed=min(speeds),
            max_speed=max(speeds),
            vehicle_count=len(speeds),
        )
        for line, speeds in speeds_by_line.items()
    }


async def fetch_historical_vehicle_speeds(
    r_client: Redis,
) -> HistoricalVehicleSpeedsResponse:
    with tracer.start_as_current_span(
        "api.services.fetch_historical_vehicle_speeds"
    ) as span:
        add_transaction_ids_to_span(span)
        raw = await r_client.hgetall(HISTORICAL_V_DATA)  # type: ignore[misc]
        snapshots: list[HistoricalVehicleSpeedSnapshot] = []
        for ts_bytes, body_bytes in raw.items():
            vehicles: dict[str, dict] = orjson.loads(body_bytes)
            snapshots.append(
                HistoricalVehicleSpeedSnapshot(
                    timestamp=_format_timestamp(float(ts_bytes)),
                    lines=_speed_stats_by_line(vehicles),
                )
            )
        snapshots.sort(key=lambda snapshot: snapshot.timestamp)
        add_span_attributes(
            span,
            {
                "historical.snapshots.count": len(snapshots),
                "historical.fetch.status": "success",
            },
        )
        return HistoricalVehicleSpeedsResponse(snapshots=snapshots)
