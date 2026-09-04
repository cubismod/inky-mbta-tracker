from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel

# ------------------------------------------------------------------------
# Vehicle counts models
#
# These models describe the typed JSON response for the "vehicle counts"
# endpoint. The UI expects counts grouped by vehicle type (light rail,
# heavy rail, regional rail, bus) across MBTA line groups:
# RL (Red Line), GL (Green Line), BL (Blue Line), OL (Orange Line),
# SL (Silver Line), CR (Commuter Rail).
# ``
# Each VehicleLineTotals holds counts for each line plus a row total.
# VehicleCountsByType groups those rows by vehicle type. TotalsByLine
# provides column totals and an overall total.
# ------------------------------------------------------------------------


class VehicleLineTotals(BaseModel):
    """Counts for a single vehicle type broken down by line."""

    RL: int = 0
    GL: int = 0
    BL: int = 0
    OL: int = 0
    SL: int = 0
    CR: int = 0
    total: int = 0


class VehicleCountsByType(BaseModel):
    """Rows for each vehicle type (used to render table rows)."""

    light_rail: VehicleLineTotals
    heavy_rail: VehicleLineTotals
    regional_rail: VehicleLineTotals
    bus: VehicleLineTotals


class TotalsByLine(BaseModel):
    """Column totals (per line) and overall total."""

    RL: int = 0
    GL: int = 0
    BL: int = 0
    OL: int = 0
    SL: int = 0
    CR: int = 0
    total: int = 0


class VehiclesCountResponse(BaseModel):
    """Typed response for the vehicle counts endpoint."""

    success: bool
    counts: VehicleCountsByType
    totals_by_line: TotalsByLine
    generated_at: datetime


class ErrorResponse(BaseModel):
    """Error body returned by HTTPException-raise validators (matches FastAPI's {detail: ...} shape)."""

    detail: str


class GeoJSONFeatureCollection(BaseModel):
    """OpenAPI schema for GeoJSON FeatureCollection responses (vehicles, shapes)."""

    type: str = "FeatureCollection"
    features: list[dict[str, Any]]


class DepartureStop(BaseModel):
    """Stop the departures were requested for.

    `name` is only populated when MBTA inlines the matching stop resource
    (parent-station queries return platform stops instead)."""

    id: str
    name: Optional[str] = None


class Departure(BaseModel):
    """A single upcoming departure, enriched with tracker data."""

    trip_id: Optional[str] = None
    route_id: str
    route_type: Optional[int] = None
    direction_id: Optional[int] = None
    headsign: Optional[str] = None
    arrival_time: Optional[datetime] = None
    departure_time: Optional[datetime] = None
    status: Optional[str] = None
    alerting: bool = False
    bikes_allowed: bool = False


class DeparturesResponse(BaseModel):
    """Typed response for GET /predictions/departures."""

    stop: DepartureStop
    departures: list[Departure]


class HistoricalSnapshot(BaseModel):
    """A snapshot of vehicle positions recorded at a single point in time."""

    timestamp: str
    vehicles: dict[str, Any]


class HistoricalVehiclesResponse(BaseModel):
    """Typed response for GET /historical/vehicles."""

    snapshots: list[HistoricalSnapshot]


class HistoricalVehicleCountSnapshot(BaseModel):
    """Vehicle counts computed from a single historical snapshot."""

    timestamp: str
    counts: VehicleCountsByType
    totals_by_line: TotalsByLine


class HistoricalVehicleCountsResponse(BaseModel):
    """Typed response for GET /historical/vehicles/counts."""

    snapshots: list[HistoricalVehicleCountSnapshot]


class LineSpeedStats(BaseModel):
    """Speed statistics for one line group within a single snapshot."""

    avg_speed: float
    min_speed: float
    max_speed: float
    vehicle_count: int


class HistoricalVehicleSpeedSnapshot(BaseModel):
    """Speed statistics computed from a single historical snapshot."""

    timestamp: str
    lines: dict[str, LineSpeedStats]


class HistoricalVehicleSpeedsResponse(BaseModel):
    """Typed response for GET /historical/vehicles/speeds."""

    snapshots: list[HistoricalVehicleSpeedSnapshot]
