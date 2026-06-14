from datetime import datetime

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
