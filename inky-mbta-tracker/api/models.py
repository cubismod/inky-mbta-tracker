from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from config import StopSetup
from pydantic import BaseModel, Field
from shared_types.shared_types import DepartureInfo, TrackPrediction
from track_predictor.track_predictor import TrackPredictionStats


class SummaryFormat(str, Enum):
    TEXT = "text"
    MARKDOWN = "markdown"
    JSON = "json"


class TrackPredictionResponse(BaseModel):
    success: bool
    prediction: TrackPrediction | str


class TrackPredictionStatsResponse(BaseModel):
    success: bool
    stats: TrackPredictionStats | str


class PredictionRequest(BaseModel):
    station_id: str
    route_id: str
    trip_id: str
    headsign: str
    direction_id: int
    scheduled_time: datetime


class ChainedPredictionsRequest(BaseModel):
    predictions: List[PredictionRequest]


class ChainedPredictionsResponse(BaseModel):
    results: List[TrackPredictionResponse]


class DepartureWithPrediction(BaseModel):
    departure_info: DepartureInfo
    prediction: Optional[TrackPrediction]


class DatePredictionsRequest(BaseModel):
    target_date: datetime


class DatePredictionsResponse(BaseModel):
    success: bool
    departures: List[DepartureWithPrediction] | str
    note: Optional[str] = None


class MLDiagnosticResult(BaseModel):
    """Diagnostic information for a single ML/pattern prediction comparison."""

    station_id: str
    route_id: str
    trip_id: str
    scheduled_time: datetime
    prediction_method: str
    predicted_track: Optional[str]
    confidence_score: float
    ml_prediction: Optional[str]
    ml_confidence: Optional[float]
    pattern_prediction: Optional[str]
    pattern_confidence: Optional[float]
    ml_result_data: Optional[Dict[str, Any]]  # Raw ML result for debugging
    created_at: datetime


class MLDiagnosticsResponse(BaseModel):
    """Response containing recent ML/pattern diagnostic results."""

    success: bool
    results: List[MLDiagnosticResult] | str
    total_count: int
    ml_enabled: bool
    ml_compare_enabled: bool


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


class LiveSchedulesRequest(BaseModel):
    """Request model for live schedule streaming endpoint."""

    stops: List[StopSetup] = Field(
        ...,
        min_length=1,
        max_length=5,
        description="List of stop configurations to monitor (max 5 stops)",
    )
