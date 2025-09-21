from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel
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
