from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class ScheduleEvent(BaseModel):
    action: str
    time: datetime
    route_id: str
    route_type: int
    headsign: str
    stop: str
    id: str
    transit_time_min: int
    trip_id: Optional[str] = None
    alerting: bool = False
    bikes_allowed: bool = False
    # Track prediction fields
    track_number: Optional[str] = None
    track_confidence: Optional[float] = None
    show_on_display: bool = True


class VehicleRedisSchema(BaseModel):
    action: str
    id: str
    current_status: str
    direction_id: int
    latitude: float
    longitude: float
    speed: Optional[float] = 0
    stop: Optional[str] = None
    route: str
    update_time: datetime
    approximate_speed: bool = False
    occupancy_status: Optional[str] = None
    carriages: Optional[list[str]] = None
    headsign: Optional[str] = None


class TrackAssignmentType(Enum):
    HISTORICAL = "historical"
    PREDICTED = "predicted"


class TrackAssignment(BaseModel):
    """Historical track assignment data for analysis"""

    station_id: str
    route_id: str
    trip_id: str
    headsign: str
    direction_id: int
    assignment_type: TrackAssignmentType
    track_number: Optional[str] = None
    scheduled_time: datetime
    actual_time: Optional[datetime] = None
    recorded_time: datetime
    day_of_week: int  # 0=Monday, 6=Sunday
    hour: int
    minute: int


class TrackPrediction(BaseModel):
    """Track prediction for a specific trip"""

    station_id: str
    route_id: str
    trip_id: str
    headsign: str
    direction_id: int
    scheduled_time: datetime
    track_number: Optional[str] = None
    confidence_score: float  # 0.0 to 1.0
    prediction_method: str  # e.g., "historical_pattern", "time_based", "headsign_based"
    historical_matches: int  # Number of historical matches used for prediction
    created_at: datetime


class TrackPredictionStats(BaseModel):
    """Statistics for track predictions to monitor accuracy"""

    station_id: str
    route_id: str
    total_predictions: int
    correct_predictions: int
    accuracy_rate: float
    last_updated: datetime
    prediction_counts_by_track: dict[str, int]  # track -> count
    average_confidence: float


class TaskType(Enum):
    OTHER = -1
    SCHEDULE_PREDICTIONS = 0
    SCHEDULES = 1
    VEHICLES = 2
    PROCESSOR = 3
    OLLAMA_QUEUE_WORKER = 4
    LIGHT_STOP = 4
    TRACK_PREDICTIONS = 6
    REDIS_BACKUP = 7
    VEHICLES_BACKGROUND_WORKER = 8
