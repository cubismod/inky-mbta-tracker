from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, TypedDict

from geojson import Feature
from pydantic import BaseModel, Field


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


class MLPredictionRequest(BaseModel):
    id: str
    station_id: str
    route_id: str
    direction_id: int
    scheduled_time: datetime
    trip_id: str
    headsign: str


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
    accuracy: float = 0.0  # historical accuracy estimate for predicted track
    model_confidence: float = (
        0.0  # confidence from model/fused pre-sharpen, pre-history
    )
    display_confidence: float = 0.0  # post-processed confidence for UX
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
    LIGHT_STOP = 4
    TRACK_PREDICTIONS = 6
    REDIS_BACKUP = 7
    VEHICLES_BACKGROUND_WORKER = 8
    ALERTS = 9


class MBTAServiceType(Enum):
    BUS_WEEKDAY = 0
    BUS_WEEKEND = 1
    RAPID_WEEKDAY = 2  # includes silver line
    RAPID_LATE = 3  # late night hours on fridays & saturdays
    RAPID_WEEKEND = 4
    COMMUTER_WEEKDAY = 5
    COMMUTER_WEEKEND = 6
    NO_SERVICE = 9


class SummarizationResponse(BaseModel):
    """Response model for alert summarization"""

    summary: str
    alert_count: int
    model_used: str
    processing_time_ms: float


class SummaryCacheEntry(BaseModel):
    """Cache entry for stored summaries"""

    summary: str
    alert_count: int
    alerts_hash: str
    generated_at: datetime
    config: dict
    ttl: int = 3600  # 1 hour default TTL


class IndividualSummaryCacheEntry(BaseModel):
    """Cache entry for individual alert summaries"""

    alert_id: str
    summary: str
    style: str
    sentence_limit: int
    generated_at: datetime
    format: str
    ttl: int = 3600  # 1 hour default TTL


type ShapeTuple = Tuple[float, float]

type LineRoute = List[List[ShapeTuple]]


class RouteShapes(BaseModel):
    lines: Dict[str, LineRoute]


class LightStop(BaseModel):
    stop_id: str
    long: Optional[float] = None
    lat: Optional[float] = None
    platform_prediction: Optional[str] = None


class DepartureInfo(TypedDict):
    trip_id: str
    station_id: str
    route_id: str
    direction_id: int
    departure_time: str


class PrometheusServerSideMetric(BaseModel):
    name: str = Field(alias="__name__")
    id: str
    instance: str
    job: str


class PrometheusResult(BaseModel):
    metric: PrometheusServerSideMetric
    value: List[float | str]


class PrometheusData(BaseModel):
    result: Optional[List[PrometheusResult]] = None


class PrometheusAPIResponse(BaseModel):
    status: str
    data: PrometheusData


class DiffApiResponse(BaseModel):
    updated: dict[
        str, Feature
    ]  # keys are the vehicle IDs, values are the full updated vehicle object
    removed: Set[str]  # keys are vehicle ids

    class Config:
        arbitrary_types_allowed = True


class VehicleSpeedHistory(BaseModel):
    long: float
    lat: float
    speed: float
    update_time: datetime


class DiscordEmbedFooter(BaseModel):
    text: str
    icon_url: Optional[str] = None
    proxy_icon_url: Optional[str] = None


class DiscordEmbedMedia(BaseModel):
    url: str
    proxy_url: Optional[str] = None
    height: Optional[int] = None
    width: Optional[int] = None


class DiscordEmbedProvider(BaseModel):
    name: Optional[str] = None
    url: Optional[str] = None


class DiscordEmbedAuthor(BaseModel):
    name: str
    url: Optional[str] = None
    icon_url: Optional[str] = None
    proxy_icon_url: Optional[str] = None


class DiscordEmbedField(BaseModel):
    name: str
    value: str
    inline: Optional[bool] = False


class DiscordEmbed(BaseModel):
    title: Optional[str] = None
    type: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    timestamp: Optional[str] = None
    color: Optional[int] = None
    footer: Optional[DiscordEmbedFooter] = None
    image: Optional[DiscordEmbedMedia] = None
    thumbnail: Optional[DiscordEmbedMedia] = None
    video: Optional[DiscordEmbedMedia] = None
    provider: Optional[DiscordEmbedProvider] = None
    fields: Optional[List[DiscordEmbedField]] = None
    author: Optional[DiscordEmbedAuthor] = None


class DiscordWebhook(BaseModel):
    content: Optional[str] = None
    username: Optional[str] = None
    avatar_url: Optional[str] = None
    tts: Optional[bool] = False
    embeds: Optional[List[DiscordEmbed]] = None
    allowed_mentions: Optional[Dict[str, List[str]]] = None


class WebhookRedisEntry(BaseModel):
    message_id: str
    message_hash: str
