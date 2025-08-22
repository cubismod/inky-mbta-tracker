import os
import pathlib
from typing import Optional

from pydantic import BaseModel, Field


class StopSetup(BaseModel):
    stop_id: str
    route_filter: str = Field(default="")
    direction_filter: int = Field(default=-1)
    transit_time_min: int
    schedule_only: bool = Field(default=False)
    show_on_display: bool = Field(default=True)
    route_substring_filter: Optional[str] = None


class OllamaConfig(BaseModel):
    """Configuration for Ollama AI summarizer"""

    enabled: bool = Field(default=False, description="Enable AI summarizer feature")
    base_url: str = Field(
        default="http://localhost:11434", description="Ollama API base URL"
    )
    model: str = Field(
        default="llama3.2:3b", description="Model to use for summarization"
    )
    timeout: int = Field(default=30, description="Request timeout in seconds")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    temperature: float = Field(
        default=0.1, description="Model temperature for generation"
    )
    cache_ttl: int = Field(
        default=300, description="Cache TTL for summaries in seconds"
    )


class Config(BaseModel):
    stops: list[StopSetup]
    # fetches real-time vehicle information with the numbers referring to
    # the routes
    vehicles_by_route: Optional[list[str]] = None
    # git repo to store vehicle location information in
    vehicle_git_repo: Optional[str] = None
    vehicle_git_user: Optional[str] = None
    # git auth token to use when cloning
    vehicle_git_token: Optional[str] = None
    vehicle_git_email: Optional[str] = None
    # track prediction precaching settings
    enable_track_predictions: bool = Field(default=False)
    track_prediction_routes: Optional[list[str]] = None
    track_prediction_stations: Optional[list[str]] = None
    track_prediction_interval_hours: int = Field(default=2)
    # AI summarizer settings
    ollama: OllamaConfig = Field(default_factory=OllamaConfig)


def load_config() -> Config:
    conf_location = os.getenv("IMT_CONFIG", "./config.json")

    json_conf = pathlib.Path(conf_location).read_text()
    config = Config.model_validate_json(json_conf)
    return config
