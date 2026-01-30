import os
import pathlib
from typing import Optional

from opentelemetry.trace import Span
from otel_config import get_tracer, is_otel_enabled
from pydantic import BaseModel, Field


class StopSetup(BaseModel):
    stop_id: str
    route_filter: str = Field(default="")
    direction_filter: int = Field(default=-1)
    transit_time_min: int
    schedule_only: bool = Field(default=False)
    show_on_display: bool = Field(default=True)
    route_substring_filter: Optional[str] = None


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
    severity_icons: Optional[list[str]] = None


def load_config() -> Config:
    tracer = get_tracer(__name__) if is_otel_enabled() else None
    if tracer:
        with tracer.start_as_current_span("config.load_config") as span:
            return _load_config_impl(span)
    else:
        return _load_config_impl(None)


def _load_config_impl(span: Optional[Span]) -> Config:
    conf_location = os.getenv("IMT_CONFIG", "./config.json")
    if span:
        span.set_attribute("config.location", conf_location)

    json_conf = pathlib.Path(conf_location).read_text()
    config = Config.model_validate_json(json_conf)

    return config
