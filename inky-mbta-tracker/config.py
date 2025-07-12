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


def load_config() -> Config:
    conf_location = os.getenv("IMT_CONFIG", "./config.json")

    json_conf = pathlib.Path(conf_location).read_text()
    config = Config.model_validate_json(json_conf)
    return config
