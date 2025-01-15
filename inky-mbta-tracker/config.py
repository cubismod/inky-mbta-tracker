import os
import pathlib
from typing import Optional

from pydantic import BaseModel, Field


class StopSetup(BaseModel):
    stop_id: str
    route_filter: str = Field(default="")
    direction_filter: str = Field(default="")
    transit_time_min: int
    schedule_only: bool = Field(default=False)


class Config(BaseModel):
    stops: list[StopSetup]
    # fetches real-time vehicle information with the numbers referring to
    # the routes
    vehicles_by_route: Optional[list[str]] = None


def load_config():
    conf_location = os.getenv("IMT_CONFIG", "./config.json")

    json_conf = pathlib.Path(conf_location).read_text()
    config = Config.model_validate_json(json_conf)
    return config
