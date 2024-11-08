import os
import pathlib

from pydantic import BaseModel, Field


class StopSetup(BaseModel):
    stop_id: str
    route_filter: str = Field(default="")
    direction_filter: str = Field(default="")
    transit_time_min: int


class Config(BaseModel):
    stops: list[StopSetup]


def load_config():
    conf_location = os.getenv("IMT_CONFIG", "./config.json")

    json_conf = pathlib.Path(conf_location).read_text()
    config = Config.model_validate_json(json_conf)
    return config
