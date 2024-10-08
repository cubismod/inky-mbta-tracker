import logging
import os
import pathlib
from typing import Optional

from pydantic import BaseModel, ValidationError


class StopSetup(BaseModel):
    stop_id: str
    route_filter: Optional[str]
    direction_filter: Optional[str]


class Config(BaseModel):
    stops: list[StopSetup]


def load_config():
    conf_location = os.getenv("IMT_CONFIG", "./config.json")

    json_conf = pathlib.Path(conf_location).read_text()
    config = Config.model_validate_json(json_conf)
    return config
