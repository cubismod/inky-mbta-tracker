from datetime import datetime
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
