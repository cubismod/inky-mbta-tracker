# various pydantic models for responses
# https://api-v3.mbta.com/docs/swagger/index.html#/Schedule/ApiWeb_ScheduleController_index
from typing import Optional

from pydantic import BaseModel


class PageLinks(BaseModel):
    self: str
    prev: str
    next: str
    last: str
    first: str


class ScheduleRelationshipLinks(BaseModel):
    self: str
    related: str


class TypeAndID(BaseModel):
    type: str
    id: str


class ScheduleRelationship(BaseModel):
    data: Optional[TypeAndID] = None


class ScheduleRelationships(BaseModel):
    trip: ScheduleRelationship
    stop: ScheduleRelationship
    route: Optional[ScheduleRelationship] = None
    prediction: Optional[ScheduleRelationship] = None


class ScheduleAttributes(BaseModel):
    timepoint: Optional[bool] = None
    stop_sequence: int
    stop_headsign: Optional[str] = None
    pickup_type: Optional[int] = None
    drop_off_type: Optional[int] = None
    direction_id: int
    departure_time: Optional[str] = None
    arrival_time: Optional[str] = None


class ScheduleResource(BaseModel):
    type: str
    relationships: ScheduleRelationships
    id: str
    attributes: ScheduleAttributes


class Schedules(BaseModel):
    links: PageLinks
    data: list[ScheduleResource]


class RouteLinks(BaseModel):
    self: str


class RouteAttributes(BaseModel):
    color: str
    direction_destinations: Optional[list[str]] = None
    fare_class: str
    direction_names: Optional[list[str]] = None
    sort_order: int
    short_name: str
    long_name: str
    text_color: str
    type: int
    description: str


class RouteResource(BaseModel):
    type: str
    relationships: dict
    links: dict
    id: str
    attributes: RouteAttributes


class Route(BaseModel):
    data: list[RouteResource]


class SelfAndRelated(BaseModel):
    self: str
    related: str


class FacilityData(BaseModel):
    type: str
    id: str


class ActivePeriod(BaseModel):
    start: str
    end: str


class InformedEntity(BaseModel):
    trip: str
    stop: str
    route_type: int
    route: str
    facility: str
    direction_id: int
    activities: list[str]
    image: str
    service_effect: str
    duration_certainty: str
    description: str
    security: int


class AlertAttributes(BaseModel):
    timeframe: str
    image_alternative_text: str
    cause: str
    created_at: str
    banner: str
    header: str
    url: str
    short_header: str
    effect: str
    updated_at: str
    effect_name: str
    active_period: ActivePeriod
    informed_entity: InformedEntity


class AlertFacility(BaseModel):
    links: SelfAndRelated
    data: FacilityData
    links: dict
    id: str
    attributes: AlertAttributes


class AlertRelationships(BaseModel):
    facility: AlertFacility


class AlertResource(BaseModel):
    type: str
    relationships: AlertRelationships


class Alerts(BaseModel):
    links: PageLinks
    data: AlertResource


class TripGeneric(BaseModel):
    links: SelfAndRelated
    data: TypeAndID


class TripAttributes(BaseModel):
    wheelchair_accessible: int
    revenue_status: Optional[str] = None
    name: str
    headsign: str
    direction_id: int
    block_id: str
    bikes_allowed: Optional[int] = None


class TripResource(BaseModel):
    type: str
    relationships: dict
    attributes: TripAttributes


class Trips(BaseModel):
    data: list[TripResource]