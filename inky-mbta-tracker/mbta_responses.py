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


# https://www.mbta.com/developers/v3-api/streaming
class AddUpdateSchedule(BaseModel):
    data: ScheduleResource


class Schedules(BaseModel):
    data: list[ScheduleResource]


class RemoveSchedule(BaseModel):
    data: TypeAndID


class RouteLinks(BaseModel):
    self: str


# About RouteAttributes.type:
# 0	Light Rail
# 1	Heavy Rail
# 2	Commuter
# 3	Bus
# 4	Ferry
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
    start: Optional[str] = None
    end: Optional[str] = None


class InformedEntity(BaseModel):
    trip: Optional[str] = None
    stop: Optional[str] = None
    route_type: Optional[int] = None
    route: Optional[str] = None
    facility: Optional[str] = None
    direction_id: Optional[int] = None
    activities: Optional[list[str]] = None
    image: Optional[str] = None
    service_effect: Optional[str] = None
    duration_certainty: Optional[str] = None
    description: Optional[str] = None


class AlertAttributes(BaseModel):
    timeframe: Optional[str] = None
    image_alternative_text: Optional[str] = None
    cause: str
    image: Optional[str] = None
    created_at: str
    banner: Optional[str] = None
    header: str
    url: Optional[str] = None
    short_header: str
    effect: str
    updated_at: str
    effect_name: Optional[str] = None
    active_period: list[ActivePeriod]
    informed_entity: list[InformedEntity]
    severity: int


class AlertResource(BaseModel):
    type: str
    id: str
    attributes: AlertAttributes


class Alerts(BaseModel):
    data: list[AlertResource]


class TripGeneric(BaseModel):
    links: SelfAndRelated
    data: TypeAndID


class TripAttributes(BaseModel):
    wheelchair_accessible: int
    revenue_status: Optional[str] = None
    name: str
    headsign: str
    direction_id: int
    block_id: Optional[str] = None
    bikes_allowed: Optional[int] = None


class TripResource(BaseModel):
    type: str
    relationships: dict
    attributes: TripAttributes


class Trips(BaseModel):
    data: list[TripResource]


class StopRelationship(BaseModel):
    parent_station: dict


# Value	Type	Description
# 0	Stop	A location where passengers board or disembark from a transit vehicle.
# 1	Station	A physical structure or area that contains one or more stops.
# 2	Station Entrance/Exit	A location where passengers can enter or exit a station from the street. The stop entry must also specify a parent_station value referencing the stop ID of the parent station for the entrance.
# 3	Generic Node	A location within a station, not matching any other location_type, which can be used to link together pathways defined in pathways.txt.


class StopAttributes(BaseModel):
    address: Optional[str] = None
    at_street: Optional[str] = None
    description: Optional[str] = None
    latitude: float
    location_type: int
    longitude: float
    municipality: Optional[str] = None
    name: str
    on_street: Optional[str] = None
    platform_code: Optional[str] = None
    platform_name: Optional[str] = None
    vehicle_type: Optional[int] = None
    wheelchair_boarding: int


class StopResource(BaseModel):
    type: str
    relationships: dict
    attributes: StopAttributes
    id: str
    links: Optional[dict] = None


class Stop(BaseModel):
    data: StopResource


class LinksAndData(BaseModel):
    links: Optional[SelfAndRelated] = None
    data: Optional[TypeAndID] = None


class PredictionRelationships(BaseModel):
    vehicle: Optional[LinksAndData]
    trip: LinksAndData
    stop: LinksAndData
    schedule: Optional[LinksAndData] = None
    route: LinksAndData
    alerts: Optional[LinksAndData] = None


class PredictionAttributes(BaseModel):
    schedule_relationship: Optional[str] = None
    departure_uncertainty: Optional[int] = None
    arrival_uncertainty: Optional[int] = None
    update_type: Optional[str] = None
    status: Optional[str] = None
    arrival_time: Optional[str] = None
    revenue: str
    departure_time: Optional[str] = None
    direction_id: int
    last_trip: Optional[bool] = None
    stop_sequence: Optional[int] = None


class PredictionResource(BaseModel):
    type: str
    relationships: PredictionRelationships
    links: Optional[dict] = None
    id: str
    attributes: PredictionAttributes


class FacilityRelationships(BaseModel):
    stop: LinksAndData


class FacilityProperty(BaseModel):
    value: str | int
    name: str


class FacilityAttributes(BaseModel):
    # [ BIKE_STORAGE, BRIDGE_PLATE, ELECTRIC_CAR_CHARGERS,
    # ELEVATED_SUBPLATFORM, ELEVATOR, ESCALATOR, FARE_MEDIA_ASSISTANCE_FACILITY,
    # FARE_MEDIA_ASSISTANT, FARE_VENDING_MACHINE, FARE_VENDING_RETAILER,
    # FULLY_ELEVATED_PLATFORM, OTHER, PARKING_AREA, PICK_DROP,
    # PORTABLE_BOARDING_LIFT, RAMP, TAXI_STAND, TICKET_WINDOW ]
    type: str
    short_name: str
    properties: list[FacilityProperty]
    longitude: Optional[float] = None
    long_name: str
    latitude: Optional[float] = None


class FacilityResource(BaseModel):
    type: str
    relationships: FacilityRelationships
    links: Optional[dict] = None
    id: str
    attributes: FacilityAttributes


class Facility(BaseModel):
    links: Optional[dict] = None
    included: TypeAndID
    data: FacilityResource


class Facilities(BaseModel):
    links: Optional[dict] = None
    data: list[FacilityResource] = []


class StopAndFacilities(BaseModel):
    stop: Stop
    facilities: Optional[Facilities] = None


class CarriageStatus(BaseModel):
    occupancy_status: Optional[str] = None
    occupancy_percentage: Optional[int] = None
    label: Optional[str] = None


# not every field is being included in responses
class VehicleAttributes(BaseModel):
    current_status: str = ""
    direction_id: int
    latitude: float = 0
    longitude: float = 0
    speed: Optional[float] = None
    occupancy_status: Optional[str] = None
    carriages: Optional[list[CarriageStatus]] = None


class TypeAndIDinData(BaseModel):
    data: Optional[TypeAndID] = None


class VehicleRelationships(BaseModel):
    route: TypeAndIDinData
    stop: Optional[TypeAndIDinData] = None
    trip: Optional[TypeAndIDinData] = None


class Vehicle(BaseModel):
    id: str
    links: Optional[dict] = None
    attributes: VehicleAttributes
    relationships: VehicleRelationships
    type: str


class ShapeAttributes(BaseModel):
    polyline: str


class ShapeResource(BaseModel):
    type: str
    relationships: Optional[dict] = None
    links: Optional[dict] = None
    id: str
    attributes: ShapeAttributes


class Shapes(BaseModel):
    data: list[ShapeResource]
