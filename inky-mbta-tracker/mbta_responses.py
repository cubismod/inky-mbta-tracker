# various pydantic models for responses
# https://api-v3.mbta.com/docs/swagger/index.html#/Schedule/ApiWeb_ScheduleController_index
from typing import Literal, Optional

from pydantic import BaseModel

DirectionId = Literal[0, 1]
PickupDropoffType = Literal[0, 1, 2, 3]
AccessibilityStatus = Literal[0, 1, 2]
RouteType = Literal[0, 1, 2, 3, 4]
StopLocationType = Literal[0, 1, 2, 3]
RevenueStatus = Literal["REVENUE", "NON_REVENUE"]
PredictionScheduleRelationship = Literal[
    "ADDED", "CANCELLED", "NO_DATA", "SKIPPED", "UNSCHEDULED"
]
PredictionUpdateType = Literal["MID_TRIP", "AT_TERMINAL", "REVERSE_TRIP"]
AlertDurationCertainty = Literal["UNKNOWN", "KNOWN", "ESTIMATED"]
AlertActivity = Literal[
    "BOARD",
    "BRINGING_BIKE",
    "EXIT",
    "PARK_CAR",
    "RIDE",
    "STORE_BIKE",
    "USING_ESCALATOR",
    "USING_WHEELCHAIR",
]
VehicleOccupancyStatus = Literal[
    "MANY_SEATS_AVAILABLE",
    "FEW_SEATS_AVAILABLE",
    "FULL",
    "NO_DATA_AVAILABLE",
]
CarriageOccupancyStatus = Literal[
    "EMPTY",
    "MANY_SEATS_AVAILABLE",
    "FEW_SEATS_AVAILABLE",
    "STANDING_ROOM_ONLY",
    "CRUSHED_STANDING_ROOM_ONLY",
    "FULL",
    "NOT_ACCEPTING_PASSENGERS",
    "NO_DATA_AVAILABLE",
    "NOT_BOARDABLE",
]
FacilityType = Literal[
    "BIKE_STORAGE",
    "BRIDGE_PLATE",
    "ELECTRIC_CAR_CHARGERS",
    "ELEVATED_SUBPLATFORM",
    "ELEVATOR",
    "ESCALATOR",
    "FARE_MEDIA_ASSISTANCE_FACILITY",
    "FARE_MEDIA_ASSISTANT",
    "FARE_VENDING_MACHINE",
    "FARE_VENDING_RETAILER",
    "FULLY_ELEVATED_PLATFORM",
    "OTHER",
    "PARKING_AREA",
    "PICK_DROP",
    "PORTABLE_BOARDING_LIFT",
    "RAMP",
    "TAXI_STAND",
    "TICKET_WINDOW",
]
AlertCause = Literal[
    "ACCIDENT",
    "AMTRAK_TRAIN_TRAFFIC",
    "COAST_GUARD_RESTRICTION",
    "CONSTRUCTION",
    "CROSSING_ISSUE",
    "DEMONSTRATION",
    "DISABLED_BUS",
    "DISABLED_TRAIN",
    "DRAWBRIDGE_BEING_RAISED",
    "ELECTRICAL_WORK",
    "FIRE",
    "FIRE_DEPARTMENT_ACTIVITY",
    "FLOODING",
    "FOG",
    "FREIGHT_TRAIN_INTERFERENCE",
    "HAZMAT_CONDITION",
    "HEAVY_RIDERSHIP",
    "HIGH_WINDS",
    "HOLIDAY",
    "HURRICANE",
    "ICE_IN_HARBOR",
    "MAINTENANCE",
    "MECHANICAL_ISSUE",
    "MECHANICAL_PROBLEM",
    "MEDICAL_EMERGENCY",
    "PARADE",
    "POLICE_ACTION",
    "POLICE_ACTIVITY",
    "POWER_PROBLEM",
    "RAIL_DEFECT",
    "SEVERE_WEATHER",
    "SIGNAL_ISSUE",
    "SIGNAL_PROBLEM",
    "SINGLE_TRACKING",
    "SLIPPERY_RAIL",
    "SNOW",
    "SPECIAL_EVENT",
    "SPEED_RESTRICTION",
    "SWITCH_ISSUE",
    "SWITCH_PROBLEM",
    "TIE_REPLACEMENT",
    "TRACK_PROBLEM",
    "TRACK_WORK",
    "TRAFFIC",
    "TRAIN_TRAFFIC",
    "UNKNOWN_CAUSE",
    "UNRULY_PASSENGER",
    "WEATHER",
]
AlertEffect = Literal[
    "ACCESS_ISSUE",
    "ADDITIONAL_SERVICE",
    "AMBER_ALERT",
    "BIKE_ISSUE",
    "CANCELLATION",
    "DELAY",
    "DETOUR",
    "DOCK_CLOSURE",
    "DOCK_ISSUE",
    "ELEVATOR_CLOSURE",
    "ESCALATOR_CLOSURE",
    "EXTRA_SERVICE",
    "FACILITY_ISSUE",
    "MODIFIED_SERVICE",
    "NOTICE",
    "NO_SERVICE",
    "OTHER_EFFECT",
    "PARKING_CLOSURE",
    "PARKING_ISSUE",
    "POLICY_CHANGE",
    "SCHEDULE_CHANGE",
    "SERVICE_CHANGE",
    "SHUTTLE",
    "SNOW_ROUTE",
    "STATION_CLOSURE",
    "STATION_ISSUE",
    "STOP_CLOSURE",
    "STOP_MOVE",
    "STOP_MOVED",
    "SUMMARY",
    "SUSPENSION",
    "TRACK_CHANGE",
    "UNKNOWN_EFFECT",
]


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


class SelfAndRelated(BaseModel):
    self: str
    related: str


class LinksAndData(BaseModel):
    links: Optional[SelfAndRelated] = None
    data: Optional[TypeAndID] = None


class LinksAndDataList(BaseModel):
    links: Optional[SelfAndRelated] = None
    data: Optional[list[TypeAndID]] = None


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
    pickup_type: Optional[PickupDropoffType] = None
    drop_off_type: Optional[PickupDropoffType] = None
    direction_id: DirectionId
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
    type: RouteType
    description: str


class RouteResource(BaseModel):
    type: str
    relationships: Optional["RouteRelationships"] = None
    links: dict
    id: str
    attributes: RouteAttributes


class Route(BaseModel):
    data: list[RouteResource]


class FacilityData(BaseModel):
    type: str
    id: str


class ActivePeriod(BaseModel):
    start: Optional[str] = None
    end: Optional[str] = None


class InformedEntity(BaseModel):
    trip: Optional[str] = None
    stop: Optional[str] = None
    route_type: Optional[RouteType] = None
    route: Optional[str] = None
    facility: Optional[str] = None
    direction_id: Optional[DirectionId] = None
    activities: Optional[list[AlertActivity]] = None
    image: Optional[str] = None
    service_effect: Optional[str] = None
    duration_certainty: Optional[AlertDurationCertainty] = None
    description: Optional[str] = None


class AlertAttributes(BaseModel):
    timeframe: Optional[str] = None
    image_alternative_text: Optional[str] = None
    cause: AlertCause
    image: Optional[str] = None
    created_at: str
    banner: Optional[str] = None
    header: str
    url: Optional[str] = None
    short_header: str
    effect: Optional[AlertEffect] = None
    updated_at: str
    effect_name: Optional[str] = None
    active_period: list[ActivePeriod]
    informed_entity: list[InformedEntity]
    severity: int


class AlertResource(BaseModel):
    type: str
    id: str
    relationships: Optional["AlertRelationships"] = None
    attributes: AlertAttributes


class Alerts(BaseModel):
    data: list[AlertResource]


class TripGeneric(BaseModel):
    links: SelfAndRelated
    data: TypeAndID


class TripAttributes(BaseModel):
    wheelchair_accessible: AccessibilityStatus
    revenue_status: Optional[RevenueStatus] = None
    name: str
    headsign: str
    direction_id: DirectionId
    block_id: Optional[str] = None
    bikes_allowed: Optional[AccessibilityStatus] = None


class TripRelationship(BaseModel):
    shape: Optional[LinksAndData] = None
    service: Optional[LinksAndData] = None
    route_pattern: Optional[LinksAndData] = None
    route: Optional[LinksAndData] = None
    occupancy: Optional[LinksAndData] = None


class TripResource(BaseModel):
    type: str
    relationships: TripRelationship
    attributes: TripAttributes


class Trips(BaseModel):
    data: list[TripResource]


class StopRelationship(BaseModel):
    parent_station: Optional[LinksAndData] = None


class RouteRelationships(BaseModel):
    route_patterns: Optional[LinksAndDataList] = None
    line: Optional[LinksAndData] = None
    agency: Optional[LinksAndData] = None


class AlertRelationships(BaseModel):
    facility: Optional[LinksAndData] = None


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
    location_type: StopLocationType
    longitude: float
    municipality: Optional[str] = None
    name: str
    on_street: Optional[str] = None
    platform_code: Optional[str] = None
    platform_name: Optional[str] = None
    vehicle_type: Optional[RouteType] = None
    wheelchair_boarding: AccessibilityStatus


class StopResource(BaseModel):
    type: str
    relationships: Optional[StopRelationship] = None
    attributes: StopAttributes
    id: str
    links: Optional[dict] = None


class Stop(BaseModel):
    data: StopResource


class PredictionRelationships(BaseModel):
    vehicle: Optional[LinksAndData]
    trip: LinksAndData
    stop: LinksAndData
    schedule: Optional[LinksAndData] = None
    route: LinksAndData
    alerts: Optional[LinksAndData] = None


class PredictionAttributes(BaseModel):
    schedule_relationship: Optional[PredictionScheduleRelationship] = None
    departure_uncertainty: Optional[int] = None
    arrival_uncertainty: Optional[int] = None
    update_type: Optional[PredictionUpdateType] = None
    status: Optional[str] = None
    arrival_time: Optional[str] = None
    revenue_status: Optional[RevenueStatus] = None
    departure_time: Optional[str] = None
    direction_id: Optional[int] = None
    last_trip: Optional[bool] = None
    stop_sequence: Optional[int] = None
    trip_headsign: Optional[str] = None


class PredictionResource(BaseModel):
    type: str
    relationships: PredictionRelationships
    links: Optional[dict] = None
    id: str
    attributes: PredictionAttributes


class PredictionResourceList(BaseModel):
    data: list[PredictionResource]


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
    type: FacilityType
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
    occupancy_status: Optional[CarriageOccupancyStatus] = None
    occupancy_percentage: Optional[int] = None
    label: Optional[str] = None


# not every field is being included in responses
class VehicleAttributes(BaseModel):
    current_status: str = ""
    direction_id: DirectionId
    latitude: float = 0
    longitude: float = 0
    speed: Optional[float] = None
    occupancy_status: Optional[VehicleOccupancyStatus] = None
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
    relationships: Optional[VehicleRelationships] = None
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
