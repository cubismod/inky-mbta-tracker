from collections.abc import Mapping, Sequence
from math import atan2, cos, degrees, radians, sin, sqrt
from typing import cast

from geojson import Feature, Point

EARTH_RADIUS_METERS = 6_371_008.8
METERS_PER_MILE = 1_609.344

GeoPoint = Feature | Point


def _coordinates(value: GeoPoint) -> tuple[float, float]:
    data = cast(Mapping[str, object], value)
    geometry = data.get("geometry") if data.get("type") == "Feature" else data

    if not isinstance(geometry, Mapping):
        raise ValueError("GeoJSON value must be a Point or Feature with Point geometry")

    coordinates = geometry.get("coordinates")
    if (
        not isinstance(coordinates, Sequence)
        or isinstance(coordinates, str)
        or len(coordinates) < 2
    ):
        raise ValueError("GeoJSON point must contain longitude and latitude")

    longitude = float(coordinates[0])
    latitude = float(coordinates[1])
    return longitude, latitude


def distance(start: GeoPoint, end: GeoPoint, unit: str = "mi") -> float:
    start_longitude, start_latitude = _coordinates(start)
    end_longitude, end_latitude = _coordinates(end)

    start_latitude_rad = radians(start_latitude)
    end_latitude_rad = radians(end_latitude)
    delta_latitude = radians(end_latitude - start_latitude)
    delta_longitude = radians(end_longitude - start_longitude)

    a = (
        sin(delta_latitude / 2) ** 2
        + cos(start_latitude_rad)
        * cos(end_latitude_rad)
        * sin(delta_longitude / 2) ** 2
    )
    meters = EARTH_RADIUS_METERS * 2 * atan2(sqrt(a), sqrt(1 - a))

    match unit:
        case "m" | "meter" | "meters":
            return meters
        case "km" | "kilometer" | "kilometers":
            return meters / 1_000
        case "mi" | "mile" | "miles":
            return meters / METERS_PER_MILE
        case _:
            raise ValueError(f"Unsupported distance unit: {unit}")


def bearing(start: Point, end: Point) -> float:
    start_longitude, start_latitude = _coordinates(start)
    end_longitude, end_latitude = _coordinates(end)

    start_latitude_rad = radians(start_latitude)
    end_latitude_rad = radians(end_latitude)
    delta_longitude = radians(end_longitude - start_longitude)

    y = sin(delta_longitude) * cos(end_latitude_rad)
    x = cos(start_latitude_rad) * sin(end_latitude_rad) - sin(start_latitude_rad) * cos(
        end_latitude_rad
    ) * cos(delta_longitude)

    return degrees(atan2(y, x))
