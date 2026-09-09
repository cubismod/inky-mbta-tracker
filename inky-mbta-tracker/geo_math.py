from collections.abc import Mapping, Sequence
from math import atan2, cos, degrees, radians, sin, sqrt
from typing import NamedTuple, cast

from geojson import Feature, Point

EARTH_RADIUS_METERS = 6_371_008.8
METERS_PER_MILE = 1_609.344

GeoPoint = Feature | Point

type Polyline = Sequence[Sequence[float]]


class SnapResult(NamedTuple):
    snapped: tuple[float, float]
    distance_along_m: float
    cross_track_m: float


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


def _to_local_meters(
    longitude: float, latitude: float, ref_lon: float, ref_lat: float
) -> tuple[float, float]:
    x = EARTH_RADIUS_METERS * radians(longitude - ref_lon) * cos(radians(ref_lat))
    y = EARTH_RADIUS_METERS * radians(latitude - ref_lat)
    return x, y


def _to_lon_lat(
    x: float, y: float, ref_lon: float, ref_lat: float
) -> tuple[float, float]:
    longitude = ref_lon + degrees(x / (EARTH_RADIUS_METERS * cos(radians(ref_lat))))
    latitude = ref_lat + degrees(y / EARTH_RADIUS_METERS)
    return longitude, latitude


def project_onto_segment(
    point: GeoPoint, seg_start: GeoPoint, seg_end: GeoPoint
) -> tuple[float, float]:
    ref_lon, ref_lat = _coordinates(seg_start)
    px, py = _to_local_meters(*_coordinates(point), ref_lon, ref_lat)
    ax, ay = 0.0, 0.0
    bx, by = _to_local_meters(*_coordinates(seg_end), ref_lon, ref_lat)

    dx, dy = bx - ax, by - ay
    length_sq = dx * dx + dy * dy
    if length_sq == 0:
        return 0.0, sqrt(px * px + py * py)

    t = ((px - ax) * dx + (py - ay) * dy) / length_sq
    t = max(0.0, min(1.0, t))
    cross_x = px - (ax + t * dx)
    cross_y = py - (ay + t * dy)
    return t, sqrt(cross_x * cross_x + cross_y * cross_y)


def snap_to_route(point: GeoPoint, polyline: Polyline) -> SnapResult:
    if len(polyline) < 2:
        raise ValueError("Polyline must contain at least two coordinates")

    ref_lon, ref_lat = polyline[0][0], polyline[0][1]
    px, py = _to_local_meters(*_coordinates(point), ref_lon, ref_lat)

    best: tuple[float, float, float, float, float, float, float, float] | None = None
    cumulative = 0.0
    for seg_start, seg_end in zip(polyline, polyline[1:], strict=False):
        ax, ay = _to_local_meters(seg_start[0], seg_start[1], ref_lon, ref_lat)
        bx, by = _to_local_meters(seg_end[0], seg_end[1], ref_lon, ref_lat)
        dx, dy = bx - ax, by - ay
        seg_len = sqrt(dx * dx + dy * dy)

        if seg_len == 0:
            t, cross = 0.0, sqrt((px - ax) ** 2 + (py - ay) ** 2)
        else:
            t = max(
                0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / (seg_len * seg_len))
            )
            cross_x = px - (ax + t * dx)
            cross_y = py - (ay + t * dy)
            cross = sqrt(cross_x * cross_x + cross_y * cross_y)

        if best is None or cross < best[2]:
            best = (cumulative, seg_len, cross, t, ax, ay, dx, dy)

        cumulative += seg_len

    assert best is not None
    seg_offset, seg_len, cross, t, ax, ay, dx, dy = best
    sx = ax + t * dx
    sy = ay + t * dy
    snapped = _to_lon_lat(sx, sy, ref_lon, ref_lat)
    return SnapResult(
        snapped=snapped,
        distance_along_m=seg_offset + t * seg_len,
        cross_track_m=cross,
    )


def snap_to_routes(point: GeoPoint, polylines: Sequence[Polyline]) -> SnapResult:
    if not polylines:
        raise ValueError("At least one polyline is required")

    best: SnapResult | None = None
    for polyline in polylines:
        result = snap_to_route(point, polyline)
        if best is None or result.cross_track_m < best.cross_track_m:
            best = result
    assert best is not None
    return best
