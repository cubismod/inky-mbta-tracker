import pytest
from geo_math import distance, project_onto_segment, snap_to_route, snap_to_routes
from geojson import Point


def _point(longitude: float, latitude: float) -> Point:
    return Point((longitude, latitude))


EAST_WEST = [[-71.0600, 42.3601], [-71.0585, 42.3601], [-71.0570, 42.3601]]

L_SHAPE = [[-71.0600, 42.3601], [-71.0585, 42.3601], [-71.0585, 42.3620]]


class TestProjectOntoSegment:
    def test_midpoint_projection(self) -> None:
        start = _point(-71.0600, 42.3601)
        end = _point(-71.0570, 42.3601)
        mid = _point(-71.0585, 42.3601)

        t, cross = project_onto_segment(mid, start, end)

        assert t == pytest.approx(0.5)
        assert cross == pytest.approx(0.0, abs=0.5)

    def test_clamps_before_start(self) -> None:
        start = _point(-71.0600, 42.3601)
        end = _point(-71.0570, 42.3601)
        before = _point(-71.0610, 42.3601)

        t, cross = project_onto_segment(before, start, end)

        assert t == 0.0
        assert cross == pytest.approx(distance(before, start, "m"), rel=0.01)

    def test_clamps_after_end(self) -> None:
        start = _point(-71.0600, 42.3601)
        end = _point(-71.0570, 42.3601)
        after = _point(-71.0560, 42.3601)

        t, cross = project_onto_segment(after, start, end)

        assert t == 1.0
        assert cross == pytest.approx(distance(after, end, "m"), rel=0.01)

    def test_cross_track_offset(self) -> None:
        start = _point(-71.0600, 42.3601)
        end = _point(-71.0570, 42.3601)
        offset = _point(-71.0585, 42.3610)

        t, cross = project_onto_segment(offset, start, end)

        assert t == pytest.approx(0.5)
        expected = distance(_point(-71.0585, 42.3601), offset, "m")
        assert cross == pytest.approx(expected, rel=0.01)


class TestSnapToRoute:
    def test_on_track_point(self) -> None:
        result = snap_to_route(_point(-71.0580, 42.3601), EAST_WEST)

        assert result.cross_track_m == pytest.approx(0.0, abs=0.5)
        expected_along = distance(_point(*EAST_WEST[0]), _point(-71.0580, 42.3601), "m")
        assert result.distance_along_m == pytest.approx(expected_along, rel=0.01)
        assert result.snapped[0] == pytest.approx(-71.0580, abs=1e-5)
        assert result.snapped[1] == pytest.approx(42.3601, abs=1e-5)

    def test_skewed_point_snaps_to_line(self) -> None:
        result = snap_to_route(_point(-71.0585, 42.3610), EAST_WEST)

        expected_cross = distance(
            _point(-71.0585, 42.3601), _point(-71.0585, 42.3610), "m"
        )
        assert result.cross_track_m == pytest.approx(expected_cross, rel=0.01)
        assert result.snapped[1] == pytest.approx(42.3601, abs=1e-6)
        expected_along = distance(_point(*EAST_WEST[0]), _point(-71.0585, 42.3601), "m")
        assert result.distance_along_m == pytest.approx(expected_along, rel=0.01)

    def test_clamps_beyond_end(self) -> None:
        result = snap_to_route(_point(-71.0560, 42.3601), EAST_WEST)

        total = distance(_point(*EAST_WEST[0]), _point(*EAST_WEST[-1]), "m")
        assert result.distance_along_m == pytest.approx(total, rel=0.01)
        assert result.snapped[0] == pytest.approx(EAST_WEST[-1][0], abs=1e-6)
        assert result.snapped[1] == pytest.approx(EAST_WEST[-1][1], abs=1e-6)

    def test_curved_polyline_uses_nearest_leg(self) -> None:
        # Point is just west of the vertical (north-south) leg of the L shape.
        point = _point(-71.0590, 42.3610)

        result = snap_to_route(point, L_SHAPE)

        expected_cross = distance(_point(-71.0585, 42.3610), point, "m")
        assert result.cross_track_m == pytest.approx(expected_cross, rel=0.01)
        leg_start = distance(_point(*L_SHAPE[0]), _point(*L_SHAPE[1]), "m")
        leg_offset = distance(_point(*L_SHAPE[1]), _point(-71.0585, 42.3610), "m")
        assert result.distance_along_m == pytest.approx(
            leg_start + leg_offset, rel=0.01
        )

    def test_rejects_degenerate_polyline(self) -> None:
        with pytest.raises(ValueError):
            snap_to_route(_point(-71.0589, 42.3601), [[-71.0600, 42.3601]])


class TestSnapToRoutes:
    FAR_AWAY = [[-71.1000, 42.3000], [-71.0900, 42.3000]]

    def test_selects_polyline_with_smallest_cross_track(self) -> None:
        result = snap_to_routes(_point(-71.0585, 42.3601), [self.FAR_AWAY, EAST_WEST])

        expected = snap_to_route(_point(-71.0585, 42.3601), EAST_WEST)
        assert result.snapped == expected.snapped
        assert result.distance_along_m == pytest.approx(
            expected.distance_along_m, rel=0.01
        )

    def test_rejects_empty_polylines(self) -> None:
        with pytest.raises(ValueError):
            snap_to_routes(_point(-71.0589, 42.3601), [])
