"""Polygon / anchor-point helpers (ported from features/no-entry-zone).

Kept free of any framework or model import so the zone logic stays unit-testable.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import cv2
import numpy as np

Point = tuple[int, int]
AnchorMode = Literal["bottom_center", "center"]

MIN_POLYGON_POINTS = 3


def anchor_point(xyxy: Sequence[float], mode: AnchorMode = "bottom_center") -> Point:
    """Which point on the box decides whether the object is inside the zone.

    ``bottom_center`` is the feet of a person / the contact patch of a ground vehicle;
    ``center`` suits airborne objects such as drones.
    """
    x1, y1, x2, y2 = (float(v) for v in xyxy)
    cx = int(round((x1 + x2) / 2))
    if mode == "center":
        return cx, int(round((y1 + y2) / 2))
    return cx, int(round(y2))


def to_numpy_polygon(points: Sequence[Sequence[int]]) -> np.ndarray:
    return np.array([(int(x), int(y)) for x, y in points], dtype=np.int32)


def is_inside(polygon: np.ndarray, point: Point) -> bool:
    """True when the point is inside the polygon or exactly on its edge."""
    return cv2.pointPolygonTest(polygon, (float(point[0]), float(point[1])), False) >= 0


def scale_polygon(
    points: Sequence[Sequence[int]],
    from_size: tuple[int, int],
    to_size: tuple[int, int],
) -> list[Point]:
    """Rescale a polygon drawn at ``from_size`` onto a frame of ``to_size``.

    Cameras occasionally come back at a different resolution after a reconnect; without
    this the zone would silently sit in the wrong place.
    """
    from_w, from_h = from_size
    to_w, to_h = to_size
    if from_w <= 0 or from_h <= 0:
        raise ValueError("reference size must be positive")
    if (from_w, from_h) == (to_w, to_h):
        return [(int(x), int(y)) for x, y in points]
    sx, sy = to_w / from_w, to_h / from_h
    return [(int(round(x * sx)), int(round(y * sy))) for x, y in points]


def _orientation(a: Point, b: Point, c: Point) -> int:
    value = (b[1] - a[1]) * (c[0] - b[0]) - (b[0] - a[0]) * (c[1] - b[1])
    if value == 0:
        return 0
    return 1 if value > 0 else 2


def _on_segment(a: Point, b: Point, c: Point) -> bool:
    return min(a[0], c[0]) <= b[0] <= max(a[0], c[0]) and min(a[1], c[1]) <= b[1] <= max(a[1], c[1])


def _segments_intersect(p1: Point, q1: Point, p2: Point, q2: Point) -> bool:
    o1, o2 = _orientation(p1, q1, p2), _orientation(p1, q1, q2)
    o3, o4 = _orientation(p2, q2, p1), _orientation(p2, q2, q1)
    if o1 != o2 and o3 != o4:
        return True
    return (
        (o1 == 0 and _on_segment(p1, p2, q1))
        or (o2 == 0 and _on_segment(p1, q2, q1))
        or (o3 == 0 and _on_segment(p2, p1, q2))
        or (o4 == 0 and _on_segment(p2, q1, q2))
    )


def is_simple_polygon(points: Sequence[Point]) -> bool:
    """False when any pair of non-adjacent edges cross (a self-intersecting polygon
    makes ``pointPolygonTest`` results meaningless)."""
    count = len(points)
    if count < MIN_POLYGON_POINTS:
        return False
    for i in range(count):
        a1, a2 = points[i], points[(i + 1) % count]
        for j in range(i + 1, count):
            if j == i or (j + 1) % count == i or j == (i + 1) % count:
                continue
            b1, b2 = points[j], points[(j + 1) % count]
            if _segments_intersect(a1, a2, b1, b2):
                return False
    return True


def validate_polygon(points: Sequence[Sequence[int]]) -> list[Point]:
    """Normalise and sanity-check a polygon coming from the API."""
    if points is None:
        raise ValueError("polygon is required")
    if len(points) < MIN_POLYGON_POINTS:
        raise ValueError(f"polygon needs at least {MIN_POLYGON_POINTS} points")

    normalised: list[Point] = []
    for point in points:
        if len(point) != 2:
            raise ValueError("each polygon point must be [x, y]")
        x, y = point
        if isinstance(x, bool) or isinstance(y, bool):
            raise ValueError("polygon coordinates must be numbers")
        normalised.append((int(x), int(y)))

    if len(set(normalised)) != len(normalised):
        raise ValueError("polygon contains duplicate points")
    if cv2.contourArea(to_numpy_polygon(normalised)) <= 0:
        raise ValueError("polygon has zero area")
    if not is_simple_polygon(normalised):
        raise ValueError("polygon edges must not cross each other")
    return normalised
