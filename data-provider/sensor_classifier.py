"""
sensor_classifier.py — mobile-vs-fixed sensor heuristic for the Data Provider.

Safecast measurements come from two kinds of device: **fixed** installations
that report from one spot (with a little GPS jitter), and **mobile** bGeigie
units that are carried or driven around. The raw CSV does not label which is
which, so we infer it at the producer from the spread of each sensor's
coordinates as the stream flows past.

Heuristic (see ADR-011):
  * Per sensor, keep a running bounding box of every latitude/longitude seen.
  * The sensor is "mobile" once the box's diagonal (great-circle distance
    between its opposite corners) exceeds ``movement_threshold_m`` metres;
    otherwise it is "fixed".
  * The label is **sticky**: once a sensor is seen to move it stays "mobile"
    for the rest of the run, even if later readings cluster again.

This is a streaming heuristic, so a mobile sensor is reported as "fixed" until
it has actually moved far enough — the label can only sharpen as more of its
readings arrive. Downstream consumers should treat ``sensor_type`` as a hint,
not ground truth.

Memory: state is one small record per *distinct* sensor_id (a handful of
floats + a flag), so it is bounded by the number of devices in the dataset,
not by the length of the stream.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import asin, cos, radians, sin, sqrt

# Default radius (metres) a sensor's readings may span and still count as
# "fixed". Comfortably above typical consumer-GPS jitter (tens of metres) yet
# far below the distance a mobile bGeigie covers (kilometres).
DEFAULT_MOVEMENT_THRESHOLD_M = 200.0

_EARTH_RADIUS_M = 6_371_000.0

FIXED = "fixed"
MOBILE = "mobile"


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two lat/lon points, in metres."""
    rlat1, rlat2 = radians(lat1), radians(lat2)
    dlat = rlat2 - rlat1
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(rlat1) * cos(rlat2) * sin(dlon / 2) ** 2
    return 2 * _EARTH_RADIUS_M * asin(sqrt(a))


@dataclass
class _SensorState:
    """Running bounding box + sticky-mobile flag for one sensor."""

    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float
    is_mobile: bool = False

    def extend(self, lat: float, lon: float) -> None:
        if lat < self.min_lat:
            self.min_lat = lat
        elif lat > self.max_lat:
            self.max_lat = lat
        if lon < self.min_lon:
            self.min_lon = lon
        elif lon > self.max_lon:
            self.max_lon = lon

    def span_m(self) -> float:
        """Great-circle distance across the bounding box's diagonal."""
        return haversine_m(self.min_lat, self.min_lon, self.max_lat, self.max_lon)


class SensorClassifier:
    """
    Stateful mobile-vs-fixed classifier.

    Feed it each event's sensor id and coordinates in stream order; it returns
    the current best label for that sensor. Not thread-safe — the producer
    calls it from its single publishing loop.
    """

    def __init__(self, movement_threshold_m: float = DEFAULT_MOVEMENT_THRESHOLD_M) -> None:
        if movement_threshold_m <= 0:
            raise ValueError("movement_threshold_m must be positive")
        self._threshold_m = movement_threshold_m
        self._state: dict[str, _SensorState] = {}

    def classify(self, sensor_id: str, latitude: float, longitude: float) -> str:
        """Return "mobile" or "fixed" for the sensor given its latest reading."""
        state = self._state.get(sensor_id)
        if state is None:
            self._state[sensor_id] = _SensorState(latitude, latitude, longitude, longitude)
            return FIXED

        if state.is_mobile:
            # Sticky: no need to keep growing the box once we've decided.
            return MOBILE

        state.extend(latitude, longitude)
        if state.span_m() > self._threshold_m:
            state.is_mobile = True
            return MOBILE
        return FIXED

    @property
    def sensors_seen(self) -> int:
        return len(self._state)

    def mobile_count(self) -> int:
        return sum(1 for s in self._state.values() if s.is_mobile)
