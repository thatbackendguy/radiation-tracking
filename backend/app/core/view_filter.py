"""Per-client view filtering (REMAP model, ADR-017).

A client's area/timespan filters are *view state*, not pipeline config: they are
applied on the read path — `GET /recent` query params and a `subscribe` message
on `/ws/stream` — so each client narrows only its own stream. The global
pipeline config (`POST /config`) no longer carries them.

Matching is **fail-open**: an event that lacks the fields a rule needs (or has
an unparseable timestamp) is delivered rather than hidden. The frontend applies
the same filters locally, so fail-open can never show wrong data — but
fail-closed could silently swallow alerts on a payload-shape drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from app.models import RadiationEvent

# Envelope-type → (lat field, lon field, timestamp fields tried in order).
_MESSAGE_FIELDS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "clean": ("latitude", "longitude", ("captured_at",)),
    "aggregated": ("centroid_latitude", "centroid_longitude", ("window_start",)),
    "alert": ("latitude", "longitude", ("window_start", "triggered_at")),
}


def _as_utc(value: datetime) -> datetime:
    """Normalise to aware-UTC so naive and offset timestamps compare safely."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_ts(value: object) -> datetime | None:
    """Parse an ISO-8601 string (tolerating a trailing 'Z') to aware UTC."""
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


@dataclass(frozen=True)
class ViewFilter:
    """An optional bounding box + time range restricting one client's view."""

    min_lat: float | None = None
    max_lat: float | None = None
    min_lon: float | None = None
    max_lon: float | None = None
    start: datetime | None = None
    end: datetime | None = None

    def __post_init__(self) -> None:
        area = (self.min_lat, self.max_lat, self.min_lon, self.max_lon)
        if any(v is not None for v in area) and not all(v is not None for v in area):
            raise ValueError("area filter requires all of min_lat, max_lat, min_lon, max_lon")
        if self.min_lat is not None:
            if not (-90.0 <= self.min_lat < self.max_lat <= 90.0):
                raise ValueError("latitude bounds must satisfy -90 <= min_lat < max_lat <= 90")
            if not (-180.0 <= self.min_lon < self.max_lon <= 180.0):
                raise ValueError("longitude bounds must satisfy -180 <= min_lon < max_lon <= 180")
        if (self.start is None) != (self.end is None):
            raise ValueError("time filter requires both start and end")
        if self.start is not None and _as_utc(self.start) >= _as_utc(self.end):
            raise ValueError("start must be before end")
        # Normalise timestamps once so matching never mixes naive and aware.
        object.__setattr__(self, "start", _as_utc(self.start) if self.start else None)
        object.__setattr__(self, "end", _as_utc(self.end) if self.end else None)

    @property
    def has_area(self) -> bool:
        return self.min_lat is not None

    @property
    def has_timespan(self) -> bool:
        return self.start is not None

    @property
    def is_noop(self) -> bool:
        return not (self.has_area or self.has_timespan)

    @classmethod
    def from_subscribe(cls, payload: dict) -> "ViewFilter":
        """Build a filter from a `/ws/stream` subscribe message.

        Shape (all parts optional; null/omitted clears that rule):
        ``{"type": "subscribe", "area": {min_lat, max_lat, min_lon, max_lon},
        "timespan": {"start": iso, "end": iso}}``. Raises ValueError on any
        malformed part — the caller keeps the previous filter.
        """
        area = payload.get("area") or {}
        timespan = payload.get("timespan") or {}
        if not isinstance(area, dict) or not isinstance(timespan, dict):
            raise ValueError("area and timespan must be objects")

        def _num(obj: dict, key: str) -> float | None:
            value = obj.get(key)
            if value is None:
                return None
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{key} must be a number")
            return float(value)

        def _ts(obj: dict, key: str) -> datetime | None:
            value = obj.get(key)
            if value is None or value == "":
                return None
            parsed = _parse_ts(value)
            if parsed is None:
                raise ValueError(f"{key} must be an ISO-8601 timestamp")
            return parsed

        return cls(
            min_lat=_num(area, "min_lat"),
            max_lat=_num(area, "max_lat"),
            min_lon=_num(area, "min_lon"),
            max_lon=_num(area, "max_lon"),
            start=_ts(timespan, "start"),
            end=_ts(timespan, "end"),
        )

    def _in_area(self, lat: object, lon: object) -> bool:
        if not self.has_area:
            return True
        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            return True  # fail-open: cannot judge, deliver
        return self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon

    def _in_timespan(self, *timestamps: object) -> bool:
        if not self.has_timespan:
            return True
        for raw in timestamps:
            parsed = _parse_ts(raw)
            if parsed is not None:
                return self.start <= parsed <= self.end
        return True  # fail-open: no parseable timestamp, deliver

    def matches_event(self, event: RadiationEvent) -> bool:
        """Predicate for ring-buffer events (`GET /recent`)."""
        return self._in_area(event.latitude, event.longitude) and self._in_timespan(
            event.captured_at
        )

    def matches_message(self, message: dict) -> bool:
        """Predicate for broadcaster envelopes (`/ws/stream`)."""
        fields = _MESSAGE_FIELDS.get(message.get("type"))
        if fields is None:
            return True  # unknown envelope types always pass through
        lat_key, lon_key, ts_keys = fields
        data = message.get("data")
        if not isinstance(data, dict):
            return True
        return self._in_area(data.get(lat_key), data.get(lon_key)) and self._in_timespan(
            *(data.get(k) for k in ts_keys)
        )
