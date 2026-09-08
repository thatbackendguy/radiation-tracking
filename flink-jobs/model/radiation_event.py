from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

_FRACTION_RE = re.compile(r"\.(\d+)")


def _parse_timestamp(value: str) -> datetime:
    """Parse an M1 timestamp into a naive datetime, portably across Python versions.

    The producer emits naive, space-separated timestamps with variable-precision
    fractional seconds (e.g. ``2026-06-19 01:59:58.05763``). Python 3.10 — the
    version in the Flink image — only accepts 3- or 6-digit fractions in
    ``fromisoformat``, so pad/truncate the fraction to microseconds first. Any
    trailing timezone offset is preserved.
    """
    text = value.strip()
    match = _FRACTION_RE.search(text)
    if match:
        fraction = (match.group(1) + "000000")[:6]
        text = f"{text[: match.start()]}.{fraction}{text[match.end():]}"
    return datetime.fromisoformat(text)


@dataclass
class RadiationEvent:
    """Mirrors schemas/radiation_event.json.

    Instances flow through the Flink pipeline as plain Python objects.
    Use from_dict() after JSON deserialisation; to_dict() before serialisation.
    classification is None on radiation.raw; populated by ThresholdClassifierOperator.
    """

    sensor_id: str
    captured_at: datetime
    uploaded_at: datetime
    latitude: float
    longitude: float
    cpm: Optional[float] = None
    unit: Optional[str] = None
    classification: Optional[str] = None
    location_name: Optional[str] = None
    height: Optional[float] = None
    surface: Optional[str] = None
    md5sum: Optional[str] = None
    loader_id: Optional[str] = None

    @classmethod
    def from_dict(cls, d: dict) -> RadiationEvent:
        # M1 emits naive timestamps, so these parse to naive datetimes. Week-4
        # event-time watermarks on captured_at must assume UTC when assigning them.
        return cls(
            sensor_id=d["sensor_id"],
            captured_at=_parse_timestamp(d["captured_at"]),
            uploaded_at=_parse_timestamp(d["uploaded_at"]),
            latitude=float(d["latitude"]),
            longitude=float(d["longitude"]),
            cpm=d.get("cpm"),
            unit=d.get("unit"),
            classification=d.get("classification"),
            location_name=d.get("location_name"),
            height=d.get("height"),
            surface=d.get("surface"),
            md5sum=d.get("md5sum"),
            loader_id=d.get("loader_id"),
        )

    def to_dict(self) -> dict:
        return {
            "sensor_id": self.sensor_id,
            "captured_at": self.captured_at.isoformat(),
            "uploaded_at": self.uploaded_at.isoformat(),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "cpm": self.cpm,
            "unit": self.unit,
            "classification": self.classification,
            "location_name": self.location_name,
            "height": self.height,
            "surface": self.surface,
            "md5sum": self.md5sum,
            "loader_id": self.loader_id,
        }
