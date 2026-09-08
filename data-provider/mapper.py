"""
mapper.py — Safecast CSV row dict → RadiationEvent dict.

Converts a raw row dict (str values from csv_reader.stream_rows) into a
dict that conforms to schemas/radiation_event.json and can be published
to the radiation.raw Kafka topic.

Returns None for rows that fail required-field checks so the producer can
skip them without crashing.  The shared SchemaValidator performs a second
authoritative check at publish time — this is a fast pre-filter only.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

logger = logging.getLogger(__name__)

_VALID_UNITS = frozenset({"cpm", "mSv/h", "uSv/h"})


def _to_float(value: str | None) -> float | None:
    if not value or not value.strip():
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _to_str(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def _resolve_sensor_id(row: dict[str, str]) -> str | None:
    sid = _to_str(row.get("sensor_id"))
    if sid:
        return sid
    did = _to_str(row.get("device_id"))
    if did and did != "0":
        return did
    return None


def _normalise_timestamp(raw: str | None) -> str | None:
    if not raw or not raw.strip():
        return None
    ts = raw.strip().replace("Z", "+00:00")
    # Canonical Safecast uses a space between date and time ("2020-06-17 08:02:49");
    # ISO8601 / RFC3339 — and Flink's event-time parsing — expect a 'T' separator.
    if len(ts) > 10 and ts[10] == " ":
        ts = ts[:10] + "T" + ts[11:]
    return ts


def _resolve_timestamp(row: dict[str, str], col: str, fallback: str | None = None) -> str | None:
    ts = _normalise_timestamp(row.get(col))
    if ts:
        return ts
    if fallback:
        return _normalise_timestamp(row.get(fallback))
    return None


def _row_md5(row: dict[str, str]) -> str:
    payload = ",".join(f"{k}={v}" for k, v in sorted(row.items()))
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def _in_range(value: float, lo: float, hi: float) -> bool:
    return lo <= value <= hi


def row_to_event(row: dict[str, str]) -> dict[str, Any] | None:
    """
    Convert one Safecast CSV row dict to a RadiationEvent dict.

    Args:
        row: A dict of raw string values as produced by csv_reader.stream_rows.

    Returns:
        A dict matching schemas/radiation_event.json, or None if the row
        is missing required fields and should be discarded.
    """
    sensor_id = _resolve_sensor_id(row)
    if not sensor_id:
        logger.debug("discard: no usable sensor_id or device_id")
        return None

    captured_at = _resolve_timestamp(row, "captured_at")
    if not captured_at:
        logger.debug("discard: missing captured_at for sensor %s", sensor_id)
        return None

    uploaded_at = _resolve_timestamp(row, "uploaded_at", fallback="captured_at")

    latitude = _to_float(row.get("latitude"))
    longitude = _to_float(row.get("longitude"))
    if latitude is None or longitude is None:
        logger.debug("discard: missing coordinates for sensor %s", sensor_id)
        return None
    if not _in_range(latitude, -90.0, 90.0) or not _in_range(longitude, -180.0, 180.0):
        logger.debug(
            "discard: coordinates out of range (%.4f, %.4f) sensor %s",
            latitude,
            longitude,
            sensor_id,
        )
        return None

    raw_unit = _to_str(row.get("unit"))
    unit = raw_unit if raw_unit in _VALID_UNITS else None
    cpm = _to_float(row.get("value")) if unit is not None else None

    event: dict[str, Any] = {
        "sensor_id": sensor_id,
        "captured_at": captured_at,
        "uploaded_at": uploaded_at,
        "latitude": latitude,
        "longitude": longitude,
        "cpm": cpm,
        "classification": None,
        "location_name": _to_str(row.get("location_name")),
        "height": _to_float(row.get("height")),
        "surface": _to_str(row.get("surface")),
        "loader_id": _to_str(row.get("measurement_import_id")),
        # Prefer Safecast's own measurement hash (its canonical dedup key); fall
        # back to a computed digest for sources/fixtures that lack one.
        "md5sum": _to_str(row.get("md5sum")) or _row_md5(row),
    }
    if unit is not None:
        event["unit"] = unit
    return event
