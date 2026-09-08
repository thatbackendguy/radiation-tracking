"""
timestamps.py — pure event-time extraction from ``captured_at``.

The Flink job uses ``captured_at`` (when the measurement was taken) as event
time, NOT ``uploaded_at`` (which only governs producer ordering). This module
holds the pure conversion so it can be unit-tested without PyFlink; the
``TimestampAssigner`` / ``WatermarkStrategy`` in ``watermark.py`` call into it.

Parsing reuses ``model.radiation_event._parse_timestamp`` so it accepts the
producer's space-separated, variable-precision timestamps (e.g.
``2026-06-19 01:59:58.05763``) that Python 3.10's bare ``fromisoformat``
rejects. M1 emits naive timestamps, so a value without an offset is treated as
UTC when converting to epoch millis.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from model.radiation_event import _parse_timestamp


def to_epoch_millis(dt: datetime) -> int:
    """Convert a datetime to epoch milliseconds (Flink's event-time unit).

    Naive datetimes (no tzinfo) are assumed to be UTC — M1 emits naive
    timestamps, and event time must be tz-stable regardless of the worker's
    local zone.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def captured_at_millis(event: dict) -> int:
    """Return the ``captured_at`` epoch millis of an already-parsed event dict.

    Used to re-derive event time downstream of the source — e.g. the geo
    aggregation re-assigns watermarks on the (dict) clean stream, since the
    classifier's broadcast connect pins the propagated watermark otherwise.
    """
    return to_epoch_millis(_parse_timestamp(event["captured_at"]))


def extract_captured_at_millis(raw_event_json: str) -> int:
    """Parse a radiation-event JSON string and return its ``captured_at`` epoch millis."""
    return captured_at_millis(json.loads(raw_event_json))
