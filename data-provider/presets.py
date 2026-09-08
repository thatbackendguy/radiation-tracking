"""
presets.py — Named time-window presets for backfill demos.

Each entry maps a preset name to (start_ts, end_ts) in ISO8601 format.
The Fukushima window covers the disaster onset through the first month of
elevated Safecast field measurements.
"""

from __future__ import annotations

PRESETS: dict[str, tuple[str, str]] = {
    "fukushima": ("2011-03-11T00:00:00", "2011-04-30T23:59:59"),
}
