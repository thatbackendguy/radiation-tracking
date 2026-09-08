"""
geo_aggregation.py — pure reducer for the geo-bucket window aggregation.

Given the events that fall into one geohash cell within one event-time window,
``aggregate_bucket`` produces a single rich-stats blob for the
``radiation.aggregated`` topic: reading count, CPM avg/max/min, per-class counts,
the worst classification seen, and the cell centroid. Kept free of any PyFlink
import so it is unit-tested directly; ``geo_window.GeoBucketWindowFunction`` wraps
it and supplies the window bounds. Field names mirror
schemas/radiation_aggregated.json so the Backend (M4) parses the output as-is.
"""

from __future__ import annotations

import os
from typing import Optional

from operators.classification import DANGER, SAFE, WARN

# Default event-time window length (seconds) for the tumbling geo aggregation.
# Read once at import; override via env for tuning. See docs/decisions/ADR-004.
_DEFAULT_WINDOW_SECONDS = int(os.environ.get("FLINK_AGG_WINDOW_SECONDS", "60"))

# Severity order, worst first — used to pick the worst classification in a cell.
_SEVERITY_ORDER = (DANGER, WARN, SAFE)


def worst_classification(events: list[dict]) -> Optional[str]:
    """Return the most severe classification among ``events`` (DANGER > WARN > SAFE).

    Returns None when no event carries a known classification — e.g. an empty
    window or events whose ``classification`` was never set.
    """
    present = {e.get("classification") for e in events}
    for label in _SEVERITY_ORDER:
        if label in present:
            return label
    return None


def aggregate_bucket(
    events: list[dict],
    geohash: str,
    precision: int,
    window_start: str,
    window_end: str,
) -> dict:
    """Reduce one geohash cell's events in one window into a radiation.aggregated blob.

    ``window_start`` / ``window_end`` are ISO8601 strings supplied by the window
    operator (which converts Flink's epoch-millis bounds). The reducer is otherwise
    pure: it derives every statistic from ``events`` and never reads the clock.
    """
    cpm_values = [float(e["cpm"]) for e in events if e.get("cpm") is not None]
    class_counts = {SAFE: 0, WARN: 0, DANGER: 0}
    for event in events:
        label = event.get("classification")
        if label in class_counts:
            class_counts[label] += 1

    latitudes = [float(e["latitude"]) for e in events]
    longitudes = [float(e["longitude"]) for e in events]

    return {
        "geohash": geohash,
        "precision": precision,
        "window_start": window_start,
        "window_end": window_end,
        "count": len(events),
        "cpm_avg": round(sum(cpm_values) / len(cpm_values), 4) if cpm_values else None,
        "cpm_max": max(cpm_values) if cpm_values else None,
        "cpm_min": min(cpm_values) if cpm_values else None,
        "class_counts": class_counts,
        "worst_classification": worst_classification(events),
        "centroid_latitude": round(sum(latitudes) / len(latitudes), 6) if latitudes else None,
        "centroid_longitude": round(sum(longitudes) / len(longitudes), 6) if longitudes else None,
    }
