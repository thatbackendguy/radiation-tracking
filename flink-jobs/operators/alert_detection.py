"""
alert_detection.py — pure reducer for sustained-high-CPM alert detection.

Given the events for one sensor that fall into one sliding event-time window,
``detect_sustained_high`` decides whether the sensor was *sustained-high* in that
window — i.e. it produced at least ``min_breaches`` DANGER-classified readings —
and, if so, produces a single alert blob for the ``radiation.alerts`` topic.

"Sustained" means repeated DANGER readings within the window, not a single spike:
a one-off DANGER reading (below ``min_breaches``) is not an alert. The reducer
reuses the ``classification`` field already set upstream by the broadcast
classifier (operators/classifier.py), so the alert threshold automatically tracks
the user-configured thresholds from ``config.updates`` — there is no second CPM
threshold to keep in sync. The alert's own knob is only *how many* DANGER readings
count as sustained (``min_breaches``).

Kept free of any PyFlink import so it is unit-tested directly;
``alert_window.SustainedHighWindowFunction`` wraps it and supplies the window
bounds. Field names mirror schemas/radiation_alert.json so the Backend (M4)
parses the output as-is.
"""

from __future__ import annotations

import os
from typing import Optional

from operators.classification import DANGER
from operators.timestamps import captured_at_millis

# Minimum number of DANGER readings in a window for the sensor to count as
# sustained-high. Read once at import; override via env for tuning. See
# docs/decisions/ADR-005.
_DEFAULT_MIN_BREACHES = int(os.environ.get("FLINK_ALERT_MIN_BREACHES", "3"))

REASON_SUSTAINED_HIGH = "sustained-high"


def _breach_sort_key(event: dict) -> int:
    """Event-time of a breaching reading, for picking the latest one in a window.

    Falls back to 0 (epoch) when captured_at is unparseable so a single bad
    record cannot crash the pure reducer — the breaching set is already cleaned
    and classified by this point, so this is defensive symmetry with the rest of
    the pipeline.
    """
    try:
        return captured_at_millis(event)
    except (KeyError, ValueError, TypeError):
        return 0


def detect_sustained_high(
    events: list[dict],
    sensor_id: str,
    window_start: str,
    window_end: str,
    min_breaches: int = _DEFAULT_MIN_BREACHES,
) -> Optional[dict]:
    """Reduce one sensor's events in one sliding window into a sustained-high alert.

    ``window_start`` / ``window_end`` are ISO8601 strings supplied by the window
    operator (which converts Flink's epoch-millis bounds). The reducer is otherwise
    pure: it derives the alert from ``events`` and never reads the clock.

    Returns the alert dict when at least ``min_breaches`` DANGER readings are
    present, otherwise ``None`` (the window operator filters None out).
    """
    breaches = [e for e in events if e.get("classification") == DANGER]
    if len(breaches) < min_breaches:
        return None

    # Peak CPM among the breaching readings drives the alert's headline value;
    # the latest breach (by captured_at) supplies the location so the map marks
    # the most recent hotspot position.
    cpm_values = [float(e["cpm"]) for e in breaches if e.get("cpm") is not None]
    latest = max(breaches, key=_breach_sort_key)

    return {
        "sensor_id": sensor_id,
        "reason": REASON_SUSTAINED_HIGH,
        "window_start": window_start,
        "window_end": window_end,
        "cpm": max(cpm_values) if cpm_values else None,
        "breach_count": len(breaches),
        "classification": DANGER,
        "latitude": latest.get("latitude"),
        "longitude": latest.get("longitude"),
        "triggered_at": latest.get("captured_at"),
    }
