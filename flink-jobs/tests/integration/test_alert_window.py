"""Event-time integration test for the sustained-high alert window (guidelines §9.1:
a watermark/event-time test with shuffled ``captured_at`` data).

The Flink MiniCluster is not available in CI (PyFlink isn't installed — see the
operator stub-import pattern), so this exercises the *event-time correctness* the
alert operator relies on at the logical level: events are assigned to sliding
windows by their ``captured_at`` (the event-time field, NOT arrival order), then
each window is reduced with ``detect_sustained_high``. The contract under test:
regardless of arrival order, an event lands in the windows its ``captured_at``
belongs to, and a sustained burst of DANGER readings fires the alert on every
overlapping window it spans (which the keyed cooldown dedup later collapses). This
mirrors how Flink's bounded-out-of-orderness watermark (operators/watermark.py)
re-establishes event-time order from a shuffled stream.
"""

from __future__ import annotations

import random
from collections import defaultdict
from datetime import datetime, timezone

from model.radiation_event import _parse_timestamp
from operators.alert_detection import detect_sustained_high
from operators.timestamps import to_epoch_millis

_WINDOW_SECONDS = 300
_SLIDE_SECONDS = 60
_MIN_BREACHES = 3


def _iso(epoch_millis: int) -> str:
    return datetime.fromtimestamp(epoch_millis / 1000, tz=timezone.utc).isoformat()


def _window_alerts(
    events: list[dict],
    window_seconds: int = _WINDOW_SECONDS,
    slide_seconds: int = _SLIDE_SECONDS,
    min_breaches: int = _MIN_BREACHES,
) -> list[dict]:
    """Assign events to sliding event-time windows by captured_at, then reduce each.

    Sliding windows are epoch-aligned like Flink's SlidingEventTimeWindows: a window
    starts at every multiple of ``slide`` and spans ``[s, s + size)``; an event at
    event-time ``t`` belongs to every such window with ``s <= t < s + size``. Returns
    one alert per window that meets the breach threshold, ordered by window start.
    """
    window_ms = window_seconds * 1000
    slide_ms = slide_seconds * 1000
    buckets: dict[int, list[dict]] = defaultdict(list)
    for event in events:
        ts = to_epoch_millis(_parse_timestamp(event["captured_at"]))
        # Every slide-aligned window start in (t - size, t] contains t.
        first_start = ((ts - window_ms) // slide_ms + 1) * slide_ms
        for start in range(first_start, ts + 1, slide_ms):
            buckets[start].append(event)

    alerts = []
    for start in sorted(buckets):
        alert = detect_sustained_high(
            buckets[start],
            sensor_id="sensor-9",
            window_start=_iso(start),
            window_end=_iso(start + window_ms),
            min_breaches=min_breaches,
        )
        if alert is not None:
            alerts.append(alert)
    return alerts


def _event(captured_at: str, cpm: float, classification: str = "SAFE") -> dict:
    return {
        "sensor_id": "sensor-9",
        "captured_at": captured_at,
        "latitude": 37.42,
        "longitude": 141.03,
        "cpm": cpm,
        "classification": classification,
    }


# Three DANGER readings clustered around 01:02–01:04 (a sustained burst), plus
# scattered SAFE/WARN noise — deliberately listed out of captured_at order so
# arrival order never matches event-time order.
_STREAM = [
    _event("2026-06-19 01:03:30", 1500.0, "DANGER"),
    _event("2026-06-19 01:00:10", 40.0, "SAFE"),
    _event("2026-06-19 01:02:15", 1200.0, "DANGER"),
    _event("2026-06-19 01:01:40", 150.0, "WARN"),
    _event("2026-06-19 01:04:05", 1800.0, "DANGER"),
    _event("2026-06-19 01:00:50", 60.0, "SAFE"),
]


class TestAlertWindowEventTime:
    def test_sustained_burst_fires_alert(self) -> None:
        alerts = _window_alerts(_STREAM)
        assert alerts, "a sustained burst of 3 DANGER readings should fire at least one alert"
        for alert in alerts:
            assert alert["reason"] == "sustained-high"
            assert alert["breach_count"] >= _MIN_BREACHES
            assert alert["cpm"] == 1800.0  # peak across the breaching readings

    def test_alert_location_is_latest_breach(self) -> None:
        # The window covering all three breaches reports the 01:04:05 reading's position.
        alerts = _window_alerts(_STREAM)
        full = [a for a in alerts if a["breach_count"] == 3]
        assert full, "expected a window spanning all three breaches"
        # triggered_at passes captured_at through verbatim (producer's space-separated form).
        assert full[0]["triggered_at"] == "2026-06-19 01:04:05"

    def test_overlapping_windows_refire_motivating_dedup(self) -> None:
        # Sliding windows overlap, so the same sustained burst is reported by more
        # than one window — exactly the duplication the cooldown dedup collapses.
        alerts = _window_alerts(_STREAM)
        assert len(alerts) > 1

    def test_transient_spike_does_not_fire(self) -> None:
        # Only two DANGER readings — below the breach threshold, no alert.
        stream = [
            _event("2026-06-19 01:02:00", 1200.0, "DANGER"),
            _event("2026-06-19 01:03:00", 1300.0, "DANGER"),
            _event("2026-06-19 01:01:00", 40.0, "SAFE"),
        ]
        assert _window_alerts(stream) == []

    def test_shuffled_arrival_order_yields_identical_alerts(self) -> None:
        ordered = _window_alerts(sorted(_STREAM, key=lambda e: e["captured_at"]))
        shuffled = list(_STREAM)
        random.Random(2026).shuffle(shuffled)
        assert _window_alerts(shuffled) == ordered
