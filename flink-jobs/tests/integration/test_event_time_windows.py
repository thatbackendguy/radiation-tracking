"""Event-time integration test for the geo-bucket aggregation (guidelines §9.1: a
watermark/event-time test with shuffled ``captured_at`` data).

The Flink MiniCluster is not available in CI (PyFlink isn't installed — see the
operator stub-import pattern), so this exercises the *event-time correctness* the
job relies on at the logical level: events are assigned to tumbling windows by their
``captured_at`` (the event-time field, NOT arrival order), then reduced with
``aggregate_bucket``. The contract under test: regardless of the order events arrive
in, an event lands in the window its ``captured_at`` belongs to and each window's
aggregate is identical. This mirrors how Flink's bounded-out-of-orderness watermark
(operators/watermark.py) re-establishes event-time order from a shuffled stream.
"""

from __future__ import annotations

import random
from collections import defaultdict
from datetime import datetime, timezone

from model.radiation_event import _parse_timestamp
from operators.geo_aggregation import aggregate_bucket
from operators.timestamps import to_epoch_millis

_GEOHASH = "xn774"
_PRECISION = 5
_WINDOW_SECONDS = 60


def _iso(epoch_millis: int) -> str:
    return datetime.fromtimestamp(epoch_millis / 1000, tz=timezone.utc).isoformat()


def _window_aggregates(events: list[dict], window_seconds: int = _WINDOW_SECONDS) -> list[dict]:
    """Assign events to tumbling event-time windows by captured_at, then reduce each.

    Tumbling windows are epoch-aligned, matching Flink's TumblingEventTimeWindows:
    an event at event-time ``t`` falls in ``[floor(t/W)*W, floor(t/W)*W + W)``.
    Returns one aggregate per non-empty window, ordered by window start.
    """
    window_ms = window_seconds * 1000
    buckets: dict[int, list[dict]] = defaultdict(list)
    for event in events:
        ts = to_epoch_millis(_parse_timestamp(event["captured_at"]))
        window_start = (ts // window_ms) * window_ms
        buckets[window_start].append(event)

    return [
        aggregate_bucket(buckets[ws], _GEOHASH, _PRECISION, _iso(ws), _iso(ws + window_ms))
        for ws in sorted(buckets)
    ]


def _event(captured_at: str, cpm: float, classification: str = "SAFE") -> dict:
    return {
        "sensor_id": "5",
        "captured_at": captured_at,
        "latitude": 35.0,
        "longitude": 139.0,
        "cpm": cpm,
        "classification": classification,
    }


# Two readings in the 01:00:00 window, three in the 01:01:00 window — deliberately
# listed out of captured_at order so arrival order never matches event-time order.
_STREAM = [
    _event("2026-06-19 01:01:10", 300.0, "DANGER"),
    _event("2026-06-19 01:00:50", 40.0, "SAFE"),
    _event("2026-06-19 01:01:30", 120.0, "WARN"),
    _event("2026-06-19 01:00:05", 60.0, "SAFE"),
    _event("2026-06-19 01:01:55", 80.0, "SAFE"),
]


class TestEventTimeWindowing:
    def test_events_land_in_window_of_their_captured_at(self) -> None:
        windows = _window_aggregates(_STREAM)
        assert len(windows) == 2
        first, second = windows
        assert first["window_start"].startswith("2026-06-19T01:00:00")
        assert first["count"] == 2  # the two 01:00 readings
        assert second["window_start"].startswith("2026-06-19T01:01:00")
        assert second["count"] == 3  # the three 01:01 readings

    def test_per_window_aggregates_use_event_time_membership(self) -> None:
        first, second = _window_aggregates(_STREAM)
        # 01:00 window: cpm 40 + 60
        assert first["cpm_max"] == 60.0
        assert first["worst_classification"] == "SAFE"
        # 01:01 window: cpm 300 + 120 + 80, DANGER present
        assert second["cpm_max"] == 300.0
        assert second["worst_classification"] == "DANGER"
        assert second["class_counts"] == {"SAFE": 1, "WARN": 1, "DANGER": 1}

    def test_shuffled_arrival_order_yields_identical_windows(self) -> None:
        ordered = _window_aggregates(sorted(_STREAM, key=lambda e: e["captured_at"]))
        shuffled_stream = list(_STREAM)
        random.Random(1234).shuffle(shuffled_stream)
        assert _window_aggregates(shuffled_stream) == ordered

    def test_out_of_order_within_a_window_is_order_independent(self) -> None:
        same_window = [
            _event("2026-06-19 01:00:55", 90.0, "SAFE"),
            _event("2026-06-19 01:00:01", 10.0, "SAFE"),
            _event("2026-06-19 01:00:30", 50.0, "SAFE"),
        ]
        forward = _window_aggregates(same_window)
        reverse = _window_aggregates(list(reversed(same_window)))
        assert len(forward) == 1
        assert forward == reverse
