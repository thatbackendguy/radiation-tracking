"""Unit tests for the rising-CPM trend detector. The Flink runtime is absent in CI, so the
per-geohash ValueState is replaced with a simple fake and process_element is driven
directly — mirroring the live graph where the stream is keyed by geohash, so each cell owns
a separate series.
"""

from __future__ import annotations

import json

from operators.trend_detector import (
    REASON_RISING_TREND,
    TrendDetectorOperator,
    build_trend_alert,
    is_rising,
    push_window,
)


class _FakeValueState:
    """Stand-in for pyflink ValueState: the value/update/clear surface the operator uses."""

    def __init__(self) -> None:
        self._v = None

    def value(self):
        return self._v

    def update(self, value) -> None:
        self._v = value

    def clear(self) -> None:
        self._v = None


def _blob(geohash: str, cpm_avg, **overrides) -> dict:
    blob = {
        "geohash": geohash,
        "precision": 5,
        "window_start": "2026-06-19T01:00:00+00:00",
        "window_end": "2026-06-19T01:01:00+00:00",
        "count": 12,
        "cpm_avg": cpm_avg,
        "cpm_max": (cpm_avg or 0) + 50,
        "cpm_min": 8.0,
        "class_counts": {"SAFE": 12, "WARN": 0, "DANGER": 0},
        "worst_classification": "SAFE",
        "centroid_latitude": 37.4204,
        "centroid_longitude": 141.0331,
    }
    blob.update(overrides)
    return blob


class TestPushWindow:
    def test_appends_and_trims_to_max_len(self) -> None:
        assert push_window([10.0, 20.0, 30.0], 40.0, max_len=3) == [20.0, 30.0, 40.0]

    def test_below_max_len_keeps_all(self) -> None:
        assert push_window([10.0], 20.0, max_len=3) == [10.0, 20.0]


class TestIsRising:
    def test_strictly_rising_series_fires(self) -> None:
        assert is_rising([10.0, 20.0, 30.0], min_windows=3, min_delta=0) is True

    def test_flat_series_does_not_fire(self) -> None:
        assert is_rising([10.0, 10.0, 10.0], min_windows=3, min_delta=0) is False

    def test_declining_series_does_not_fire(self) -> None:
        assert is_rising([30.0, 20.0, 10.0], min_windows=3, min_delta=0) is False

    def test_dip_in_middle_breaks_trend(self) -> None:
        assert is_rising([10.0, 5.0, 30.0], min_windows=3, min_delta=0) is False

    def test_too_few_windows_does_not_fire(self) -> None:
        assert is_rising([10.0, 20.0], min_windows=3, min_delta=0) is False

    def test_rise_below_min_delta_does_not_fire(self) -> None:
        # Strictly rising but total rise (30 − 10 = 20) is under the 50 threshold.
        assert is_rising([10.0, 20.0, 30.0], min_windows=3, min_delta=50) is False

    def test_only_last_n_windows_considered(self) -> None:
        # A leading dip is irrelevant once it falls outside the last min_windows samples.
        assert is_rising([99.0, 10.0, 20.0, 30.0], min_windows=3, min_delta=0) is True

    def test_trivial_jitter_below_default_delta_does_not_fire(self) -> None:
        # Sub-CPM sensor jitter is strictly rising but must not alert under the shipped
        # non-zero default — the exact case the review flagged.
        from operators import trend_detector

        assert (
            is_rising([10.00, 10.01, 10.02], min_delta=trend_detector._DEFAULT_MIN_DELTA) is False
        )


class TestShippedDefaults:
    def test_min_delta_default_is_non_zero(self) -> None:
        # A 0 default disables the magnitude gate entirely (see ADR-008).
        from operators import trend_detector

        assert trend_detector._DEFAULT_MIN_DELTA > 0

    def test_min_windows_default_floored_at_two(self) -> None:
        # A "rise" needs ≥ 2 samples; a stray FLINK_TREND_WINDOWS=1 must not slip through.
        from operators import trend_detector

        assert trend_detector._DEFAULT_MIN_WINDOWS >= 2


class TestBuildTrendAlert:
    def test_carries_geohash_reason_and_series(self) -> None:
        alert = build_trend_alert("xn774", [10.0, 20.0, 30.0], _blob("xn774", 30.0))
        assert alert["sensor_id"] == "xn774"
        assert alert["geohash"] == "xn774"
        assert alert["reason"] == REASON_RISING_TREND
        assert alert["cpm"] == _blob("xn774", 30.0)["cpm_max"]
        assert alert["trend"] == {
            "window_count": 3,
            "cpm_series": [10.0, 20.0, 30.0],
            "delta": 20.0,
        }

    def test_location_is_cell_centroid(self) -> None:
        alert = build_trend_alert("xn774", [10.0, 20.0, 30.0], _blob("xn774", 30.0))
        assert alert["latitude"] == 37.4204
        assert alert["longitude"] == 141.0331

    def test_no_danger_classification_field(self) -> None:
        # A trend is not a DANGER breach; classification is omitted (schema enum is DANGER).
        alert = build_trend_alert("xn774", [10.0, 20.0, 30.0], _blob("xn774", 30.0))
        assert "classification" not in alert

    def test_breach_count_is_number_of_rising_windows(self) -> None:
        # The map popup renders "Breaches:" for every alert kind (2026-07-03 main review);
        # for a trend the count of rising windows is the analogous evidence count.
        alert = build_trend_alert("xn774", [10.0, 20.0, 30.0], _blob("xn774", 30.0))
        assert alert["breach_count"] == 3
        assert alert["breach_count"] == alert["trend"]["window_count"]

    def test_triggered_at_is_latest_window_end(self) -> None:
        # The popup's "Time:" field; the trend is confirmed at the latest window's end.
        alert = build_trend_alert("xn774", [10.0, 20.0, 30.0], _blob("xn774", 30.0))
        assert alert["triggered_at"] == "2026-06-19T01:01:00+00:00"
        assert alert["triggered_at"] == alert["window_end"]


class TestProcessElement:
    def _operator(self) -> TrendDetectorOperator:
        op = TrendDetectorOperator(min_windows=3, min_delta=0)
        op._series = _FakeValueState()
        return op

    def _run(self, op: TrendDetectorOperator, blob: dict) -> list[dict]:
        return list(op.process_element(blob, ctx=None))

    def test_fires_after_three_rising_windows(self) -> None:
        op = self._operator()
        assert self._run(op, _blob("xn774", 10.0)) == []
        assert self._run(op, _blob("xn774", 20.0)) == []
        out = self._run(op, _blob("xn774", 30.0))
        assert len(out) == 1
        assert out[0]["reason"] == REASON_RISING_TREND

    def test_does_not_fire_while_flat(self) -> None:
        op = self._operator()
        self._run(op, _blob("xn774", 10.0))
        self._run(op, _blob("xn774", 10.0))
        assert self._run(op, _blob("xn774", 10.0)) == []

    def test_resets_after_firing_so_no_immediate_refire(self) -> None:
        op = self._operator()
        self._run(op, _blob("xn774", 10.0))
        self._run(op, _blob("xn774", 20.0))
        assert self._run(op, _blob("xn774", 30.0)) != []  # fires
        # Series cleared; one more rising window is not yet three, so no re-fire.
        assert self._run(op, _blob("xn774", 40.0)) == []

    def test_empty_window_avg_is_skipped(self) -> None:
        op = self._operator()
        self._run(op, _blob("xn774", 10.0))
        self._run(op, _blob("xn774", None))  # empty window — no average to trend
        self._run(op, _blob("xn774", 20.0))
        # Only two real samples seen, so no alert yet.
        assert self._run(op, _blob("xn774", None)) == []

    def test_series_persisted_as_json(self) -> None:
        op = self._operator()
        self._run(op, _blob("xn774", 10.0))
        assert json.loads(op._series.value()) == [10.0]
