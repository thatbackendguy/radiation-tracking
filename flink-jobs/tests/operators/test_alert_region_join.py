"""Unit tests for the alerts × aggregated hot-region join. The Flink runtime is absent in
CI, so the per-geohash ValueState is replaced with a simple fake and process_element1/2
are driven directly — mirroring the live graph where both inputs are keyed by geohash, so
each cell owns a separate ValueState.
"""

from __future__ import annotations

import json

from operators.alert_region_join import (
    AlertRegionJoinFunction,
    _with_alert_geohash,
    alert_geohash,
    enrich_alert_with_region,
    region_summary,
)


class _FakeValueState:
    """Stand-in for pyflink ValueState: just the value/update surface the operator uses."""

    def __init__(self) -> None:
        self._v = None

    def value(self):
        return self._v

    def update(self, value) -> None:
        self._v = value


def _alert(*, lat=37.42, lon=141.03, sensor_id="sensor-9") -> dict:
    return {
        "sensor_id": sensor_id,
        "reason": "sustained-high",
        "window_start": "2026-06-19T01:00:00+00:00",
        "window_end": "2026-06-19T01:05:00+00:00",
        "cpm": 1850.0,
        "breach_count": 4,
        "classification": "DANGER",
        "latitude": lat,
        "longitude": lon,
        "triggered_at": "2026-06-19T01:04:05+00:00",
    }


def _blob(geohash: str, **overrides) -> dict:
    blob = {
        "geohash": geohash,
        "precision": 5,
        "window_start": "2026-06-19T01:00:00+00:00",
        "window_end": "2026-06-19T01:01:00+00:00",
        "count": 12,
        "cpm_avg": 41.3,
        "cpm_max": 1850.0,
        "cpm_min": 8.0,
        "class_counts": {"SAFE": 10, "WARN": 1, "DANGER": 1},
        "worst_classification": "DANGER",
        "centroid_latitude": 37.4204,
        "centroid_longitude": 141.0331,
    }
    blob.update(overrides)
    return blob


class TestAlertGeohash:
    def test_hashes_coordinates_to_aggregation_precision(self) -> None:
        gh = alert_geohash(_alert())
        # Default precision is 5 (FLINK_GEOHASH_PRECISION) — must match the aggregation key.
        assert gh is not None
        assert len(gh) == 5

    def test_alert_and_blob_centroid_share_the_same_cell(self) -> None:
        # A breach and its cell centroid are in the same ~5 km cell, so they key together.
        assert alert_geohash(_alert(lat=37.4204, lon=141.0331)) == alert_geohash(_alert())

    def test_null_latitude_returns_none(self) -> None:
        assert alert_geohash(_alert(lat=None)) is None

    def test_null_longitude_returns_none(self) -> None:
        assert alert_geohash(_alert(lon=None)) is None


class TestEnrichAlertWithRegion:
    def test_region_present_adds_hot_region_summary(self) -> None:
        out = enrich_alert_with_region(_alert(), _blob("xn774"))
        assert out["hot_region"] == region_summary(_blob("xn774"))
        assert out["hot_region"]["cpm_avg"] == 41.3
        assert out["hot_region"]["worst_classification"] == "DANGER"

    def test_region_absent_sets_hot_region_none(self) -> None:
        out = enrich_alert_with_region(_alert(), None)
        assert out["hot_region"] is None

    def test_does_not_mutate_input(self) -> None:
        alert = _alert()
        enrich_alert_with_region(alert, _blob("xn774"))
        assert "hot_region" not in alert

    def test_region_summary_only_carries_cell_stats(self) -> None:
        # window bounds / precision stay on the aggregated topic, not on the alert.
        summary = region_summary(_blob("xn774"))
        assert set(summary) == {
            "cpm_avg",
            "cpm_max",
            "count",
            "worst_classification",
            "centroid_latitude",
            "centroid_longitude",
        }


class TestWithAlertGeohash:
    def test_adds_real_geohash_field(self) -> None:
        out = _with_alert_geohash(_alert())
        assert out["geohash"] == alert_geohash(_alert())

    def test_null_coordinates_leave_geohash_none(self) -> None:
        out = _with_alert_geohash(_alert(lat=None))
        assert out["geohash"] is None


class TestProcessElements:
    def _operator(self) -> AlertRegionJoinFunction:
        op = AlertRegionJoinFunction()
        op._latest_blob = _FakeValueState()
        return op

    def test_alert_after_blob_is_enriched(self) -> None:
        op = self._operator()
        gh = alert_geohash(_alert())
        list(op.process_element2(_blob(gh), ctx=None))  # cell stats arrive first
        [out] = list(op.process_element1(_with_alert_geohash(_alert()), ctx=None))
        assert out["hot_region"]["cpm_avg"] == 41.3

    def test_alert_before_any_blob_has_null_hot_region(self) -> None:
        op = self._operator()
        [out] = list(op.process_element1(_with_alert_geohash(_alert()), ctx=None))
        assert out["hot_region"] is None

    def test_latest_blob_wins(self) -> None:
        op = self._operator()
        gh = alert_geohash(_alert())
        list(op.process_element2(_blob(gh, cpm_avg=41.3), ctx=None))
        list(op.process_element2(_blob(gh, cpm_avg=99.9), ctx=None))
        [out] = list(op.process_element1(_with_alert_geohash(_alert()), ctx=None))
        assert out["hot_region"]["cpm_avg"] == 99.9

    def test_blob_stored_as_json_string(self) -> None:
        op = self._operator()
        gh = alert_geohash(_alert())
        list(op.process_element2(_blob(gh), ctx=None))
        # State holds a serialised blob the next alert deserialises — round-trips cleanly.
        assert json.loads(op._latest_blob.value())["geohash"] == gh

    def test_process_element2_emits_nothing(self) -> None:
        op = self._operator()
        assert list(op.process_element2(_blob("xn774"), ctx=None)) == []
