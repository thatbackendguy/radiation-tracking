"""Unit tests for the pure geo-bucket reducer (no PyFlink runtime)."""

from __future__ import annotations

from operators.geo_aggregation import aggregate_bucket, worst_classification

_WIN_START = "2026-06-19T01:00:00"
_WIN_END = "2026-06-19T01:01:00"


def _event(
    *,
    cpm: float | None = 50.0,
    classification: str | None = "SAFE",
    lat: float = 35.0,
    lon: float = 139.0,
) -> dict:
    return {
        "sensor_id": "5",
        "latitude": lat,
        "longitude": lon,
        "cpm": cpm,
        "classification": classification,
    }


def _aggregate(events: list[dict]) -> dict:
    return aggregate_bucket(events, "xn774", 5, _WIN_START, _WIN_END)


class TestAggregateBucket:
    def test_count_and_window_bounds(self) -> None:
        result = _aggregate([_event(), _event()])
        assert result["count"] == 2
        assert result["geohash"] == "xn774"
        assert result["precision"] == 5
        assert result["window_start"] == _WIN_START
        assert result["window_end"] == _WIN_END

    def test_cpm_stats(self) -> None:
        result = _aggregate([_event(cpm=10.0), _event(cpm=20.0), _event(cpm=60.0)])
        assert result["cpm_avg"] == 30.0
        assert result["cpm_max"] == 60.0
        assert result["cpm_min"] == 10.0

    def test_per_class_counts(self) -> None:
        events = [
            _event(classification="SAFE"),
            _event(classification="WARN"),
            _event(classification="WARN"),
            _event(classification="DANGER"),
        ]
        assert _aggregate(events)["class_counts"] == {"SAFE": 1, "WARN": 2, "DANGER": 1}

    def test_worst_classification_picks_most_severe(self) -> None:
        events = [
            _event(classification="SAFE"),
            _event(classification="DANGER"),
            _event(classification="WARN"),
        ]
        assert _aggregate(events)["worst_classification"] == "DANGER"

    def test_worst_classification_none_when_unclassified(self) -> None:
        assert worst_classification([_event(classification=None)]) is None
        assert _aggregate([_event(classification=None)])["worst_classification"] is None

    def test_centroid_is_mean_position(self) -> None:
        events = [_event(lat=10.0, lon=20.0), _event(lat=20.0, lon=40.0)]
        result = _aggregate(events)
        assert result["centroid_latitude"] == 15.0
        assert result["centroid_longitude"] == 30.0

    def test_null_cpm_excluded_from_stats_but_counted(self) -> None:
        # cpm is non-null in clean_stream, but the reducer must not crash if one slips
        # through: null readings still count toward the cell but not the cpm stats.
        result = _aggregate([_event(cpm=40.0), _event(cpm=None, classification=None)])
        assert result["count"] == 2
        assert result["cpm_avg"] == 40.0
        assert result["cpm_max"] == 40.0

    def test_single_event(self) -> None:
        result = _aggregate([_event(cpm=12.5, lat=1.0, lon=2.0)])
        assert result["count"] == 1
        assert result["cpm_avg"] == 12.5
        assert result["centroid_latitude"] == 1.0
