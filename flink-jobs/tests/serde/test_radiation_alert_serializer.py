"""Tests for the radiation.alerts JSON serialiser. Runs without a Flink runtime."""

from __future__ import annotations

import json

from operators.alert_detection import detect_sustained_high
from operators.alert_region_join import enrich_alert_with_region
from operators.trend_detector import build_trend_alert
from serde.radiation_alert_serializer import serialize_alert

_WIN_START = "2026-06-19T01:00:00+00:00"
_WIN_END = "2026-06-19T01:05:00+00:00"


def _alert() -> dict:
    events = [
        {
            "sensor_id": "sensor-9",
            "captured_at": "2026-06-19T01:0%d:00" % i,
            "latitude": 37.42,
            "longitude": 141.03,
            "cpm": 1200.0 + i,
            "classification": "DANGER",
        }
        for i in range(3)
    ]
    return detect_sustained_high(events, "sensor-9", _WIN_START, _WIN_END, min_breaches=3)


class TestSerializeAlert:
    def test_round_trips_through_json(self) -> None:
        alert = _alert()
        assert json.loads(serialize_alert(alert)) == alert

    def test_emits_expected_contract_field_names(self) -> None:
        payload = json.loads(serialize_alert(_alert()))
        assert set(payload) == {
            "sensor_id",
            "reason",
            "window_start",
            "window_end",
            "cpm",
            "breach_count",
            "classification",
            "latitude",
            "longitude",
            "triggered_at",
        }

    def test_backend_required_fields_present(self) -> None:
        # The backend's best-effort alert consumer forwards {sensor_id, cpm, reason};
        # those must always be present and well-typed.
        payload = json.loads(serialize_alert(_alert()))
        assert payload["sensor_id"] == "sensor-9"
        assert payload["reason"] == "sustained-high"
        assert isinstance(payload["cpm"], (int, float))


def _aggregated_blob() -> dict:
    return {
        "geohash": "xn774",
        "window_start": "2026-06-19T01:00:00+00:00",
        "window_end": "2026-06-19T01:01:00+00:00",
        "count": 12,
        "cpm_avg": 41.3,
        "cpm_max": 1850.0,
        "worst_classification": "DANGER",
        "centroid_latitude": 37.4204,
        "centroid_longitude": 141.0331,
    }


class TestSerializeEnrichedAlert:
    def test_hot_region_block_round_trips(self) -> None:
        enriched = enrich_alert_with_region(_alert(), _aggregated_blob())
        payload = json.loads(serialize_alert(enriched))
        assert payload["hot_region"]["cpm_avg"] == 41.3
        assert payload["reason"] == "sustained-high"

    def test_backend_required_fields_survive_enrichment(self) -> None:
        payload = json.loads(serialize_alert(enrich_alert_with_region(_alert(), None)))
        assert payload["sensor_id"] == "sensor-9"
        assert payload["hot_region"] is None


class TestSerializeTrendAlert:
    def test_trend_alert_round_trips(self) -> None:
        alert = build_trend_alert("xn774", [10.0, 20.0, 30.0], _aggregated_blob())
        payload = json.loads(serialize_alert(alert))
        assert payload == alert

    def test_trend_alert_carries_backend_fields(self) -> None:
        payload = json.loads(
            serialize_alert(build_trend_alert("xn774", [10.0, 20.0, 30.0], _aggregated_blob()))
        )
        assert payload["sensor_id"] == "xn774"
        assert payload["reason"] == "rising-trend"
        assert isinstance(payload["cpm"], (int, float))
        assert payload["trend"]["cpm_series"] == [10.0, 20.0, 30.0]
