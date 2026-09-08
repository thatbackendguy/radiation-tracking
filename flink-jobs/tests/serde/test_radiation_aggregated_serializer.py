"""Tests for the radiation.aggregated JSON serialiser. Runs without a Flink runtime."""

from __future__ import annotations

import json

from operators.geo_aggregation import aggregate_bucket
from operators.rolling_stats import enrich_blob
from serde.radiation_aggregated_serializer import serialize_aggregated


def _bucket() -> dict:
    events = [
        {"latitude": 35.0, "longitude": 139.0, "cpm": 40.0, "classification": "SAFE"},
        {"latitude": 35.0, "longitude": 139.0, "cpm": 300.0, "classification": "DANGER"},
    ]
    return aggregate_bucket(
        events, "xn774", 5, "2026-06-19T01:00:00+00:00", "2026-06-19T01:01:00+00:00"
    )


class TestSerializeAggregated:
    def test_round_trips_through_json(self) -> None:
        aggregated = _bucket()
        assert json.loads(serialize_aggregated(aggregated)) == aggregated

    def test_emits_expected_contract_field_names(self) -> None:
        payload = json.loads(serialize_aggregated(_bucket()))
        assert set(payload) == {
            "geohash",
            "precision",
            "window_start",
            "window_end",
            "count",
            "cpm_avg",
            "cpm_max",
            "cpm_min",
            "class_counts",
            "worst_classification",
            "centroid_latitude",
            "centroid_longitude",
        }

    def test_nested_class_counts_survive(self) -> None:
        payload = json.loads(serialize_aggregated(_bucket()))
        assert payload["class_counts"] == {"SAFE": 1, "WARN": 0, "DANGER": 1}
        assert payload["worst_classification"] == "DANGER"

    def test_rolling_stats_enrichment_survives(self) -> None:
        # the enrichment fields (rolling_cpm_avg / cpm_zscore / anomaly) must round-trip
        # so the Backend (M4) receives them on radiation.aggregated
        enriched = enrich_blob(_bucket(), [8.0, 12.0])
        payload = json.loads(serialize_aggregated(enriched))
        assert payload["rolling_cpm_avg"] == enriched["rolling_cpm_avg"]
        assert payload["cpm_zscore"] == enriched["cpm_zscore"]
        assert payload["anomaly"] == enriched["anomaly"]
