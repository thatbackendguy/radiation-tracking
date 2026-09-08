"""Unit tests for the pure row-mapping + SQL of the aggregated sink."""

import pytest

from aggregated_sink.db import (
    COLUMNS,
    UPSERT_SQL,
    build_rows,
    record_to_row,
)


def _full_record(**overrides):
    record = {
        "geohash": "xn774c",
        "precision": 6,
        "window_start": "2011-03-11T05:00:00+00:00",
        "window_end": "2011-03-11T05:01:00+00:00",
        "count": 12,
        "cpm_avg": 44.5,
        "cpm_max": 91.0,
        "cpm_min": 20.0,
        "class_counts": {"SAFE": 10, "WARN": 1, "DANGER": 1},
        "worst_classification": "DANGER",
        "centroid_latitude": 37.75,
        "centroid_longitude": 140.47,
        "rolling_cpm_avg": 41.2,
        "cpm_zscore": 1.3,
        "anomaly": False,
    }
    record.update(overrides)
    return record


class TestRecordToRow:
    def test_maps_all_columns_in_order(self):
        row = record_to_row(_full_record())
        assert len(row) == len(COLUMNS)
        by_col = dict(zip(COLUMNS, row))
        assert by_col["geohash"] == "xn774c"
        assert by_col["window_start"] == "2011-03-11T05:00:00+00:00"
        assert by_col["count"] == 12
        assert by_col["safe_count"] == 10
        assert by_col["warn_count"] == 1
        assert by_col["danger_count"] == 1
        assert by_col["worst_classification"] == "DANGER"
        assert by_col["anomaly"] is False

    def test_optional_fields_default_to_none(self):
        # A minimal valid record — only the required keys present.
        minimal = {"geohash": "abc", "window_start": "2026-01-01T00:00:00+00:00", "count": 0}
        by_col = dict(zip(COLUMNS, record_to_row(minimal)))
        assert by_col["cpm_avg"] is None
        assert by_col["centroid_latitude"] is None
        assert by_col["rolling_cpm_avg"] is None
        assert by_col["safe_count"] is None  # no class_counts → None, not 0

    def test_missing_geohash_raises(self):
        with pytest.raises(ValueError, match="missing"):
            record_to_row({"window_start": "2026-01-01T00:00:00+00:00", "count": 1})

    def test_missing_window_start_raises(self):
        with pytest.raises(ValueError, match="missing"):
            record_to_row({"geohash": "abc", "count": 1})

    def test_missing_count_raises(self):
        with pytest.raises(ValueError, match="missing"):
            record_to_row({"geohash": "abc", "window_start": "2026-01-01T00:00:00+00:00"})

    def test_count_zero_is_valid(self):
        # count == 0 is a real empty cell, not "missing".
        row = record_to_row(
            {"geohash": "abc", "window_start": "2026-01-01T00:00:00+00:00", "count": 0}
        )
        assert dict(zip(COLUMNS, row))["count"] == 0

    def test_non_dict_raises(self):
        with pytest.raises(ValueError):
            record_to_row(["not", "a", "dict"])


class TestBuildRows:
    def test_skips_malformed_keeps_valid(self):
        records = [
            _full_record(geohash="a"),
            {"count": 1},  # missing geohash + window_start → skipped
            _full_record(geohash="b"),
        ]
        rows = build_rows(records)
        assert len(rows) == 2
        assert [r[0] for r in rows] == ["a", "b"]


class TestUpsertSql:
    def test_is_idempotent_upsert_on_the_key(self):
        assert "INSERT INTO aggregated_blobs" in UPSERT_SQL
        assert "ON CONFLICT (geohash, window_start)" in UPSERT_SQL
        assert "DO UPDATE SET" in UPSERT_SQL
        # The key columns are never in the SET clause.
        assert "geohash = EXCLUDED.geohash" not in UPSERT_SQL
        assert "window_start = EXCLUDED.window_start" not in UPSERT_SQL
        # A representative non-key column is updated.
        assert "cpm_avg = EXCLUDED.cpm_avg" in UPSERT_SQL
