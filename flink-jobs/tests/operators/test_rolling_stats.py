"""Unit tests for the rolling-average / z-score enrichment helpers (no PyFlink runtime)."""

from __future__ import annotations

from operators.rolling_stats import (
    enrich_blob,
    flag_anomaly,
    push_sample,
    rolling_average,
    zscore,
)


def _blob(*, cpm_avg: float | None = 50.0, geohash: str = "xn774") -> dict:
    return {
        "geohash": geohash,
        "precision": 5,
        "window_start": "2026-07-01T01:00:00",
        "window_end": "2026-07-01T01:01:00",
        "count": 3,
        "cpm_avg": cpm_avg,
    }


class TestPushSample:
    def test_appends_within_capacity(self) -> None:
        assert push_sample([1.0, 2.0], 3.0, 6) == [1.0, 2.0, 3.0]

    def test_trims_oldest_beyond_capacity(self) -> None:
        assert push_sample([1.0, 2.0, 3.0], 4.0, 3) == [2.0, 3.0, 4.0]

    def test_from_empty(self) -> None:
        assert push_sample([], 9.0, 3) == [9.0]


class TestRollingAverage:
    def test_none_when_empty(self) -> None:
        assert rolling_average([]) is None

    def test_mean_rounded(self) -> None:
        assert rolling_average([10.0, 20.0, 30.0]) == 20.0

    def test_rounds_to_four_places(self) -> None:
        assert rolling_average([1.0, 2.0]) == 1.5


class TestZscore:
    def test_none_below_two_samples(self) -> None:
        assert zscore([], 5.0) is None
        assert zscore([5.0], 5.0) is None

    def test_none_on_flat_baseline(self) -> None:
        # zero variance → deviation undefined, not a divide-by-zero
        assert zscore([10.0, 10.0, 10.0], 50.0) is None

    def test_positive_spike(self) -> None:
        # baseline [8, 12] mean 10, population std 2 → (16-10)/2 = 3.0
        assert zscore([8.0, 12.0], 16.0) == 3.0

    def test_negative_dip_is_signed(self) -> None:
        assert zscore([8.0, 12.0], 4.0) == -3.0


class TestFlagAnomaly:
    def test_none_is_not_anomaly(self) -> None:
        assert flag_anomaly(None) is False

    def test_below_threshold(self) -> None:
        assert flag_anomaly(2.9, threshold=3.0) is False

    def test_at_or_above_threshold_either_direction(self) -> None:
        assert flag_anomaly(3.0, threshold=3.0) is True
        assert flag_anomaly(-4.1, threshold=3.0) is True


class TestEnrichBlob:
    def test_empty_window_passes_through_with_nulls(self) -> None:
        result = enrich_blob(_blob(cpm_avg=None), [10.0, 12.0])
        assert result["rolling_cpm_avg"] is None
        assert result["cpm_zscore"] is None
        assert result["anomaly"] is False
        # original fields preserved
        assert result["geohash"] == "xn774"

    def test_rolling_average_includes_current_window(self) -> None:
        result = enrich_blob(_blob(cpm_avg=30.0), [10.0, 20.0])
        assert result["rolling_cpm_avg"] == 20.0  # mean of [10, 20, 30]

    def test_zscore_scored_against_prior_only(self) -> None:
        # baseline [8, 12] mean 10 std 2 → (16-10)/2 = 3.0, flagged at default threshold
        result = enrich_blob(_blob(cpm_avg=16.0), [8.0, 12.0])
        assert result["cpm_zscore"] == 3.0
        assert result["anomaly"] is True

    def test_no_anomaly_within_baseline(self) -> None:
        result = enrich_blob(_blob(cpm_avg=11.0), [8.0, 12.0])
        assert result["anomaly"] is False

    def test_does_not_mutate_input(self) -> None:
        blob = _blob(cpm_avg=30.0)
        enrich_blob(blob, [10.0, 20.0])
        assert "rolling_cpm_avg" not in blob
