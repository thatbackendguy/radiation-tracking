"""Unit tests for operators/classification.py (pure CPM → class logic)."""

from __future__ import annotations

from operators.classification import (
    DANGER,
    DEFAULT_DANGER_THRESHOLD,
    DEFAULT_WARN_THRESHOLD,
    SAFE,
    WARN,
    classify,
)


class TestDefaultThresholds:
    def test_null_cpm_is_unclassified(self) -> None:
        assert classify(None) is None

    def test_below_warn_is_safe(self) -> None:
        assert classify(DEFAULT_WARN_THRESHOLD - 1) == SAFE

    def test_zero_is_safe(self) -> None:
        assert classify(0.0) == SAFE

    def test_warn_boundary_is_inclusive(self) -> None:
        assert classify(DEFAULT_WARN_THRESHOLD) == WARN

    def test_between_warn_and_danger_is_warn(self) -> None:
        midpoint = (DEFAULT_WARN_THRESHOLD + DEFAULT_DANGER_THRESHOLD) / 2
        assert classify(midpoint) == WARN

    def test_danger_boundary_is_inclusive(self) -> None:
        assert classify(DEFAULT_DANGER_THRESHOLD) == DANGER

    def test_above_danger_is_danger(self) -> None:
        assert classify(DEFAULT_DANGER_THRESHOLD + 5000) == DANGER

    def test_string_cpm_is_coerced(self) -> None:
        # M1 may emit cpm as a JSON number that arrives as a string after parsing.
        assert classify(str(DEFAULT_DANGER_THRESHOLD + 500)) == DANGER


class TestCustomThresholds:
    def test_custom_thresholds_applied(self) -> None:
        assert classify(60.0, warn_threshold=50.0, danger_threshold=500.0) == WARN
        assert classify(40.0, warn_threshold=50.0, danger_threshold=500.0) == SAFE
        assert classify(500.0, warn_threshold=50.0, danger_threshold=500.0) == DANGER

    def test_reconfigured_thresholds_change_outcome(self) -> None:
        """Same reading, different config → different class (broadcast-config behaviour)."""
        reading = 120.0
        assert classify(reading, warn_threshold=100.0, danger_threshold=300.0) == WARN
        assert classify(reading, warn_threshold=200.0, danger_threshold=300.0) == SAFE
