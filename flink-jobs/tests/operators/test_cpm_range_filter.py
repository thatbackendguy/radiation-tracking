from __future__ import annotations

import pytest

from operators.cpm_range_filter import CPMRangeFilter


@pytest.fixture
def f() -> CPMRangeFilter:
    return CPMRangeFilter()


class TestCPMRangeFilter:
    def test_negative_cpm_discarded(self, f: CPMRangeFilter) -> None:
        assert f.filter({"sensor_id": "1", "cpm": -1.0}) is False

    def test_over_max_discarded(self, f: CPMRangeFilter) -> None:
        assert f.filter({"sensor_id": "1", "cpm": 1_000_001.0}) is False

    def test_zero_is_valid_boundary(self, f: CPMRangeFilter) -> None:
        assert f.filter({"sensor_id": "1", "cpm": 0.0}) is True

    def test_max_boundary_is_valid(self, f: CPMRangeFilter) -> None:
        assert f.filter({"sensor_id": "1", "cpm": 1_000_000.0}) is True

    def test_typical_cpm_passes(self, f: CPMRangeFilter) -> None:
        assert f.filter({"sensor_id": "1", "cpm": 55.3}) is True

    def test_non_numeric_cpm_discarded(self, f: CPMRangeFilter) -> None:
        assert f.filter({"sensor_id": "1", "cpm": "bad"}) is False

    def test_null_cpm_passes_through(self, f: CPMRangeFilter) -> None:
        # Null is NullCPMFilter's concern; CPMRangeFilter lets it through
        assert f.filter({"sensor_id": "1", "cpm": None}) is True

    def test_integer_cpm_passes(self, f: CPMRangeFilter) -> None:
        assert f.filter({"sensor_id": "1", "cpm": 100}) is True
