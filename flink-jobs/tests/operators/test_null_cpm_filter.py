from __future__ import annotations

import pytest

from operators.null_cpm_filter import NullCPMFilter


@pytest.fixture
def f() -> NullCPMFilter:
    return NullCPMFilter()


class TestNullCPMFilter:
    def test_null_cpm_is_discarded(self, f: NullCPMFilter) -> None:
        assert f.filter({"sensor_id": "1", "cpm": None}) is False

    def test_missing_cpm_key_is_discarded(self, f: NullCPMFilter) -> None:
        assert f.filter({"sensor_id": "1"}) is False

    def test_zero_cpm_passes(self, f: NullCPMFilter) -> None:
        assert f.filter({"sensor_id": "1", "cpm": 0.0}) is True

    def test_normal_cpm_passes(self, f: NullCPMFilter) -> None:
        assert f.filter({"sensor_id": "1", "cpm": 142.5}) is True

    def test_high_cpm_passes(self, f: NullCPMFilter) -> None:
        # NullCPMFilter only checks for null — range is CPMRangeFilter's job
        assert f.filter({"sensor_id": "1", "cpm": 9_999_999}) is True

    def test_empty_event_is_discarded(self, f: NullCPMFilter) -> None:
        assert f.filter({}) is False
