"""
Tests for the backfill span-selector: --start / --end filtering in csv_reader.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from csv_reader import _in_span, _parse_ts, stream_rows  # noqa: E402
from presets import PRESETS  # noqa: E402


# ---------------------------------------------------------------------------
# Unit tests for _parse_ts
# ---------------------------------------------------------------------------


class TestParseTsNormalisation:
    def test_space_separator_converted(self):
        assert _parse_ts("2011-03-11 05:46:23") == "2011-03-11T05:46:23"

    def test_t_separator_unchanged(self):
        assert _parse_ts("2011-03-11T05:46:23") == "2011-03-11T05:46:23"

    def test_z_suffix_replaced(self):
        assert _parse_ts("2011-03-11T05:46:23Z") == "2011-03-11T05:46:23+00:00"

    def test_strips_whitespace(self):
        assert _parse_ts("  2011-03-11 05:46:23  ") == "2011-03-11T05:46:23"


# ---------------------------------------------------------------------------
# Unit tests for _in_span
# ---------------------------------------------------------------------------


class TestInSpan:
    def _row(self, captured_at: str) -> dict[str, str]:
        return {"captured_at": captured_at}

    def test_no_bounds_always_true(self):
        assert _in_span(self._row("2011-03-11T00:00:00"), None, None)

    def test_row_before_start_excluded(self):
        assert not _in_span(self._row("2011-03-10T23:59:59"), "2011-03-11T00:00:00", None)

    def test_row_at_start_included(self):
        assert _in_span(self._row("2011-03-11T00:00:00"), "2011-03-11T00:00:00", None)

    def test_row_after_end_excluded(self):
        assert not _in_span(self._row("2011-04-12T00:00:00"), None, "2011-04-11T23:59:59")

    def test_row_at_end_included(self):
        assert _in_span(self._row("2011-04-11T23:59:59"), None, "2011-04-11T23:59:59")

    def test_row_inside_window_included(self):
        assert _in_span(
            self._row("2011-03-15T12:00:00"), "2011-03-11T00:00:00", "2011-04-11T23:59:59"
        )

    def test_missing_captured_at_passes_through(self):
        assert _in_span({"captured_at": ""}, "2011-03-11T00:00:00", "2011-04-11T23:59:59")

    def test_space_format_compared_correctly(self):
        # Raw CSV value uses space separator — should still be filtered correctly.
        assert _in_span(
            self._row("2011-03-15 12:00:00"), "2011-03-11T00:00:00", "2011-04-11T23:59:59"
        )
        assert not _in_span(self._row("2011-03-10 12:00:00"), "2011-03-11T00:00:00", None)


# ---------------------------------------------------------------------------
# Integration-style test via stream_rows with a temp CSV
# ---------------------------------------------------------------------------

_SAFECAST_HEADER = (
    "Captured Time,Device ID,Value,Unit,Location Name,Height,"
    "Surface,Radiation,Uploaded Time,Loader ID,md5sum,Latitude,Longitude"
)


@pytest.fixture()
def sample_csv(tmp_path: Path) -> Path:
    rows = [
        # captured_time, device_id, value, unit, loc, height, surface, radiation, uploaded_time, loader_id, md5sum, lat, lon
        (
            "2011-03-10 00:00:00",
            "1",
            "25",
            "cpm",
            "",
            "",
            "",
            "",
            "2011-03-10 01:00:00",
            "",
            "aaa",
            "35.0",
            "139.0",
        ),
        (
            "2011-03-11 06:00:00",
            "1",
            "80",
            "cpm",
            "",
            "",
            "",
            "",
            "2011-03-11 07:00:00",
            "",
            "bbb",
            "35.0",
            "139.0",
        ),
        (
            "2011-03-20 12:00:00",
            "1",
            "120",
            "cpm",
            "",
            "",
            "",
            "",
            "2011-03-20 13:00:00",
            "",
            "ccc",
            "35.0",
            "139.0",
        ),
        (
            "2011-04-12 00:00:00",
            "1",
            "30",
            "cpm",
            "",
            "",
            "",
            "",
            "2011-04-12 01:00:00",
            "",
            "ddd",
            "35.0",
            "139.0",
        ),
    ]
    p = tmp_path / "test.csv"
    buf = io.StringIO()
    buf.write(_SAFECAST_HEADER + "\n")
    for r in rows:
        buf.write(",".join(str(v) for v in r) + "\n")
    p.write_text(buf.getvalue(), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Preset registry tests
# ---------------------------------------------------------------------------


class TestPresets:
    def test_fukushima_preset_exists(self):
        assert "fukushima" in PRESETS

    def test_fukushima_start_is_disaster_date(self):
        start, _ = PRESETS["fukushima"]
        assert start == "2011-03-11T00:00:00"

    def test_fukushima_end_after_start(self):
        start, end = PRESETS["fukushima"]
        assert end > start

    def test_preset_bounds_are_iso8601(self):
        for name, (start, end) in PRESETS.items():
            assert "T" in start, f"{name} start not ISO8601"
            assert "T" in end, f"{name} end not ISO8601"


class TestStreamRowsSpan:
    def test_no_filter_yields_all(self, sample_csv):
        rows = list(stream_rows(sample_csv))
        assert len(rows) == 4

    def test_start_only_excludes_before(self, sample_csv):
        rows = list(stream_rows(sample_csv, start_ts="2011-03-11T00:00:00"))
        captured = [_parse_ts(r["captured_at"]) for r in rows]
        assert all(ts >= "2011-03-11T00:00:00" for ts in captured)
        assert len(rows) == 3

    def test_end_only_excludes_after(self, sample_csv):
        rows = list(stream_rows(sample_csv, end_ts="2011-04-11T23:59:59"))
        assert len(rows) == 3

    def test_start_and_end_window(self, sample_csv):
        rows = list(
            stream_rows(sample_csv, start_ts="2011-03-11T00:00:00", end_ts="2011-04-11T23:59:59")
        )
        assert len(rows) == 2

    def test_empty_window_yields_nothing(self, sample_csv):
        rows = list(
            stream_rows(sample_csv, start_ts="2025-01-01T00:00:00", end_ts="2025-12-31T23:59:59")
        )
        assert rows == []

    def test_space_separated_bound_matches_same_as_t_bound(self, sample_csv):
        # Bounds passed with a space separator must filter identically to T-form.
        rows_space = list(stream_rows(sample_csv, start_ts="2011-03-11 00:00:00"))
        rows_t = list(stream_rows(sample_csv, start_ts="2011-03-11T00:00:00"))
        assert len(rows_space) == len(rows_t)

    def test_z_suffix_bound_on_exact_boundary(self, sample_csv):
        # A bound with a Z suffix must still match rows sitting exactly on it.
        rows = list(stream_rows(sample_csv, start_ts="2011-03-11T06:00:00Z"))
        captured = [_parse_ts(r["captured_at"]) for r in rows]
        assert all(ts >= "2011-03-11T06:00:00+00:00" for ts in captured)
