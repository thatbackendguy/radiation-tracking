"""
Tests for data_provider/mapper.py

Run:
    pytest data_provider/tests/test_mapper.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from csv_reader import stream_rows  # noqa: E402
from mapper import row_to_event  # noqa: E402

SAMPLE_CSV = Path(__file__).parent.parent.parent / "data" / "sample.csv"


def _row(**overrides) -> dict[str, str]:
    base = {
        "captured_at": "2020-06-17T08:02:49.000Z",
        "device_id": "100162",
        "sensor_id": "",
        "latitude": "35.74591",
        "longitude": "139.91815",
        "value": "9.0",
        "unit": "cpm",
        "height": "20.0",
        "location_name": "",
        "measurement_import_id": "",
        "surface": "",
        "uploaded_at": "",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_valid_row_returns_dict():
    assert row_to_event(_row()) is not None


def test_sensor_id_falls_back_to_device_id():
    event = row_to_event(_row(sensor_id="", device_id="100162"))
    assert event["sensor_id"] == "100162"


def test_sensor_id_column_takes_priority():
    event = row_to_event(_row(sensor_id="S-42", device_id="100162"))
    assert event["sensor_id"] == "S-42"


def test_uploaded_at_falls_back_to_captured_at():
    event = row_to_event(_row(uploaded_at=""))
    assert event is not None
    assert event["uploaded_at"] == event["captured_at"]


def test_uploaded_at_used_when_present():
    event = row_to_event(_row(uploaded_at="2021-03-01T12:00:00.000Z"))
    assert event["uploaded_at"] == "2021-03-01T12:00:00.000+00:00"


def test_z_suffix_replaced_with_utc_offset():
    event = row_to_event(_row(captured_at="2020-06-17T08:02:49.000Z"))
    assert event["captured_at"].endswith("+00:00")
    assert "Z" not in event["captured_at"]


def test_latitude_and_longitude_are_floats():
    event = row_to_event(_row())
    assert isinstance(event["latitude"], float)
    assert isinstance(event["longitude"], float)


def test_cpm_populated_for_valid_unit():
    event = row_to_event(_row(unit="cpm", value="42.0"))
    assert event["cpm"] == pytest.approx(42.0)


def test_msv_unit_accepted():
    event = row_to_event(_row(unit="mSv/h", value="0.05"))
    assert event["unit"] == "mSv/h"
    assert event["cpm"] == pytest.approx(0.05)


def test_usv_unit_accepted():
    event = row_to_event(_row(unit="uSv/h", value="0.12"))
    assert event["unit"] == "uSv/h"


def test_classification_always_none():
    assert row_to_event(_row())["classification"] is None


def test_md5sum_is_hex_string():
    event = row_to_event(_row())
    assert isinstance(event["md5sum"], str)
    assert len(event["md5sum"]) == 32
    assert all(c in "0123456789abcdef" for c in event["md5sum"])


def test_md5sum_differs_between_rows():
    e1 = row_to_event(_row(value="9.0"))
    e2 = row_to_event(_row(value="99.0"))
    assert e1["md5sum"] != e2["md5sum"]


def test_location_name_returned():
    event = row_to_event(_row(location_name="  Tokyo  "))
    assert event["location_name"] == "Tokyo"


def test_loader_id_from_measurement_import_id():
    event = row_to_event(_row(measurement_import_id="53526"))
    assert event["loader_id"] == "53526"


def test_height_as_float():
    event = row_to_event(_row(height="20.0"))
    assert event["height"] == pytest.approx(20.0)


# ---------------------------------------------------------------------------
# Invalid / unknown unit → cpm = None
# ---------------------------------------------------------------------------


def test_status_unit_gives_null_cpm():
    event = row_to_event(_row(unit="status", value="29.2"))
    assert event is not None
    assert event["cpm"] is None
    assert "unit" not in event


def test_numeric_unit_string_gives_null_cpm():
    event = row_to_event(_row(unit="211", value="19.0"))
    assert event["cpm"] is None


def test_empty_unit_gives_null_cpm():
    event = row_to_event(_row(unit="", value="10.0"))
    assert event["cpm"] is None


def test_cpm_null_when_value_empty():
    event = row_to_event(_row(unit="cpm", value=""))
    assert event["cpm"] is None


# ---------------------------------------------------------------------------
# Discard cases (must return None)
# ---------------------------------------------------------------------------


def test_discard_when_sensor_and_device_id_both_empty():
    assert row_to_event(_row(sensor_id="", device_id="")) is None


def test_discard_when_device_id_is_sentinel_zero():
    assert row_to_event(_row(sensor_id="", device_id="0")) is None


def test_discard_when_captured_at_empty():
    assert row_to_event(_row(captured_at="")) is None


def test_discard_when_latitude_missing():
    assert row_to_event(_row(latitude="")) is None


def test_discard_when_longitude_missing():
    assert row_to_event(_row(longitude="")) is None


def test_discard_when_latitude_out_of_range():
    assert row_to_event(_row(latitude="999.0")) is None


def test_discard_when_longitude_out_of_range():
    assert row_to_event(_row(longitude="-999.0")) is None


def test_discard_when_latitude_not_numeric():
    assert row_to_event(_row(latitude="abc")) is None


# ---------------------------------------------------------------------------
# Integration: real sample CSV (canonical Safecast schema, via csv_reader)
#
# These go through stream_rows so they exercise the actual ingestion path
# (header normalisation + mapping), not synthetic lowercase dicts.
# ---------------------------------------------------------------------------


def test_sample_csv_no_exceptions():
    if not SAMPLE_CSV.exists():
        pytest.skip("sample.csv not present")
    for row in stream_rows(SAMPLE_CSV, batch_size=100):
        result = row_to_event(row)
        assert result is None or isinstance(result, dict)


def test_sample_csv_yields_real_events():
    # Regression guard: the canonical sample must actually map to events.
    # (Previously every row was discarded due to a header-schema mismatch.)
    if not SAMPLE_CSV.exists():
        pytest.skip("sample.csv not present")
    events = [e for e in (row_to_event(r) for r in stream_rows(SAMPLE_CSV, batch_size=500)) if e]
    assert len(events) > 0, "no rows mapped — schema mismatch regression"


def test_sample_csv_events_have_required_fields():
    if not SAMPLE_CSV.exists():
        pytest.skip("sample.csv not present")
    required = {"sensor_id", "captured_at", "uploaded_at", "latitude", "longitude"}
    seen = 0
    for row in stream_rows(SAMPLE_CSV, batch_size=500):
        event = row_to_event(row)
        if event is not None:
            seen += 1
            for field in required:
                assert event[field] is not None, f"{field} is None: {event}"
    assert seen > 0


def test_sample_csv_timestamps_are_iso8601_t_separated():
    if not SAMPLE_CSV.exists():
        pytest.skip("sample.csv not present")
    for row in stream_rows(SAMPLE_CSV, batch_size=500):
        event = row_to_event(row)
        if event is not None:
            assert "T" in event["captured_at"] and " " not in event["captured_at"]
