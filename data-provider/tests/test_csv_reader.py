"""
Tests for data_provider/csv_reader.py

Run:
    pytest data_provider/tests/test_csv_reader.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from csv_reader import stream_rows, _pick_order_col  # noqa: E402

SAMPLE_CSV = Path(__file__).parent.parent.parent / "data" / "sample.csv"

# ---------------------------------------------------------------------------
# CSV fixture helpers
# ---------------------------------------------------------------------------

HEADER = "captured_at,device_id,sensor_id,latitude,longitude,value,unit,height,location_name,measurement_import_id"


def _write(tmp_path: Path, body: str, name: str = "test.csv") -> Path:
    p = tmp_path / name
    p.write_text(HEADER + "\n" + body, encoding="utf-8")
    return p


def _make_row(
    captured_at: str = "2020-01-01T00:00:00.000Z",
    device_id: str = "100",
    lat: str = "35.0",
    lon: str = "139.0",
    value: str = "10.0",
    unit: str = "cpm",
) -> str:
    return f"{captured_at},{device_id},,{lat},{lon},{value},{unit},,,"


# ---------------------------------------------------------------------------
# Basic streaming
# ---------------------------------------------------------------------------


def test_yields_correct_row_count(tmp_path):
    p = _write(tmp_path, "\n".join(_make_row(device_id=str(i)) for i in range(5)))
    rows = list(stream_rows(p, batch_size=10))
    assert len(rows) == 5


def test_rows_are_dicts(tmp_path):
    p = _write(tmp_path, _make_row())
    rows = list(stream_rows(p, batch_size=10))
    assert all(isinstance(r, dict) for r in rows)


def test_empty_csv_yields_nothing(tmp_path):
    p = _write(tmp_path, "")
    assert list(stream_rows(p, batch_size=10)) == []


def test_single_row(tmp_path):
    p = _write(tmp_path, _make_row(device_id="solo"))
    rows = list(stream_rows(p, batch_size=10))
    assert len(rows) == 1
    assert rows[0]["device_id"] == "solo"


def test_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        list(stream_rows(tmp_path / "ghost.csv"))


def test_multiple_batches_all_rows_returned(tmp_path):
    body = "\n".join(_make_row(device_id=str(i)) for i in range(7))
    p = _write(tmp_path, body)
    rows = list(stream_rows(p, batch_size=3))
    assert len(rows) == 7


# ---------------------------------------------------------------------------
# Ordering
# ---------------------------------------------------------------------------


def test_sorted_by_captured_at_within_batch(tmp_path):
    body = "\n".join(
        [
            _make_row(captured_at="2020-01-03T00:00:00Z", device_id="C"),
            _make_row(captured_at="2020-01-01T00:00:00Z", device_id="A"),
            _make_row(captured_at="2020-01-02T00:00:00Z", device_id="B"),
        ]
    )
    p = _write(tmp_path, body)
    rows = list(stream_rows(p, batch_size=100))
    assert [r["device_id"] for r in rows] == ["A", "B", "C"]


def test_sorted_by_uploaded_at_when_present(tmp_path):
    content = (
        "captured_at,uploaded_at,device_id,latitude,longitude,value,unit\n"
        "2020-01-01T00:00:00Z,2020-01-03T00:00:00Z,X,35.0,139.0,10.0,cpm\n"
        "2020-01-02T00:00:00Z,2020-01-01T00:00:00Z,Y,35.0,139.0,10.0,cpm\n"
        "2020-01-03T00:00:00Z,2020-01-02T00:00:00Z,Z,35.0,139.0,10.0,cpm\n"
    )
    p = tmp_path / "with_uploaded.csv"
    p.write_text(content)
    rows = list(stream_rows(p, batch_size=100))
    assert [r["device_id"] for r in rows] == ["Y", "Z", "X"]


def test_rows_with_empty_timestamp_sort_last(tmp_path):
    body = "\n".join(
        [
            _make_row(captured_at="2020-01-02T00:00:00Z", device_id="B"),
            _make_row(captured_at="", device_id="EMPTY"),
            _make_row(captured_at="2020-01-01T00:00:00Z", device_id="A"),
        ]
    )
    p = _write(tmp_path, body)
    rows = list(stream_rows(p, batch_size=100))
    assert rows[-1]["device_id"] == "EMPTY"


def test_partial_final_batch_is_flushed(tmp_path):
    # 5 rows with batch_size=3 → batch of 3 + batch of 2
    body = "\n".join(_make_row(device_id=str(i)) for i in range(5))
    p = _write(tmp_path, body)
    rows = list(stream_rows(p, batch_size=3))
    assert len(rows) == 5


# ---------------------------------------------------------------------------
# Column filtering
# ---------------------------------------------------------------------------


def test_unknown_columns_are_dropped(tmp_path):
    content = (
        "captured_at,channel_id,device_id,latitude,longitude,value,unit\n"
        "2020-01-01T00:00:00Z,CH99,111,35.0,139.0,10.0,cpm\n"
    )
    p = tmp_path / "extra.csv"
    p.write_text(content)
    rows = list(stream_rows(p, batch_size=10))
    assert "channel_id" not in rows[0]


def test_known_columns_are_present(tmp_path):
    p = _write(tmp_path, _make_row())
    rows = list(stream_rows(p, batch_size=10))
    for col in ("captured_at", "device_id", "latitude", "longitude"):
        assert col in rows[0]


# ---------------------------------------------------------------------------
# skip_rows (resume support)
# ---------------------------------------------------------------------------


def test_skip_rows_reduces_output(tmp_path):
    body = "\n".join(_make_row(device_id=str(i)) for i in range(5))
    p = _write(tmp_path, body)
    rows = list(stream_rows(p, batch_size=100, skip_rows=3))
    assert len(rows) == 2


def test_skip_rows_zero_returns_all(tmp_path):
    body = "\n".join(_make_row(device_id=str(i)) for i in range(4))
    p = _write(tmp_path, body)
    assert len(list(stream_rows(p, batch_size=100, skip_rows=0))) == 4


def test_skip_rows_beyond_file_yields_nothing(tmp_path):
    p = _write(tmp_path, _make_row())
    assert list(stream_rows(p, batch_size=100, skip_rows=999)) == []


# ---------------------------------------------------------------------------
# Order column selection
# ---------------------------------------------------------------------------


def test_pick_order_col_prefers_uploaded_at():
    assert _pick_order_col(["captured_at", "uploaded_at", "value"]) == "uploaded_at"


def test_pick_order_col_falls_back_to_captured_at():
    assert _pick_order_col(["captured_at", "latitude", "longitude"]) == "captured_at"


# ---------------------------------------------------------------------------
# Encoding resilience
# ---------------------------------------------------------------------------


def test_non_utf8_bytes_do_not_crash(tmp_path):
    p = tmp_path / "bad_enc.csv"
    p.write_bytes(
        HEADER.encode("utf-8") + b"\n" + b"2020-01-01T00:00:00Z,111,,35.0,139.0,10.0,cpm,,,\x84\n"
    )
    rows = list(stream_rows(p, batch_size=10))
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# Integration against real sample CSV
# ---------------------------------------------------------------------------


def test_sample_csv_streams_rows():
    if not SAMPLE_CSV.exists():
        pytest.skip("sample_data.csv not present")
    rows = list(stream_rows(SAMPLE_CSV, batch_size=50))
    assert len(rows) > 0


def test_sample_csv_rows_are_dicts():
    if not SAMPLE_CSV.exists():
        pytest.skip("sample_data.csv not present")
    for row in stream_rows(SAMPLE_CSV, batch_size=50):
        assert isinstance(row, dict)


def test_sample_csv_within_batch_ordering():
    if not SAMPLE_CSV.exists():
        pytest.skip("sample_data.csv not present")
    batch_size = 10
    rows = list(stream_rows(SAMPLE_CSV, batch_size=batch_size))
    # The canonical sample carries an upload time, so the reader orders by
    # uploaded_at; fall back to captured_at if a future sample drops it.
    order_col = "uploaded_at" if rows and rows[0].get("uploaded_at") else "captured_at"
    for start in range(0, len(rows) - 1, batch_size):
        chunk = rows[start : start + batch_size]
        timestamps = [r.get(order_col, "") for r in chunk]
        assert timestamps == sorted(timestamps)


# ---------------------------------------------------------------------------
# Canonical Safecast schema (display column names)
# ---------------------------------------------------------------------------

CANONICAL_HEADER = (
    "Captured Time,Latitude,Longitude,Value,Unit,Location Name,"
    "Device ID,MD5Sum,Height,Surface,Radiation,Uploaded Time,Loader ID"
)


def test_canonical_headers_mapped_to_internal_names(tmp_path):
    content = (
        CANONICAL_HEADER + "\n"
        "2026-06-19 01:59:57,49.6,20.8,36,cpm,,65128,abc123,49,,,2026-06-19 01:59:58,77\n"
    )
    p = tmp_path / "canonical.csv"
    p.write_text(content, encoding="utf-8")
    rows = list(stream_rows(p, batch_size=10))
    assert len(rows) == 1
    row = rows[0]
    # Display names are translated to the internal schema the mapper expects.
    assert row["captured_at"] == "2026-06-19 01:59:57"
    assert row["uploaded_at"] == "2026-06-19 01:59:58"
    assert row["device_id"] == "65128"
    assert row["value"] == "36"
    assert row["md5sum"] == "abc123"
    assert row["measurement_import_id"] == "77"
    # "Radiation" is not a recognised column and is dropped.
    assert "radiation" not in row


def test_canonical_header_orders_by_uploaded_time(tmp_path):
    content = (
        CANONICAL_HEADER + "\n"
        "2026-01-01 00:00:00,35,139,1,cpm,,A,h1,0,,,2026-01-03 00:00:00,1\n"
        "2026-01-02 00:00:00,35,139,1,cpm,,B,h2,0,,,2026-01-01 00:00:00,1\n"
        "2026-01-03 00:00:00,35,139,1,cpm,,C,h3,0,,,2026-01-02 00:00:00,1\n"
    )
    p = tmp_path / "canonical_order.csv"
    p.write_text(content, encoding="utf-8")
    rows = list(stream_rows(p, batch_size=100))
    assert [r["device_id"] for r in rows] == ["B", "C", "A"]
