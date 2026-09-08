"""Unit coverage for the captured_at watermark assigners (operators/watermark.py).

PyFlink is absent in CI, so ``watermark.py`` falls back to a ``TimestampAssigner``
stub base (mirroring the ``geo_window`` / ``geo_bucket`` stub pattern). The work
each assigner does per event — ``extract_timestamp`` — is pure ``captured_at``
maths from ``timestamps.py``, so it runs here without a Flink runtime. This is the
only CI-runnable coverage of the geo aggregation's watermark re-assignment
(``geo_window.build_geo_aggregation`` re-derives event time from ``captured_at``
via ``DictCapturedAtTimestampAssigner``); the ``assign_timestamps_and_watermarks``
wiring itself needs a MiniCluster and is exercised manually.
"""

from __future__ import annotations

import json

from operators.watermark import (
    CapturedAtTimestampAssigner,
    DictCapturedAtTimestampAssigner,
)

# extract_timestamp ignores record_timestamp; -1 stands in for "no upstream timestamp".
_NO_RECORD_TS = -1


class TestDictCapturedAtTimestampAssigner:
    """The assigner that re-derives event time on the cleaned (dict) stream."""

    def test_extracts_captured_at_not_uploaded_at(self) -> None:
        assigner = DictCapturedAtTimestampAssigner()
        event = {
            "captured_at": "1970-01-01T00:00:02+00:00",
            "uploaded_at": "2030-01-01T00:00:00+00:00",
        }
        assert assigner.extract_timestamp(event, _NO_RECORD_TS) == 2000

    def test_producer_space_separated_variable_precision(self) -> None:
        assigner = DictCapturedAtTimestampAssigner()
        event = {"captured_at": "2026-06-19 01:59:58.05763"}
        assert assigner.extract_timestamp(event, _NO_RECORD_TS) > 0

    def test_poison_pill_returns_zero_not_raises(self) -> None:
        # Tolerance contract: a malformed record must yield 0 (which cannot advance a
        # bounded-out-of-orderness watermark) rather than raise and fail the task.
        assigner = DictCapturedAtTimestampAssigner()
        assert assigner.extract_timestamp({}, _NO_RECORD_TS) == 0  # missing key
        assert assigner.extract_timestamp({"captured_at": "not-a-date"}, _NO_RECORD_TS) == 0

    def test_agrees_with_source_string_assigner(self) -> None:
        # The re-assigned dict watermark must reproduce the source's event time exactly,
        # else the geo windows would bucket on a different timeline than the source.
        captured_at = "2026-06-19 01:00:30.5"
        dict_ts = DictCapturedAtTimestampAssigner().extract_timestamp(
            {"captured_at": captured_at}, _NO_RECORD_TS
        )
        string_ts = CapturedAtTimestampAssigner().extract_timestamp(
            json.dumps({"captured_at": captured_at}), _NO_RECORD_TS
        )
        assert dict_ts == string_ts


class TestCapturedAtTimestampAssigner:
    """The source assigner that parses raw JSON strings at the Kafka source."""

    def test_extracts_captured_at_millis(self) -> None:
        assigner = CapturedAtTimestampAssigner()
        event = json.dumps({"captured_at": "1970-01-01T00:00:02+00:00"})
        assert assigner.extract_timestamp(event, _NO_RECORD_TS) == 2000

    def test_poison_pill_returns_zero_not_raises(self) -> None:
        assigner = CapturedAtTimestampAssigner()
        assert assigner.extract_timestamp("not json at all", _NO_RECORD_TS) == 0
        assert assigner.extract_timestamp(json.dumps({}), _NO_RECORD_TS) == 0
