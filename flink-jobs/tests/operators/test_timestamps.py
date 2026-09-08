"""Unit tests for operators/timestamps.py (event-time extraction)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from operators.timestamps import (
    captured_at_millis,
    extract_captured_at_millis,
    to_epoch_millis,
)


class TestToEpochMillis:
    def test_known_epoch(self) -> None:
        dt = datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        assert to_epoch_millis(dt) == 1000

    def test_subsecond_truncated_to_millis(self) -> None:
        dt = datetime(1970, 1, 1, 0, 0, 0, 500_000, tzinfo=timezone.utc)
        assert to_epoch_millis(dt) == 500

    def test_naive_datetime_assumed_utc(self) -> None:
        naive = datetime(1970, 1, 1, 0, 0, 1)
        aware = datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        assert to_epoch_millis(naive) == to_epoch_millis(aware)


class TestExtractCapturedAtMillis:
    def test_uses_captured_at_not_uploaded_at(self) -> None:
        event = json.dumps(
            {
                "captured_at": "1970-01-01T00:00:02+00:00",
                "uploaded_at": "2030-01-01T00:00:00+00:00",
            }
        )
        assert extract_captured_at_millis(event) == 2000

    def test_later_capture_yields_larger_timestamp(self) -> None:
        earlier = json.dumps({"captured_at": "2026-05-29T01:58:49+00:00"})
        later = json.dumps({"captured_at": "2026-05-29T01:59:49+00:00"})
        assert extract_captured_at_millis(later) > extract_captured_at_millis(earlier)

    def test_producer_space_separated_variable_precision(self) -> None:
        # M1 emits naive, space-separated timestamps with variable fractional digits
        # that Python 3.10's bare fromisoformat rejects; _parse_timestamp handles them.
        event = json.dumps({"captured_at": "2026-06-19 01:59:58.05763"})
        assert extract_captured_at_millis(event) > 0


class TestCapturedAtMillisFromEvent:
    """The dict entry point used to re-assign watermarks on the clean (dict) stream."""

    def test_uses_captured_at_not_uploaded_at(self) -> None:
        event = {
            "captured_at": "1970-01-01T00:00:02+00:00",
            "uploaded_at": "2030-01-01T00:00:00+00:00",
        }
        assert captured_at_millis(event) == 2000

    def test_later_capture_yields_larger_timestamp(self) -> None:
        earlier = {"captured_at": "2026-05-29T01:58:49+00:00"}
        later = {"captured_at": "2026-05-29T01:59:49+00:00"}
        assert captured_at_millis(later) > captured_at_millis(earlier)

    def test_producer_space_separated_variable_precision(self) -> None:
        event = {"captured_at": "2026-06-19 01:59:58.05763"}
        assert captured_at_millis(event) > 0

    def test_matches_json_string_entry_point(self) -> None:
        # The dict and JSON-string paths must agree — extract_captured_at_millis
        # now delegates to captured_at_millis, so the same captured_at yields the same millis.
        event = {"captured_at": "2026-06-19 01:00:30.5"}
        assert captured_at_millis(event) == extract_captured_at_millis(json.dumps(event))
