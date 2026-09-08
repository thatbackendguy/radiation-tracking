"""
Unit tests for shared/validator/schema_validator.py.

M3 responsibility (Week 2): validate the discard logic for null/empty
fields and CPM range sanity checks, as required before events enter
the Flink pipeline.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from shared.validator.schema_validator import should_discard, validate

FIXTURES_PATH = Path(__file__).parent.parent / "fixtures" / "radiation_events.json"


@pytest.fixture(scope="module")
def fixtures() -> dict[str, Any]:
    return json.loads(FIXTURES_PATH.read_text())


# ---------------------------------------------------------------------------
# Valid events — must NOT be discarded
# ---------------------------------------------------------------------------


class TestValidEvents:
    def test_full_event_passes(self, fixtures: dict[str, Any]) -> None:
        event = fixtures["valid"][0]
        result = validate(event)
        assert result.is_valid, result.discard_reason

    def test_minimal_event_with_null_cpm_passes(self, fixtures: dict[str, Any]) -> None:
        """CPM is allowed to be null (sensor malfunction)."""
        event = fixtures["valid"][2]
        assert event["cpm"] is None
        result = validate(event)
        assert result.is_valid, result.discard_reason

    def test_high_location_event_passes(self, fixtures: dict[str, Any]) -> None:
        event = fixtures["valid"][1]
        result = validate(event)
        assert result.is_valid, result.discard_reason

    def test_should_discard_returns_false_for_valid(self, fixtures: dict[str, Any]) -> None:
        discard, reason = should_discard(fixtures["valid"][0])
        assert discard is False
        assert reason is None


# ---------------------------------------------------------------------------
# Missing / empty required fields — must be discarded
# ---------------------------------------------------------------------------


class TestMissingRequiredFields:
    def test_missing_sensor_id(self, fixtures: dict[str, Any]) -> None:
        result = validate(fixtures["invalid"]["missing_sensor_id"])
        assert not result.is_valid
        assert "sensor_id" in (result.discard_reason or "")

    def test_empty_sensor_id(self, fixtures: dict[str, Any]) -> None:
        result = validate(fixtures["invalid"]["empty_sensor_id"])
        assert not result.is_valid
        assert "sensor_id" in (result.discard_reason or "")

    def test_missing_captured_at(self, fixtures: dict[str, Any]) -> None:
        result = validate(fixtures["invalid"]["missing_captured_at"])
        assert not result.is_valid
        assert "captured_at" in (result.discard_reason or "")

    def test_missing_latitude(self, fixtures: dict[str, Any]) -> None:
        result = validate(fixtures["invalid"]["missing_latitude"])
        assert not result.is_valid
        assert "latitude" in (result.discard_reason or "")

    def test_empty_dict_discarded(self, fixtures: dict[str, Any]) -> None:
        result = validate(fixtures["invalid"]["empty_dict"])
        assert not result.is_valid

    def test_missing_uploaded_at(self) -> None:
        event = {
            "sensor_id": "42",
            "captured_at": "2026-05-29T01:58:49+00:00",
            "latitude": 50.0,
            "longitude": 10.0,
        }
        result = validate(event)
        assert not result.is_valid
        assert "uploaded_at" in (result.discard_reason or "")

    def test_missing_longitude(self) -> None:
        event = {
            "sensor_id": "42",
            "captured_at": "2026-05-29T01:58:49+00:00",
            "uploaded_at": "2026-05-29T01:59:00+00:00",
            "latitude": 50.0,
        }
        result = validate(event)
        assert not result.is_valid
        assert "longitude" in (result.discard_reason or "")


# ---------------------------------------------------------------------------
# CPM range sanity checks
# ---------------------------------------------------------------------------


class TestCPMRangeSanity:
    def test_negative_cpm_discarded(self, fixtures: dict[str, Any]) -> None:
        result = validate(fixtures["invalid"]["cpm_negative"])
        assert not result.is_valid
        assert "cpm" in (result.discard_reason or "")

    def test_over_max_cpm_discarded(self, fixtures: dict[str, Any]) -> None:
        result = validate(fixtures["invalid"]["cpm_over_max"])
        assert not result.is_valid
        assert "cpm" in (result.discard_reason or "")

    def test_zero_cpm_accepted(self) -> None:
        event = {
            "sensor_id": "99",
            "captured_at": "2026-05-29T01:58:49+00:00",
            "uploaded_at": "2026-05-29T01:59:00+00:00",
            "latitude": 35.0,
            "longitude": 139.0,
            "cpm": 0.0,
            "unit": "cpm",
        }
        result = validate(event)
        assert result.is_valid, result.discard_reason

    def test_max_boundary_cpm_accepted(self) -> None:
        event = {
            "sensor_id": "99",
            "captured_at": "2026-05-29T01:58:49+00:00",
            "uploaded_at": "2026-05-29T01:59:00+00:00",
            "latitude": 35.0,
            "longitude": 139.0,
            "cpm": 1_000_000.0,
            "unit": "cpm",
        }
        result = validate(event)
        assert result.is_valid, result.discard_reason

    def test_non_numeric_cpm_discarded(self) -> None:
        event = {
            "sensor_id": "99",
            "captured_at": "2026-05-29T01:58:49+00:00",
            "uploaded_at": "2026-05-29T01:59:00+00:00",
            "latitude": 35.0,
            "longitude": 139.0,
            "cpm": "not-a-number",
            "unit": "cpm",
        }
        result = validate(event)
        assert not result.is_valid
        assert "cpm" in (result.discard_reason or "")


# ---------------------------------------------------------------------------
# Non-dict / null inputs — must be discarded
# ---------------------------------------------------------------------------


class TestNonDictInputs:
    def test_string_input_discarded(self, fixtures: dict[str, Any]) -> None:
        result = validate(fixtures["invalid"]["not_a_dict"])
        assert not result.is_valid
        assert "not a dict" in (result.discard_reason or "")

    def test_none_input_discarded(self, fixtures: dict[str, Any]) -> None:
        result = validate(fixtures["invalid"]["null_event"])
        assert not result.is_valid

    def test_list_input_discarded(self) -> None:
        result = validate([{"sensor_id": "1"}])  # type: ignore[arg-type]
        assert not result.is_valid

    def test_int_input_discarded(self) -> None:
        result = validate(42)  # type: ignore[arg-type]
        assert not result.is_valid


# ---------------------------------------------------------------------------
# Coordinate range validation (via JSON Schema)
# ---------------------------------------------------------------------------


class TestCoordinateRanges:
    def test_latitude_over_90_discarded(self, fixtures: dict[str, Any]) -> None:
        result = validate(fixtures["invalid"]["latitude_out_of_range"])
        assert not result.is_valid

    def test_longitude_under_minus_180_discarded(self) -> None:
        event = {
            "sensor_id": "99",
            "captured_at": "2026-05-29T01:58:49+00:00",
            "uploaded_at": "2026-05-29T01:59:00+00:00",
            "latitude": 35.0,
            "longitude": -181.0,
            "cpm": 20.0,
            "unit": "cpm",
        }
        result = validate(event)
        assert not result.is_valid

    def test_valid_negative_coordinates_accepted(self) -> None:
        """Southern hemisphere, western longitude — valid."""
        event = {
            "sensor_id": "99",
            "captured_at": "2026-05-29T01:58:49+00:00",
            "uploaded_at": "2026-05-29T01:59:00+00:00",
            "latitude": -33.87,
            "longitude": -70.66,
            "cpm": 15.0,
            "unit": "cpm",
        }
        result = validate(event)
        assert result.is_valid, result.discard_reason


# ---------------------------------------------------------------------------
# should_discard convenience wrapper
# ---------------------------------------------------------------------------


class TestShouldDiscard:
    def test_returns_true_and_reason_for_invalid(self) -> None:
        event = {"sensor_id": "", "captured_at": "x", "uploaded_at": "x", "latitude": 0, "longitude": 0}
        discard, reason = should_discard(event)
        assert discard is True
        assert reason is not None

    def test_returns_false_and_none_for_valid(self) -> None:
        event = {
            "sensor_id": "1",
            "captured_at": "2026-05-29T01:58:49+00:00",
            "uploaded_at": "2026-05-29T01:59:00+00:00",
            "latitude": 0.0,
            "longitude": 0.0,
        }
        discard, reason = should_discard(event)
        assert discard is False
        assert reason is None
