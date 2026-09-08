"""Edge-case tests for the JSON deserialiser, written as M2's secondary
contribution to cover the interface contract between M1's producer and
the Flink pipeline.  Each test exercises a shape of payload that M1's CSV
parser might emit.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from model.radiation_event import RadiationEvent
from serde.radiation_event_deserializer import deserialize


class TestDeserializeHappyPath:
    def test_full_event_round_trips(self) -> None:
        event = {
            "sensor_id": "12345",
            "captured_at": "2011-03-15T08:30:00+00:00",
            "uploaded_at": "2011-03-16T12:00:00+00:00",
            "latitude": 37.4219,
            "longitude": 141.0328,
            "cpm": 142.5,
            "unit": "cpm",
            "classification": None,
        }
        result = deserialize(json.dumps(event))
        assert result == event

    def test_minimal_required_fields_only(self) -> None:
        payload = json.dumps(
            {
                "sensor_id": "7",
                "captured_at": "2011-03-11T14:46:23+00:00",
                "uploaded_at": "2011-03-12T00:00:00+00:00",
                "latitude": 38.1,
                "longitude": 142.8,
            }
        )
        result = deserialize(payload)
        assert result["sensor_id"] == "7"
        assert result["latitude"] == 38.1

    def test_null_cpm_is_preserved(self) -> None:
        payload = json.dumps(
            {
                "sensor_id": "99",
                "captured_at": "2011-03-11T14:46:23+00:00",
                "uploaded_at": "2011-03-12T00:00:00+00:00",
                "latitude": 35.0,
                "longitude": 139.0,
                "cpm": None,
            }
        )
        result = deserialize(payload)
        assert result["cpm"] is None

    def test_extra_optional_fields_pass_through(self) -> None:
        payload = json.dumps(
            {
                "sensor_id": "1",
                "captured_at": "2011-03-11T14:46:23+00:00",
                "uploaded_at": "2011-03-12T00:00:00+00:00",
                "latitude": 35.0,
                "longitude": 139.0,
                "cpm": 50.0,
                "unit": "cpm",
                "location_name": "Fukushima",
                "height": 1.2,
                "surface": "asphalt",
                "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
                "loader_id": "loader-001",
            }
        )
        result = deserialize(payload)
        assert result["location_name"] == "Fukushima"
        assert result["md5sum"] == "d41d8cd98f00b204e9800998ecf8427e"

    def test_integer_sensor_id_as_string(self) -> None:
        """M1 emits sensor_id as a string even when the CSV column is numeric."""
        payload = json.dumps(
            {
                "sensor_id": "000042",
                "captured_at": "2011-03-11T14:46:23+00:00",
                "uploaded_at": "2011-03-12T00:00:00+00:00",
                "latitude": 35.0,
                "longitude": 139.0,
            }
        )
        result = deserialize(payload)
        assert isinstance(result["sensor_id"], str)
        assert result["sensor_id"] == "000042"


class TestDeserializeEdgeCases:
    def test_negative_longitude_preserved(self) -> None:
        payload = json.dumps(
            {
                "sensor_id": "5",
                "captured_at": "2011-03-11T14:46:23+00:00",
                "uploaded_at": "2011-03-12T00:00:00+00:00",
                "latitude": -33.87,
                "longitude": -70.66,
                "cpm": 15.0,
            }
        )
        result = deserialize(payload)
        assert result["longitude"] == -70.66

    def test_zero_coordinates_preserved(self) -> None:
        payload = json.dumps(
            {
                "sensor_id": "6",
                "captured_at": "2011-03-11T14:46:23+00:00",
                "uploaded_at": "2011-03-12T00:00:00+00:00",
                "latitude": 0.0,
                "longitude": 0.0,
            }
        )
        result = deserialize(payload)
        assert result["latitude"] == 0.0
        assert result["longitude"] == 0.0

    def test_unicode_location_name(self) -> None:
        payload = json.dumps(
            {
                "sensor_id": "8",
                "captured_at": "2011-03-11T14:46:23+00:00",
                "uploaded_at": "2011-03-12T00:00:00+00:00",
                "latitude": 37.0,
                "longitude": 140.0,
                "location_name": "福島",
            }
        )
        result = deserialize(payload)
        assert result["location_name"] == "福島"

    def test_malformed_json_raises(self) -> None:
        with pytest.raises(Exception):
            deserialize("{bad json")

    def test_empty_string_raises(self) -> None:
        with pytest.raises(Exception):
            deserialize("")


class TestDeserializeProducerContract:
    """Pin the deserialiser to the *actual* output of data-provider/mapper.py.

    Unlike the generic ISO payloads above, the Safecast producer emits naive,
    space-separated timestamps (the CSV uses a space, not 'T', and carries no
    timezone), drops the ``unit`` key entirely when the unit is missing/invalid,
    and nulls ``cpm`` in that case. These are the shapes the Flink pipeline must
    actually accept, taken from rows of data/sample.csv after row_to_event().
    """

    # A representative valid-CPM row from data/sample.csv (Device ID 65128).
    PRODUCER_EVENT = {
        "sensor_id": "65128",
        "captured_at": "2026-06-19 01:59:57",
        "uploaded_at": "2026-06-19 01:59:58.05763",
        "latitude": 49.608234,
        "longitude": 20.81973,
        "cpm": 36.0,
        "classification": None,
        "location_name": None,
        "height": 49.0,
        "surface": None,
        "loader_id": None,
        "md5sum": "2d87f72dd7bc580248fa79e23dc6718a",
        "unit": "cpm",
    }

    def test_space_separated_naive_timestamps_round_trip(self) -> None:
        result = deserialize(json.dumps(self.PRODUCER_EVENT))
        assert result["captured_at"] == "2026-06-19 01:59:57"
        assert "T" not in result["uploaded_at"]

    def test_from_dict_parses_producer_payload(self) -> None:
        event = RadiationEvent.from_dict(deserialize(json.dumps(self.PRODUCER_EVENT)))
        assert event.sensor_id == "65128"
        assert event.captured_at == datetime(2026, 6, 19, 1, 59, 57)
        # Producer emits naive timestamps; Week-4 watermarks must assume UTC.
        assert event.captured_at.tzinfo is None
        assert event.cpm == 36.0
        assert event.unit == "cpm"

    def test_variable_precision_fraction_normalised(self) -> None:
        # ".05763" (5 digits) must parse on Python 3.10 (Flink image), which only
        # accepts 3- or 6-digit fractions; it is padded to 6 → 57630 microseconds.
        event = RadiationEvent.from_dict(deserialize(json.dumps(self.PRODUCER_EVENT)))
        assert event.uploaded_at == datetime(2026, 6, 19, 1, 59, 58, 57630)

    def test_unit_key_omitted_when_cpm_null(self) -> None:
        # mapper.py drops the "unit" key and nulls cpm when the unit is invalid/missing.
        payload = {
            "sensor_id": "209",
            "captured_at": "2026-06-19 01:58:52",
            "uploaded_at": "2026-06-19 01:59:54.454769",
            "latitude": 51.0634,
            "longitude": 11.7586,
            "cpm": None,
            "classification": None,
            "location_name": "Sieglitz, DE",
            "height": None,
            "surface": None,
            "loader_id": None,
            "md5sum": "a756f4567c2ce47e5bdb3b90151d9f45",
        }
        result = deserialize(json.dumps(payload))
        assert "unit" not in result
        event = RadiationEvent.from_dict(result)
        assert event.unit is None
        assert event.cpm is None
