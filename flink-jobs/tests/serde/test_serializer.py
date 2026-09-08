"""Tests for the radiation.clean JSON serialiser. The serialiser is the inverse of
the deserialiser used on radiation.raw, so the central guarantee is a lossless
round-trip: deserialise(serialise(event)) == event for every payload shape the
pipeline emits. These run without a Flink runtime.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from model.radiation_event import RadiationEvent
from serde.radiation_event_deserializer import deserialize
from serde.radiation_event_serializer import serialize


class TestSerializeRoundTrip:
    def test_full_clean_event_round_trips(self) -> None:
        event = {
            "sensor_id": "65128",
            "captured_at": "2026-06-19T01:59:57",
            "uploaded_at": "2026-06-19T01:59:58.057630",
            "latitude": 49.608234,
            "longitude": 20.81973,
            "cpm": 36.0,
            "unit": "cpm",
            "classification": None,
            "location_name": None,
            "height": 49.0,
            "surface": None,
            "md5sum": "2d87f72dd7bc580248fa79e23dc6718a",
            "loader_id": None,
        }
        assert deserialize(serialize(event)) == event

    def test_null_cpm_and_missing_unit_preserved(self) -> None:
        # The shape mapper.py emits when the unit is invalid: cpm null, no unit key.
        event = {
            "sensor_id": "209",
            "captured_at": "2026-06-19T01:58:52",
            "uploaded_at": "2026-06-19T01:59:54.454769",
            "latitude": 51.0634,
            "longitude": 11.7586,
            "cpm": None,
            "classification": None,
            "md5sum": "a756f4567c2ce47e5bdb3b90151d9f45",
        }
        result = deserialize(serialize(event))
        assert result["cpm"] is None
        assert "unit" not in result

    def test_classification_passthrough_unchanged(self) -> None:
        # M2 leaves classification null; this asserts a value set by M3's classifier
        # later in the pipeline survives serialisation untouched.
        event = {
            "sensor_id": "1",
            "captured_at": "2011-03-11T14:46:23",
            "uploaded_at": "2011-03-12T00:00:00",
            "latitude": 37.0,
            "longitude": 140.0,
            "cpm": 850.0,
            "classification": "WARN",
        }
        assert deserialize(serialize(event))["classification"] == "WARN"

    def test_unicode_location_not_escaped_to_ascii(self) -> None:
        event = {
            "sensor_id": "8",
            "captured_at": "2011-03-11T14:46:23",
            "uploaded_at": "2011-03-12T00:00:00",
            "latitude": 37.0,
            "longitude": 140.0,
            "location_name": "福島",
        }
        assert deserialize(serialize(event))["location_name"] == "福島"

    def test_output_is_a_string(self) -> None:
        event = {
            "sensor_id": "5",
            "captured_at": "2011-03-11T14:46:23",
            "uploaded_at": "2011-03-12T00:00:00",
            "latitude": -33.87,
            "longitude": -70.66,
        }
        assert isinstance(serialize(event), str)


class TestSerializeFromRadiationEvent:
    def test_to_dict_output_serialises_with_datetime_fallback(self) -> None:
        # RadiationEvent.to_dict() already isoformats timestamps, but if a dict still
        # carries datetimes the _iso_default fallback must handle them, not raise.
        event = RadiationEvent.from_dict(
            deserialize(
                json.dumps(
                    {
                        "sensor_id": "65128",
                        "captured_at": "2026-06-19 01:59:57",
                        "uploaded_at": "2026-06-19 01:59:58.05763",
                        "latitude": 49.608234,
                        "longitude": 20.81973,
                        "cpm": 36.0,
                        "unit": "cpm",
                    }
                )
            )
        )
        payload = event.to_dict()
        payload["captured_at"] = datetime(2026, 6, 19, 1, 59, 57)  # force a raw datetime
        result = deserialize(serialize(payload))
        assert result["captured_at"] == "2026-06-19T01:59:57"

    def test_non_serialisable_object_raises_type_error(self) -> None:
        with pytest.raises(TypeError):
            serialize({"sensor_id": object()})
