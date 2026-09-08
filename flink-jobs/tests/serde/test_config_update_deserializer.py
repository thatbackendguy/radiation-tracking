"""Unit tests for serde/config_update_deserializer.py."""

from __future__ import annotations

import json
from datetime import datetime

from serde.config_update_deserializer import deserialize_config


class TestDeserializeConfig:
    def test_thresholds_only(self) -> None:
        config = deserialize_config(
            json.dumps({"cpm_warn_threshold": 120, "cpm_danger_threshold": 400})
        )
        assert config.cpm_warn_threshold == 120.0
        assert config.cpm_danger_threshold == 400.0
        assert config.area is None
        assert config.timespan is None

    def test_with_area_and_timespan(self) -> None:
        config = deserialize_config(
            json.dumps(
                {
                    "cpm_warn_threshold": 100,
                    "cpm_danger_threshold": 300,
                    "area": {
                        "min_lat": 35.0,
                        "max_lat": 38.0,
                        "min_lon": 139.0,
                        "max_lon": 141.0,
                    },
                    "timespan": {
                        "start": "2026-06-01T00:00:00",
                        "end": "2026-06-02T00:00:00",
                    },
                }
            )
        )
        assert config.area is not None
        assert config.area.min_lat == 35.0
        assert config.timespan is not None
        assert config.timespan.start == datetime(2026, 6, 1, 0, 0, 0)
