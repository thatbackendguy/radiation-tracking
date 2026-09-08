from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Area:
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float


@dataclass
class Timespan:
    start: datetime
    end: datetime


@dataclass
class ConfigUpdate:
    """Mirrors schemas/config_update.json.

    Published to the config.updates Kafka topic by the backend (M4).
    Consumed by ThresholdClassifierOperator via Flink broadcast state.
    """

    cpm_warn_threshold: float
    cpm_danger_threshold: float
    area: Optional[Area] = None
    timespan: Optional[Timespan] = None

    @classmethod
    def from_dict(cls, d: dict) -> ConfigUpdate:
        area_data = d.get("area")
        timespan_data = d.get("timespan")
        return cls(
            cpm_warn_threshold=float(d["cpm_warn_threshold"]),
            cpm_danger_threshold=float(d["cpm_danger_threshold"]),
            area=Area(**area_data) if area_data else None,
            timespan=(
                Timespan(
                    start=datetime.fromisoformat(timespan_data["start"]),
                    end=datetime.fromisoformat(timespan_data["end"]),
                )
                if timespan_data
                else None
            ),
        )
