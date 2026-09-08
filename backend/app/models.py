"""Shared Pydantic models for events flowing through the backend.

`RadiationEvent` mirrors `schemas/radiation_event.json` (guidelines.md §7.1) —
the locked contract for `radiation.raw` and `radiation.clean` topics. Field
names match the JSON schema exactly so Kafka payloads parse without aliasing.
"""

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel


class RadiationEvent(BaseModel):
    sensor_id: str
    captured_at: datetime
    uploaded_at: datetime
    latitude: float
    longitude: float
    cpm: Optional[float] = None
    classification: Optional[Literal["SAFE", "WARN", "DANGER"]] = None
    unit: Optional[str] = None
    location_name: Optional[str] = None
    height: Optional[float] = None
    surface: Optional[str] = None
    md5sum: Optional[str] = None
    loader_id: Optional[str] = None
