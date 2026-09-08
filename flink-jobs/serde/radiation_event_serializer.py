from __future__ import annotations

import json
from datetime import datetime


def serialize(event: dict) -> str:
    """Serialise a cleaned radiation event dict into a JSON string for radiation.clean.

    The inverse of serde.radiation_event_deserializer.deserialize. Events flow through
    the pipeline as plain dicts whose timestamps are already ISO strings (json.loads
    output), but the datetime fallback keeps this safe if a RadiationEvent.to_dict()
    payload with real datetimes is serialised directly. Field names mirror
    schemas/radiation_event.json so the Backend Pydantic model parses the output as-is.
    """
    return json.dumps(event, default=_iso_default)


def _iso_default(obj: object) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
