from __future__ import annotations

import json
from datetime import datetime


def serialize_aggregated(aggregated: dict) -> str:
    """Serialise a geo-bucket aggregate dict into a JSON string for radiation.aggregated.

    Produced by the GeoBucket window operator; field names mirror
    schemas/radiation_aggregated.json so the Backend (M4) parses the output as-is.
    Window bounds are already ISO strings, but the datetime fallback keeps this safe
    if a payload carrying real datetimes is serialised directly.
    """
    return json.dumps(aggregated, default=_iso_default)


def _iso_default(obj: object) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
