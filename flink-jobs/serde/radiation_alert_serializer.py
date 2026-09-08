from __future__ import annotations

import json
from datetime import datetime


def serialize_alert(alert: dict) -> str:
    """Serialise a sustained-high alert dict into a JSON string for radiation.alerts.

    Produced by the alert window operator; field names mirror
    schemas/radiation_alert.json so the Backend (M4) parses the output as-is.
    Window bounds and ``triggered_at`` are already ISO strings, but the datetime
    fallback keeps this safe if a payload carrying real datetimes is serialised
    directly.
    """
    return json.dumps(alert, default=_iso_default)


def _iso_default(obj: object) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
