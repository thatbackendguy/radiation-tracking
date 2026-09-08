from __future__ import annotations

import json


def deserialize(raw: str) -> dict:
    """Parse a JSON string from radiation.raw into a plain Python dict."""
    return json.loads(raw)
