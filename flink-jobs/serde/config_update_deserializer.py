from __future__ import annotations

import json

from model.config_update import ConfigUpdate


def deserialize_config(raw: str) -> ConfigUpdate:
    """Parse a ``config.updates`` JSON string into a ConfigUpdate.

    Published by the Backend (M4) and consumed by the threshold classifier via
    Flink broadcast state. Mirrors schemas/config_update.json.
    """
    return ConfigUpdate.from_dict(json.loads(raw))
