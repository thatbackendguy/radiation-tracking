"""
classification.py — pure CPM → threshold-class logic.

Maps a counts-per-minute reading to one of SAFE / WARN / DANGER given the
warn and danger thresholds. Kept free of any PyFlink import so it can be
unit-tested directly; the broadcast operator in ``classifier.py`` wraps it.

The defaults are used only until the first ``config.updates`` message arrives
on the broadcast stream; the backend (M4) can override them at runtime. They
match the backend's own defaults (``cpm_warn_threshold`` 100 / ``cpm_danger_threshold``
1000 in backend settings.py and schemas/config_update.json), since the backend is
the source of truth for thresholds — the two must agree before the first config
lands. (This is distinct from the Week-5 ``radiation.alerts`` operator's own
threshold.)
"""

from __future__ import annotations

from typing import Optional

DEFAULT_WARN_THRESHOLD: float = 100.0
DEFAULT_DANGER_THRESHOLD: float = 1000.0

SAFE = "SAFE"
WARN = "WARN"
DANGER = "DANGER"


def classify(
    cpm: Optional[float],
    warn_threshold: float = DEFAULT_WARN_THRESHOLD,
    danger_threshold: float = DEFAULT_DANGER_THRESHOLD,
) -> Optional[str]:
    """Classify a CPM reading.

    Returns:
        ``DANGER`` if ``cpm >= danger_threshold``,
        ``WARN``   if ``cpm >= warn_threshold`` (but below danger),
        ``SAFE``   if below the warn threshold,
        ``None``   if ``cpm`` is None (unknown — null readings are not classified).
    """
    if cpm is None:
        return None
    cpm = float(cpm)
    if cpm >= danger_threshold:
        return DANGER
    if cpm >= warn_threshold:
        return WARN
    return SAFE
