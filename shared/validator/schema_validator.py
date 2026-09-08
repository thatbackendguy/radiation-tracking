"""
Validates raw radiation event dicts against schemas/radiation_event.json.

Used by both the Data Provider (pre-Kafka) and Flink (post-deserialization)
to ensure malformed or incomplete events are discarded before processing.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jsonschema
from jsonschema import ValidationError

logger = logging.getLogger(__name__)

_SCHEMA_PATH = Path(__file__).parent.parent.parent / "schemas" / "radiation_event.json"


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    discard_reason: str | None = None


def _load_schema() -> dict[str, Any]:
    with _SCHEMA_PATH.open() as f:
        return json.load(f)


_SCHEMA: dict[str, Any] = _load_schema()
_VALIDATOR = jsonschema.Draft7Validator(_SCHEMA)

# CPM range accepted as physically plausible; anything outside is sensor noise.
_CPM_MIN: float = 0.0
_CPM_MAX: float = 1_000_000.0


def validate(event: dict[str, Any]) -> ValidationResult:
    """Return ValidationResult indicating whether the event should be forwarded.

    Discard conditions (in priority order):
    1. Event is None or not a dict.
    2. Required fields (sensor_id, captured_at, uploaded_at, lat, lon) are
       absent or empty-string.
    3. cpm is present but outside the plausible range [0, 1_000_000].
    4. JSON Schema structural validation fails.
    """
    if not isinstance(event, dict):
        return ValidationResult(False, "event is not a dict")

    # --- Required field presence check ----------------------------------------
    required_fields = ("sensor_id", "captured_at", "uploaded_at", "latitude", "longitude")
    for field in required_fields:
        value = event.get(field)
        if value is None or value == "":
            return ValidationResult(False, f"missing or empty required field: {field}")

    # --- CPM range sanity check -----------------------------------------------
    cpm = event.get("cpm")
    if cpm is not None:
        try:
            cpm_float = float(cpm)
        except (TypeError, ValueError):
            return ValidationResult(False, f"cpm is not numeric: {cpm!r}")
        if not (_CPM_MIN <= cpm_float <= _CPM_MAX):
            return ValidationResult(
                False, f"cpm {cpm_float} outside plausible range [{_CPM_MIN}, {_CPM_MAX}]"
            )

    # --- JSON Schema structural validation ------------------------------------
    errors = list(_VALIDATOR.iter_errors(event))
    if errors:
        # Report the first error; the rest are usually cascading.
        first = min(errors, key=lambda e: len(list(e.absolute_path)))
        return ValidationResult(False, f"schema violation: {first.message}")

    return ValidationResult(True)


def should_discard(event: dict[str, Any]) -> tuple[bool, str | None]:
    """Convenience wrapper — returns (discard, reason) for producer-side use."""
    result = validate(event)
    if not result.is_valid:
        logger.debug("discarding event: %s", result.discard_reason)
        return True, result.discard_reason
    return False, None
