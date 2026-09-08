"""
geo_bucket.py — geohash bucketing for the geo aggregation stage.

Each cleaned event carries a GPS ``latitude``/``longitude``; the aggregation keys
events into geographic cells by encoding that point as a geohash. Precision 5
(~4.9 km cells) is the default — coarse enough to group neighbouring readings into
a meaningful blob, fine enough to localise a hotspot — and is overridable via
``FLINK_GEOHASH_PRECISION`` for tuning. See docs/decisions/ADR-004.

The pure helpers (``geo_bucket`` / ``with_geo_bucket``) wrap ``pygeohash`` and are
unit-tested without PyFlink. ``GeoBucketEnricher`` is the thin Flink MapFunction
wrapper; PyFlink is imported lazily so the module imports in plain pytest.
"""

from __future__ import annotations

import os

import pygeohash

try:
    from pyflink.datastream import MapFunction
except ImportError:  # running tests outside the Flink Docker image

    class MapFunction:  # type: ignore[no-redef]
        def map(self, value):
            raise NotImplementedError


# Default geohash precision (characters). 5 ≈ 4.9 km cells; override for finer/coarser
# buckets. Read once at import — the job process sets the env before the graph is built.
_DEFAULT_PRECISION = int(os.environ.get("FLINK_GEOHASH_PRECISION", "5"))


def geo_bucket(latitude: float, longitude: float, precision: int = _DEFAULT_PRECISION) -> str:
    """Encode a GPS point into its geohash cell of the given precision.

    The geohash length equals ``precision``; nearby points share a common prefix,
    so keying by the full hash groups readings within the same ~cell together.
    """
    return pygeohash.encode(float(latitude), float(longitude), precision=precision)


def with_geo_bucket(event: dict, precision: int = _DEFAULT_PRECISION) -> dict:
    """Return a copy of ``event`` with a ``geohash`` field added from its lat/lon.

    Returns a shallow copy rather than mutating the caller's dict so the enrichment
    is side-effect free and easy to reason about in tests. ``geohash`` is an internal
    aggregation key — it is added on the aggregation branch only, not on the
    radiation.clean contract.
    """
    enriched = dict(event)
    enriched["geohash"] = geo_bucket(event["latitude"], event["longitude"], precision)
    return enriched


class GeoBucketEnricher(MapFunction):
    """Tags each cleaned event with its ``geohash`` cell ahead of the keyed window."""

    def __init__(self, precision: int = _DEFAULT_PRECISION) -> None:
        self._precision = precision

    def map(self, event: dict) -> dict:
        return with_geo_bucket(event, self._precision)
