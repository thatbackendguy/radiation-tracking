"""
alert_region_join.py — enrich sustained-high alerts with their geohash cell stats.

The Week-5 alert operator (operators/alert_window + operators/alert_dedup) emits one
``radiation.alerts`` record per sensor per cooldown, located at the latest breaching
reading. This stage joins each alert against the ``radiation.aggregated`` blob for the
same geohash cell so the alert carries the cell's recent context — average/peak CPM,
reading count, worst classification, centroid — as an additive ``hot_region`` block.
That lets the map label an alert with the *region* around it ("hot region"), not just
the single triggering sensor. The alert schema is extended additively, so the Backend
(M4) and frontend (M5) keep working unchanged. See docs/decisions/ADR-007.

The two inputs are keyed by **geohash**: the aggregated blob already carries one
(operators/geo_bucket), and the alert is hashed from its breach location to the *same*
precision. Alert lat/lon are nullable (schemas/radiation_alert.json), so the hashing is
null-guarded — a null-coordinate alert passes through with ``hot_region: None`` rather
than crashing the task on ``pygeohash.encode(None, …)`` (an unhandled error would fail
the operator and crash-loop on restart).

The pure helpers (``alert_geohash`` / ``region_summary`` / ``enrich_alert_with_region``)
carry no PyFlink import and are unit-tested directly; ``AlertRegionJoinFunction`` is the
thin keyed two-input wrapper (PyFlink imported with a stub fallback, same pattern as
operators/classifier.py).
"""

from __future__ import annotations

import json
import os
from typing import Iterator, Optional

try:
    from pyflink.datastream.functions import KeyedCoProcessFunction
except ImportError:  # running tests outside the Flink Docker image

    class KeyedCoProcessFunction:  # type: ignore[no-redef]
        def open(self, runtime_context) -> None: ...

        def process_element1(self, value, ctx):
            raise NotImplementedError

        def process_element2(self, value, ctx):
            raise NotImplementedError


from operators.geo_bucket import geo_bucket

# How long (seconds) to keep a cell's latest aggregated blob in join state. Generously
# longer than the aggregation window so an alert can still find recent cell stats; bounds
# idle cells so per-geohash state cannot grow unbounded over a long soak (mirrors the
# AlertCooldownOperator / SensorDedupOperator TTL discipline).
_AGG_WINDOW_SECONDS = int(os.environ.get("FLINK_AGG_WINDOW_SECONDS", "60"))
_STATE_TTL_SECONDS = max(_AGG_WINDOW_SECONDS * 10, 3600)


def alert_geohash(alert: dict) -> Optional[str]:
    """Geohash cell of an alert's breach location, or None when coordinates are missing.

    Encoded at the default precision so it matches the aggregation's geohash key. Returns
    None (rather than raising) when ``latitude``/``longitude`` is null — those alerts are
    still emitted, just without a ``hot_region``.
    """
    latitude = alert.get("latitude")
    longitude = alert.get("longitude")
    if latitude is None or longitude is None:
        return None
    return geo_bucket(latitude, longitude)


def region_summary(blob: dict) -> dict:
    """Project an aggregated blob down to the fields surfaced on an alert's hot_region."""
    return {
        "cpm_avg": blob.get("cpm_avg"),
        "cpm_max": blob.get("cpm_max"),
        "count": blob.get("count"),
        "worst_classification": blob.get("worst_classification"),
        "centroid_latitude": blob.get("centroid_latitude"),
        "centroid_longitude": blob.get("centroid_longitude"),
    }


def enrich_alert_with_region(alert: dict, region: Optional[dict]) -> dict:
    """Return a copy of ``alert`` with an additive ``hot_region`` block.

    ``hot_region`` is the cell's ``region_summary`` when stats are known, else None (the
    alert fired before its cell's first aggregation window, or has no coordinates). A
    shallow copy keeps the helper side-effect free for testing, mirroring with_geo_bucket.
    """
    enriched = dict(alert)
    enriched["hot_region"] = region_summary(region) if region else None
    return enriched


class AlertRegionJoinFunction(KeyedCoProcessFunction):
    """Joins alerts (input 1) with the latest aggregated blob per geohash (input 2).

    Both inputs are keyed by geohash upstream. The latest blob for each cell is kept in
    per-key ValueState (JSON), refreshed on every aggregated record and expired by TTL so
    idle cells release state. Each alert is emitted immediately, enriched best-effort with
    whatever cell stats are currently known (None when none have arrived yet).
    """

    def __init__(self, ttl_seconds: int = _STATE_TTL_SECONDS) -> None:
        self._ttl_seconds = ttl_seconds
        self._latest_blob = None

    def open(self, runtime_context) -> None:
        from pyflink.common.time import Time
        from pyflink.common.typeinfo import Types
        from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

        descriptor = ValueStateDescriptor("latest-agg-blob", Types.STRING())
        ttl_config = (
            StateTtlConfig.new_builder(Time.seconds(self._ttl_seconds))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanup_full_snapshot()
            .build()
        )
        descriptor.enable_time_to_live(ttl_config)
        self._latest_blob = runtime_context.get_state(descriptor)

    def process_element1(self, alert: dict, ctx) -> Iterator[dict]:
        raw = self._latest_blob.value()
        region = json.loads(raw) if raw else None
        yield enrich_alert_with_region(alert, region)

    def process_element2(self, blob: dict, ctx) -> Iterator[dict]:
        # Keep only the latest blob per cell; alerts read it on arrival. Emit nothing.
        self._latest_blob.update(json.dumps(blob, default=str))
        return iter(())


def build_alert_region_join(alert_stream, aggregated_stream):
    """Wire the alerts × aggregated geohash join into the job graph.

    Args:
        alert_stream: ``DataStream[dict]`` of deduped sustained-high alerts.
        aggregated_stream: ``DataStream[dict]`` of ``radiation.aggregated`` blobs (each
            carries a ``geohash``).

    Returns:
        ``DataStream[dict]`` of alerts enriched with a ``hot_region`` block, ready for the
        union with the trend stream and the ``radiation.alerts`` KafkaSink.
    """
    from pyflink.common.typeinfo import Types

    # Tag each alert with its geohash (None when it has no coordinates) for both the
    # output field and the join key. The key selector coalesces None → "" so a
    # null-coordinate alert still keys deterministically; "" never matches a real cell,
    # so it is emitted with hot_region None.
    keyed_alerts = alert_stream.map(_with_alert_geohash)

    return (
        keyed_alerts.connect(aggregated_stream)
        .key_by(
            lambda alert: alert.get("geohash") or "",
            lambda blob: blob["geohash"],
            key_type=Types.STRING(),
        )
        .process(AlertRegionJoinFunction())
    )


def _with_alert_geohash(alert: dict) -> dict:
    """Return a copy of ``alert`` with its ``geohash`` field set (None when no lat/lon)."""
    enriched = dict(alert)
    enriched["geohash"] = alert_geohash(alert)
    return enriched
