"""
trend_detector.py — rising-CPM trend detection per geohash → secondary alert stream.

The sustained-high alert (ADR-005) fires on *absolute* DANGER readings. This operator
catches a different signal: a geohash cell whose average CPM is **rising across N
consecutive aggregation windows**, even while each reading is still below DANGER. That is
an early-warning trend — a region heating up — surfaced as a secondary alert on the same
``radiation.alerts`` topic with ``reason: "rising-trend"`` (additive; the backend and
frontend already fan out alerts unchanged). See docs/decisions/ADR-008.

It consumes the in-job ``radiation.aggregated`` stream (operators/geo_window) — the same
``DataStream[dict]`` of per-cell blobs the aggregated sink uses, so there is no extra Kafka
round-trip. Keyed by geohash, it keeps the last N ``cpm_avg`` values per cell in ValueState
and emits a trend alert when they are strictly rising by at least ``min_delta`` overall.
After firing it resets the cell's series (cooldown-by-reset) so a single climb yields one
alert per N rising windows, not one per window.

The pure helpers (``is_rising`` / ``build_trend_alert`` / ``push_window``) carry no PyFlink
import and are unit-tested directly; ``TrendDetectorOperator`` is the thin keyed wrapper
(PyFlink imported with a stub fallback, same pattern as operators/alert_dedup.py).
"""

from __future__ import annotations

import json
import os
from typing import Iterator

try:
    from pyflink.datastream.functions import KeyedProcessFunction
except ImportError:  # running tests outside the Flink Docker image

    class KeyedProcessFunction:  # type: ignore[no-redef]
        def open(self, runtime_context) -> None: ...

        def process_element(self, value, ctx):
            raise NotImplementedError


# Number of consecutive aggregation windows that must be strictly rising to alert, and the
# minimum total rise (last − first CPM) across them to filter out trivial drift. Read once
# at import; override via env for tuning. See docs/decisions/ADR-008.
#
# min-windows is floored at 2 (a "rise" needs at least two samples): a stray
# FLINK_TREND_WINDOWS=1 would otherwise silently disable every alert (is_rising short-circuits
# on min_windows < 2). min-delta defaults to a meaningful non-zero CPM rise — with the default
# WARN threshold at 100 CPM, a 5 CPM climb across the windows is a real regional heat-up, while
# strict-monotonic-only (delta 0) would fire on sub-CPM sensor jitter (10.00 → 10.01 → 10.02).
_DEFAULT_MIN_WINDOWS = max(2, int(os.environ.get("FLINK_TREND_WINDOWS", "3")))
_DEFAULT_MIN_DELTA = float(os.environ.get("FLINK_TREND_MIN_DELTA", "5.0"))

REASON_RISING_TREND = "rising-trend"

# How long (seconds) to keep a quiet cell's trend series before it expires, bounding
# per-geohash state over a long soak (mirrors the join / dedup TTL discipline).
_AGG_WINDOW_SECONDS = int(os.environ.get("FLINK_AGG_WINDOW_SECONDS", "60"))
_STATE_TTL_SECONDS = max(_AGG_WINDOW_SECONDS * 10, 3600)


def push_window(series: list[float], cpm_avg: float, max_len: int) -> list[float]:
    """Append a window's ``cpm_avg`` to the rolling series, trimmed to ``max_len`` items."""
    updated = [*series, cpm_avg]
    return updated[-max_len:]


def is_rising(
    cpm_series: list[float],
    min_windows: int = _DEFAULT_MIN_WINDOWS,
    min_delta: float = _DEFAULT_MIN_DELTA,
) -> bool:
    """Whether the last ``min_windows`` values are strictly increasing by ≥ ``min_delta``.

    Needs at least ``min_windows`` samples; each must be strictly greater than the previous
    (a flat or dipping window breaks the trend), and the total rise across the window must
    be at least ``min_delta`` so trivial drift does not alert.
    """
    if min_windows < 2 or len(cpm_series) < min_windows:
        return False
    window = cpm_series[-min_windows:]
    strictly_rising = all(window[i] > window[i - 1] for i in range(1, len(window)))
    return strictly_rising and (window[-1] - window[0]) >= min_delta


def build_trend_alert(geohash: str, cpm_series: list[float], latest_blob: dict) -> dict:
    """Build a rising-trend alert for one cell from its series and latest aggregated blob.

    The cell's geohash stands in for ``sensor_id`` (the alert's identifier — a trend is a
    regional, not a per-sensor, signal); ``cpm`` is the latest window's peak; location is the
    cell centroid. A ``trend`` block carries the supporting series. Field names mirror
    schemas/radiation_alert.json so the backend parses it as-is.

    The map popup renders ``breach_count`` and ``triggered_at`` for every alert kind, so the
    trend alert mirrors the sustained-high shape: ``breach_count`` is the number of rising
    windows behind the trend and ``triggered_at`` is the end of the latest rising window —
    without them the popup shows blank Breaches/Time lines (2026-07-03 main review).
    """
    return {
        "sensor_id": geohash,
        "reason": REASON_RISING_TREND,
        "window_start": latest_blob.get("window_start"),
        "window_end": latest_blob.get("window_end"),
        "cpm": latest_blob.get("cpm_max"),
        "breach_count": len(cpm_series),
        "triggered_at": latest_blob.get("window_end"),
        "geohash": geohash,
        "latitude": latest_blob.get("centroid_latitude"),
        "longitude": latest_blob.get("centroid_longitude"),
        "trend": {
            "window_count": len(cpm_series),
            "cpm_series": list(cpm_series),
            "delta": round(cpm_series[-1] - cpm_series[0], 4) if cpm_series else 0.0,
        },
    }


class TrendDetectorOperator(KeyedProcessFunction):
    """Emits a rising-trend alert when a geohash cell's cpm_avg rises across N windows.

    The stream must be keyed by geohash upstream so each cell owns a separate series in
    ValueState. On each aggregated blob the cell's ``cpm_avg`` is pushed onto a rolling
    series (trimmed to ``min_windows``); when the series is rising the operator emits an
    alert and clears the series so it does not re-fire every subsequent window.
    """

    def __init__(
        self,
        min_windows: int = _DEFAULT_MIN_WINDOWS,
        min_delta: float = _DEFAULT_MIN_DELTA,
        ttl_seconds: int = _STATE_TTL_SECONDS,
    ) -> None:
        self._min_windows = min_windows
        self._min_delta = min_delta
        self._ttl_seconds = ttl_seconds
        self._series = None

    def open(self, runtime_context) -> None:
        from pyflink.common.time import Time
        from pyflink.common.typeinfo import Types
        from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

        descriptor = ValueStateDescriptor("trend-cpm-series", Types.STRING())
        ttl_config = (
            StateTtlConfig.new_builder(Time.seconds(self._ttl_seconds))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanup_full_snapshot()
            .build()
        )
        descriptor.enable_time_to_live(ttl_config)
        self._series = runtime_context.get_state(descriptor)

    def process_element(self, blob: dict, ctx) -> Iterator[dict]:
        cpm_avg = blob.get("cpm_avg")
        if cpm_avg is None:  # an empty window carries no average — nothing to trend
            return

        raw = self._series.value()
        series = json.loads(raw) if raw else []
        series = push_window(series, float(cpm_avg), self._min_windows)

        if is_rising(series, self._min_windows, self._min_delta):
            yield build_trend_alert(blob["geohash"], series, blob)
            self._series.clear()  # cooldown-by-reset: start a fresh series after alerting
        else:
            self._series.update(json.dumps(series))


def build_trend_detection(aggregated_stream):
    """Wire the rising-trend detector onto the in-job aggregated stream.

    Args:
        aggregated_stream: ``DataStream[dict]`` of ``radiation.aggregated`` blobs (each
            carries a ``geohash`` and ``cpm_avg``).

    Returns:
        ``DataStream[dict]`` of rising-trend alerts, ready to union with the sustained-high
        alert stream into the ``radiation.alerts`` KafkaSink.
    """
    from pyflink.common.typeinfo import Types

    return aggregated_stream.key_by(lambda blob: blob["geohash"], key_type=Types.STRING()).process(
        TrendDetectorOperator()
    )
