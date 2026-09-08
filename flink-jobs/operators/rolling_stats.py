"""
rolling_stats.py — rolling CPM average + z-score anomaly enrichment per geohash.

The geo-bucket aggregation (operators/geo_window) already reduces each geohash cell's
readings within one tumbling window into a blob carrying ``cpm_avg``. This operator adds a
*cross-window* view on top: keyed by geohash, it keeps the last N windows' ``cpm_avg`` in
ValueState and enriches every blob with

  * ``rolling_cpm_avg`` — the smoothed average CPM over this window plus up to N prior
    windows (up to N+1 values), and
  * ``cpm_zscore`` — how far this window's ``cpm_avg`` sits from that recent baseline, in
    standard deviations, and
  * ``anomaly`` — whether ``|cpm_zscore|`` crosses the configured threshold.

Unlike the rising-trend detector (ADR-008), which fires on a *monotonic climb* and emits a
secondary alert, this is a *statistical outlier* signal (a spike or dip in either
direction) and it stays on ``radiation.aggregated`` — it only adds fields to the blob, so
the aggregated sink, the hot-region join, and any other aggregated consumer keep working
unchanged. See docs/decisions/ADR-009.

The pure helpers (``push_sample`` / ``rolling_average`` / ``zscore`` / ``flag_anomaly`` /
``enrich_blob``) carry no PyFlink import and are unit-tested directly; ``RollingStatsOperator``
is the thin keyed wrapper (PyFlink imported with a stub fallback, same pattern as
operators/trend_detector.py and operators/alert_dedup.py).
"""

from __future__ import annotations

import json
import math
import os
from typing import Iterator, Optional

try:
    from pyflink.datastream.functions import KeyedProcessFunction
except ImportError:  # running tests outside the Flink Docker image

    class KeyedProcessFunction:  # type: ignore[no-redef]
        def open(self, runtime_context) -> None: ...

        def process_element(self, value, ctx):
            raise NotImplementedError


# How many recent aggregation windows form the rolling baseline, and how many standard
# deviations a window's cpm_avg must sit from that baseline to be flagged an anomaly. Read
# once at import; override via env for tuning. See docs/decisions/ADR-009.
_DEFAULT_WINDOW_COUNT = int(os.environ.get("FLINK_ROLLING_WINDOWS", "6"))
_DEFAULT_ZSCORE_THRESHOLD = float(os.environ.get("FLINK_ZSCORE_THRESHOLD", "3.0"))

# How long (seconds) to keep a quiet cell's series before it expires, bounding per-geohash
# state over a long soak (mirrors the trend-detector / dedup TTL discipline).
_AGG_WINDOW_SECONDS = int(os.environ.get("FLINK_AGG_WINDOW_SECONDS", "60"))
_STATE_TTL_SECONDS = max(_AGG_WINDOW_SECONDS * 10, 3600)


def push_sample(series: list[float], cpm_avg: float, max_len: int) -> list[float]:
    """Append a window's ``cpm_avg`` to the rolling series, trimmed to ``max_len`` items."""
    updated = [*series, cpm_avg]
    return updated[-max_len:]


def rolling_average(series: list[float]) -> Optional[float]:
    """Mean of the rolling series, or None when it is empty."""
    if not series:
        return None
    return round(sum(series) / len(series), 4)


def zscore(series: list[float], value: float) -> Optional[float]:
    """Z-score of ``value`` against the baseline ``series`` (population std).

    Returns None when the baseline is too small (< 2 samples) or flat (std == 0) — in
    both cases a deviation is undefined / meaningless, so the caller reports no z-score
    rather than a divide-by-zero or a spurious 0.
    """
    if len(series) < 2:
        return None
    mean = sum(series) / len(series)
    variance = sum((x - mean) ** 2 for x in series) / len(series)
    std = math.sqrt(variance)
    if std == 0:
        return None
    return round((value - mean) / std, 4)


def flag_anomaly(z: Optional[float], threshold: float = _DEFAULT_ZSCORE_THRESHOLD) -> bool:
    """Whether a z-score crosses the anomaly threshold in either direction.

    None (undefined z-score — too little history or a flat baseline) is not an anomaly.
    """
    return z is not None and abs(z) >= threshold


def enrich_blob(
    blob: dict,
    baseline: list[float],
    threshold: float = _DEFAULT_ZSCORE_THRESHOLD,
) -> dict:
    """Return a copy of ``blob`` with rolling_cpm_avg / cpm_zscore / anomaly added.

    ``baseline`` is the cell's series of *prior* windows' cpm_avg (this window excluded, up to
    N values), so the current window is scored against its own history. ``rolling_cpm_avg``
    then averages the baseline together with this window (up to N+1 values), while
    ``cpm_zscore`` uses the prior-only baseline. A blob with no ``cpm_avg`` (an empty window) is
    passed through with null rolling stats and anomaly False — it must still reach the
    aggregated sink. Field names mirror schemas/radiation_aggregated.json.
    """
    cpm_avg = blob.get("cpm_avg")
    if cpm_avg is None:
        return {**blob, "rolling_cpm_avg": None, "cpm_zscore": None, "anomaly": False}

    # The rolling average includes the current window; the z-score baseline is prior-only so
    # a lone spike is not diluted by its own value.
    z = zscore(baseline, float(cpm_avg))
    return {
        **blob,
        "rolling_cpm_avg": rolling_average([*baseline, float(cpm_avg)]),
        "cpm_zscore": z,
        "anomaly": flag_anomaly(z, threshold),
    }


class RollingStatsOperator(KeyedProcessFunction):
    """Enriches each aggregated blob with its geohash cell's rolling CPM stats.

    The stream must be keyed by geohash upstream so each cell owns a separate baseline in
    ValueState. On each blob the operator scores this window's ``cpm_avg`` against the
    stored prior windows, emits the enriched blob, then pushes ``cpm_avg`` onto the series
    (trimmed to ``window_count``). Empty windows (no ``cpm_avg``) are passed through and do
    not disturb the baseline.
    """

    def __init__(
        self,
        window_count: int = _DEFAULT_WINDOW_COUNT,
        threshold: float = _DEFAULT_ZSCORE_THRESHOLD,
        ttl_seconds: int = _STATE_TTL_SECONDS,
    ) -> None:
        self._window_count = window_count
        self._threshold = threshold
        self._ttl_seconds = ttl_seconds
        self._series = None

    def open(self, runtime_context) -> None:
        from pyflink.common.time import Time
        from pyflink.common.typeinfo import Types
        from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

        descriptor = ValueStateDescriptor("rolling-cpm-series", Types.STRING())
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
        raw = self._series.value()
        baseline = json.loads(raw) if raw else []

        yield enrich_blob(blob, baseline, self._threshold)

        cpm_avg = blob.get("cpm_avg")
        if cpm_avg is not None:  # empty windows leave the baseline untouched
            self._series.update(
                json.dumps(push_sample(baseline, float(cpm_avg), self._window_count))
            )


def build_rolling_stats(aggregated_stream):
    """Wire the rolling-stats enrichment onto the in-job aggregated stream.

    Args:
        aggregated_stream: ``DataStream[dict]`` of ``radiation.aggregated`` blobs (each
            carries a ``geohash`` and ``cpm_avg``).

    Returns:
        ``DataStream[dict]`` of the same blobs, each additively enriched with
        ``rolling_cpm_avg`` / ``cpm_zscore`` / ``anomaly``.
    """
    from pyflink.common.typeinfo import Types

    return aggregated_stream.key_by(lambda blob: blob["geohash"], key_type=Types.STRING()).process(
        RollingStatsOperator()
    )
