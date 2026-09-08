"""Checkpoint/restart state-recovery test for the rolling-stats operator (no MiniCluster).

PyFlink and the Flink MiniCluster are unavailable in CI (see the operator stub-import
pattern), so this exercises the *state-recovery contract* the job's checkpointing relies on at
the logical level. ``RollingStatsOperator`` persists its per-geohash cpm_avg baseline as a JSON
string in keyed ValueState (operators/rolling_stats). A checkpoint serialises that string; a
restart deserialises it and resumes. This test drives the same pure state transitions,
snapshots the state to JSON mid-stream, restores it into a fresh series, and asserts the
enriched outputs after the "restart" are identical to an uninterrupted run.
"""

from __future__ import annotations

import json

from operators.rolling_stats import enrich_blob, push_sample

_WINDOW_COUNT = 6


def _blob(cpm_avg: float) -> dict:
    return {"geohash": "xn774", "precision": 5, "cpm_avg": cpm_avg, "count": 3}


def _process(series: list[float], blob: dict, window_count: int = _WINDOW_COUNT):
    """Mirror RollingStatsOperator.process_element on a plain series (no ValueState).

    Enrich against the prior baseline, then fold the current cpm_avg into the series exactly
    as the operator persists it. Returns ``(enriched_blob, next_series)``.
    """
    enriched = enrich_blob(blob, series)
    cpm_avg = blob.get("cpm_avg")
    if cpm_avg is not None:
        series = push_sample(series, float(cpm_avg), window_count)
    return enriched, series


def _run(cpms: list[float], restart_after: int | None = None) -> list[dict]:
    """Process a sequence of cpm_avg windows, optionally checkpoint+restart midway.

    When ``restart_after`` is set, after that many windows the series is snapshotted to JSON
    (as ValueState would be at a checkpoint) and reloaded into a fresh series — simulating a
    JobManager/TaskManager restart that recovers from the checkpoint.
    """
    series: list[float] = []
    out: list[dict] = []
    for i, cpm in enumerate(cpms):
        if restart_after is not None and i == restart_after:
            series = json.loads(json.dumps(series))  # checkpoint → restart round-trip
        enriched, series = _process(series, _blob(cpm))
        out.append(enriched)
    return out


# A spike (60) after a settled baseline so the z-score/anomaly depends on recovered history.
_STREAM = [10.0, 12.0, 11.0, 40.0, 13.0, 12.5, 60.0, 11.0]


class TestCheckpointRestart:
    def test_state_survives_restart_midstream(self) -> None:
        assert _run(_STREAM, restart_after=4) == _run(_STREAM)

    def test_restart_preserves_rolling_and_anomaly(self) -> None:
        # window 6 (cpm 60) is a spike scored against the recovered baseline; it must be
        # flagged the same whether or not a restart happened just before it
        restarted = _run(_STREAM, restart_after=4)
        uninterrupted = _run(_STREAM)
        assert restarted[6]["anomaly"] is True
        assert restarted[6]["rolling_cpm_avg"] == uninterrupted[6]["rolling_cpm_avg"]

    def test_restart_at_any_point_is_equivalent(self) -> None:
        uninterrupted = _run(_STREAM)
        for k in range(len(_STREAM)):
            assert _run(_STREAM, restart_after=k) == uninterrupted

    def test_snapshot_is_json_serialisable(self) -> None:
        # the ValueState holds json.dumps(series); a checkpoint must be able to serialise it
        _, series = _process([10.0, 12.0], _blob(15.0))
        assert json.loads(json.dumps(series)) == series
