# ADR-009: rolling-average / z-score anomaly enrichment on radiation.aggregated

Date: 2026-07-01
Status: Accepted
Authors: M2 (Sahil)

---

## Context

The Week-6 plan (approach.md) assigns M2 an advanced operator:

> PyFlink operator: rolling average per geohash, anomaly z-score.

The geo-bucket aggregation (ADR-004) already reduces each geohash cell's readings within one
event-time tumbling window into a `radiation.aggregated` blob carrying `cpm_avg`/`cpm_max`/…
That is a *single-window* view. What Week 6 adds is a *cross-window* statistical view per
cell: a smoothed rolling average and a measure of how unusual the current window is against
the cell's own recent history.

M3 owns a neighbouring Week-6 signal — the rising-CPM **trend detector** (ADR-008), which
fires a secondary `radiation.alerts` record when a cell's `cpm_avg` climbs monotonically
across N windows. The two must not collide, in schema or in meaning.

---

## Decision

**Signal — statistical outlier, not a monotonic trend.** The operator keeps the last N
windows' `cpm_avg` per geohash and computes a z-score of the current window against that
baseline. It flags an `anomaly` when `|z|` crosses a threshold, in **either** direction — a
sudden spike *or* dip. This is deliberately distinct from ADR-008's rising-trend alert:
ADR-008 catches a slow climb (each step higher than the last); this catches a single window
that departs from the cell's normal level. A region can be anomalous without trending, and
vice versa.

**Output — enrich `radiation.aggregated`, do not touch `radiation.alerts`.** The three new
fields (`rolling_cpm_avg`, `cpm_zscore`, `anomaly`) are **added** to the existing aggregated
blob (`schemas/radiation_aggregated.json`); every existing field is preserved. This keeps the
signal on M2's own topic and, critically, **decouples it from M3's trend detector (MR !35)**,
which extends the separate `schemas/radiation_alert.json`. There is no shared schema and no
new Kafka topic (which would need an M1/M2/M4 contract change). The map's density overlay and
legend (M5) can shade or badge anomalous cells straight from the aggregated feed.

**Placement — after the aggregation, before the sink and the join.** `build_rolling_stats`
is inserted immediately after `build_geo_aggregation`; the aggregated sink and the hot-region
join (ADR-007) then consume the enriched stream. Because enrichment is purely additive
(preserving `geohash`, `cpm_avg`, centroid, …), every downstream consumer — including M3's
trend detector once MR !35 lands, which reads `geohash`/`cpm_avg` — keeps working unchanged.

**Baseline geometry.** The rolling average spans the current window plus up to N prior windows
(up to N+1 values); the z-score baseline is **prior windows only** (up to N), so a lone spike
is scored against history it has not yet polluted.
A z-score is undefined (reported `null`, `anomaly=False`) when the baseline has fewer than two
windows or is perfectly flat (zero variance — avoids divide-by-zero). Defaults:
`FLINK_ROLLING_WINDOWS=6`, `FLINK_ZSCORE_THRESHOLD=3.0`. Per-cell series live in keyed
`ValueState` with a TTL (`max(10 × agg-window, 3600s)`), mirroring the trend-detector / dedup
TTL discipline so quiet cells do not accumulate state over a long soak.

---

## Consequences

- Adds a fifth+ distinct PyFlink operator toward the Definition-of-Done operator count.
- `radiation.aggregated` consumers (Backend M4) receive three extra optional fields; the
  contract is backward-compatible (additive, `additionalProperties: false` updated).
- Empty windows (no `cpm_avg`) pass through with `null` rolling stats and do not disturb the
  baseline, so the aggregated sink still emits one record per cell/window.
- No dependency on MR !35: this change and the trend detector share only a small, mechanical
  edit region in `radiation_tracking_job.py`; whichever merges second resolves a trivial
  conflict. Neither needs the other to function.
