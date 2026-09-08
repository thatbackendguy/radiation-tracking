# ADR-008: rising-CPM trend detector — per-geohash secondary alert stream

Date: 2026-06-30
Status: Accepted
Authors: M3 (Yash)

---

## Context

The Week-6 plan (approach.md) assigns M3 an advanced operator:

> PyFlink operator: trend detector (rising CPM over N windows) → secondary alert stream.

The sustained-high alert (ADR-005) only fires on *absolute* DANGER readings — a region can
be steadily heating up, window over window, while every reading is still below the DANGER
threshold, and nothing alerts until it crosses. A **trend** signal catches that climb
early. This is also the operator that, together with the hot-region join (ADR-007), takes
M3 past the Definition-of-Done bar of "≥ 5 PyFlink operators + 1 multi-stream join".

The geo aggregation (ADR-004) already emits one rich-stats blob per geohash cell per
tumbling window on `radiation.aggregated`, carrying `cpm_avg`. That per-cell, per-window
average is exactly the series a trend detector needs.

---

## Decision

**Signal — `cpm_avg` strictly rising across N consecutive aggregation windows per cell.**
A geohash cell alerts when its last `FLINK_TREND_WINDOWS` (default 3, floored at 2) window
averages are each strictly greater than the previous, with a total rise ≥
`FLINK_TREND_MIN_DELTA` (default 5.0 CPM) to filter trivial drift. The delta default is
deliberately non-zero: with the default WARN threshold at 100 CPM, a 5 CPM climb across the
windows is a genuine regional heat-up, whereas a `0` default (strict-monotonic only) would
fire on sub-CPM sensor jitter (e.g. `10.00 → 10.01 → 10.02`) and flood `radiation.alerts`
with low-value trend alerts. `FLINK_TREND_WINDOWS` is floored at 2 because a "rise" needs at
least two samples — a stray `1` would otherwise silently disable every alert with no warning.
Per-cell (geohash), not per-sensor, because a trend is a regional phenomenon and matches the
aggregation's keying.

**Source — the in-job `aggregated_stream`, not a re-read of Kafka.** The detector consumes
the same `DataStream[dict]` of aggregated blobs the `radiation.aggregated` sink uses (a
third fan-out, alongside the sink and the ADR-007 join). No extra Kafka round-trip, no
second job; the aggregation's event-time watermarks already flow through its window into
this branch.

**Operator — `KeyedProcessFunction` keyed by geohash (`operators/trend_detector.py`).**
Each cell keeps its last N `cpm_avg` values in `ValueState` (JSON list, trimmed by
`push_window`). On each blob the value is pushed; when `is_rising` holds, an alert is
emitted and the series is **cleared (cooldown-by-reset)** so a single sustained climb
yields one alert per N rising windows rather than one every window. Empty windows
(`cpm_avg is None`) are skipped, not treated as a break in the trend.

**State TTL.** The per-cell series carries a TTL of `max(FLINK_AGG_WINDOW_SECONDS·10,
3600)s` so quiet cells release state — same discipline as ADR-003/005/007, keeping per-key
state bounded over a long soak.

**Output — `radiation.alerts` with `reason: "rising-trend"`, additive.** The trend alert
stream is **unioned** with the hot-region-enriched sustained-high stream into the single
existing alerts sink (two logical sources, one topic). The cell's `geohash` stands in for
`sensor_id` (the alert identifier), `cpm` is the latest window peak, location is the cell
centroid, and a `trend` block carries `{window_count, cpm_series, delta}`. The backend
(M4) forwards `{sensor_id, cpm, reason}` unchanged.

**Schema changes (backward-compatible).** `radiation_alert.json` gains `"rising-trend"` in
the `reason` enum and an optional `trend` block, and `breach_count` is dropped from
`required` (it is meaningful only for sustained-high alerts; relaxing `required` never
invalidates existing records). `classification` is omitted on trend alerts (its enum is
DANGER-only and a trend is not a DANGER breach).

---

## Consequences

- An early-warning regional signal lands on the same topic the map already renders, with no
  backend or frontend change — they distinguish kinds by `reason` if they wish.
- Pure helpers (`push_window`, `is_rising`, `build_trend_alert`) are unit-tested without
  Flink; the operator is driven through a fake `ValueState`. A serde round-trip covers the
  new `trend` block and the enriched `hot_region` block together.
- The trend cadence is tied to the aggregation window length, so at 1× vs 100× producer
  speed it tracks event-time data progression consistently (the aggregation is event-time).
- Knobs (`FLINK_TREND_WINDOWS`, `FLINK_TREND_MIN_DELTA`) are env-tunable without code change.

---

## Alternatives considered

- **Anomaly z-score / rolling stddev:** this is M2's Week-6 operator (rolling average +
  z-score). To avoid overlap, M3 owns the simpler, complementary "monotonic rise over N
  windows" signal; the two can co-exist as distinct alert reasons later.
- **Per-sensor trend (key by sensor_id):** noisier (single-sensor series are sparse and
  jumpy) and redundant with the sustained-high alert's keying. Per-geohash uses the smoothed
  cell average and matches the aggregation.
- **Re-firing every rising window (no reset):** would spam one alert per window during a long
  climb. Cooldown-by-reset collapses a climb to one alert per N rising windows — the trend
  analogue of the sustained-high cooldown (ADR-005).
- **Linear-regression slope test:** more statistically principled but heavier and harder to
  unit-test without numpy in the no-PyFlink CI path; strict monotonic rise + min-delta is
  transparent and dependency-free.
