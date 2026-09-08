# ADR-007: alerts × aggregated "hot region" join — geohash-keyed co-process with TTL state

Date: 2026-06-30
Status: Accepted
Authors: M3 (Yash)

---

## Context

The Week-5 plan (approach.md) assigns M3, after the sustained-high alert operator (ADR-005):

> then multi-stream join alerts × aggregated for "hot region" labels.

ADR-005 shipped the alert operator and explicitly deferred this join as a follow-up,
noting the alert schema could be extended additively with a `hot_region` block "without
breaking consumers". This is that follow-up.

A sustained-high alert today carries only the *triggering sensor's* latest breach
(`sensor_id`, peak `cpm`, the breach location). The map wants to label the **region**
around the alert — how hot the surrounding cell is on average, how many readings, the
worst class seen — which is exactly what the geo aggregation (ADR-004) already computes
per geohash cell on `radiation.aggregated`. Joining the two gives each alert its regional
context.

The backend (M4) consumes `radiation.alerts` and fans it out as-is; the frontend (M5)
renders it. Both must keep working, so the join must extend the alert **additively**.

---

## Decision

**Join key — geohash, at the aggregation's precision.** The aggregated blob already
carries a `geohash` (precision 5, ADR-004). The alert is hashed from its breach
`latitude`/`longitude` with the same `operators/geo_bucket.geo_bucket` (default
`FLINK_GEOHASH_PRECISION`), so an alert and the cell it sits in key together. Reusing the
one geohash function keeps the two sides on identical cell boundaries — no second
precision constant to drift.

**Operator — `KeyedCoProcessFunction` keyed by geohash (`operators/alert_region_join.py`).**
Input 2 (aggregated blobs) updates a per-cell `ValueState[str]` holding the *latest* blob
as JSON; input 1 (alerts) reads that state on arrival and emits the alert enriched with a
`hot_region` block. Alerts are emitted **immediately, best-effort** — an alert that fires
before its cell's first aggregation window (or in a window not yet seen) gets
`hot_region: None` rather than being held back. Correctness of the alert itself never
depends on the join.

**Null-coordinate guard.** Alert `latitude`/`longitude` are nullable
(`schemas/radiation_alert.json`). `alert_geohash` returns `None` for a null coordinate
instead of calling `pygeohash.encode(None, …)`; such alerts pass through with
`hot_region: None`. Without the guard an unhandled error would fail the operator and Flink
would crash-loop it on restart (same poison-pill reasoning as `_parse_record`).

**State TTL.** The per-geohash latest-blob state carries a TTL of
`max(FLINK_AGG_WINDOW_SECONDS·10, 3600)s` (`StateTtlConfig`, full-snapshot cleanup), so
cells that go quiet release their state and per-key state cannot grow unbounded over a long
soak — mirroring `AlertCooldownOperator` (ADR-005) and `SensorDedupOperator` (ADR-003).

**Schema — additive.** `radiation_alert.json` gains an optional `geohash` (string) and an
optional `hot_region` object (`cpm_avg`, `cpm_max`, `count`, `worst_classification`,
`centroid_latitude`, `centroid_longitude`). Existing required fields are untouched, so the
backend/frontend parse old and new records identically.

**Placement.** The join sits between the cooldown dedup and the `radiation.alerts` sink, so
it enriches the already-deduped alert stream (one record per sensor per cooldown), not the
raw overlapping sliding-window candidates.

---

## Consequences

- Each `radiation.alerts` record now carries the surrounding cell's stats when known,
  enabling a "hot region" map label without a new topic or a backend change.
- The join re-keys the alert stream `sensor_id → geohash`; co-keying both inputs by geohash
  co-locates them on the same key group. One extra keyed shuffle on the (low-volume,
  post-cooldown) alert stream — negligible cost.
- The enrichment is event-order tolerant: it reflects the most recent aggregation seen for
  the cell at the moment the alert arrives. It is not a windowed temporal join, so it does
  not wait for or align window bounds — a deliberate simplicity/latency trade.
- Pure helpers (`alert_geohash`, `region_summary`, `enrich_alert_with_region`) are
  unit-tested without Flink; the operator is driven through a fake `ValueState`.

---

## Alternatives considered

- **Windowed interval/temporal join (align alert window with aggregation window):** more
  precise time alignment but heavier, and an alert's value does not hinge on which exact
  60 s window it lands in — the latest cell stats are the useful signal. Rejected for
  complexity with little gain.
- **Re-read `radiation.aggregated` from Kafka in a separate job:** adds a Kafka round-trip
  and a second job to operate. Joining the in-job `aggregated_stream` keeps it in one graph.
- **Hash the alert at a coarser precision for a wider "region":** would mismatch the
  aggregation's cell boundaries and need its own re-aggregation. Reusing precision 5 keeps a
  single source of truth; a wider region can be layered later if needed.
- **Drop alerts with no coordinates:** loses real alerts over a missing GPS field. Emitting
  them with `hot_region: None` keeps the alert and degrades only the enrichment.
