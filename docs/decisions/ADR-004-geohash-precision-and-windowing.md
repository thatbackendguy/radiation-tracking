# ADR-004: geo-bucket aggregation — geohash precision 5 and tumbling event-time windows

Date: 2026-06-24
Status: Accepted
Authors: M2 (Sahil)

---

## Context

The Week-5 plan (approach.md / guidelines.md) assigns M2 the geo-bucket aggregation:

> Geo-bucket aggregation (geohash precision 5/6), session/tumbling windows, emit to
> `radiation.aggregated`.

The map needs density/heat blobs and region filters rather than millions of individual
markers, so cleaned events are grouped into geographic cells and time windows and reduced
to per-cell summaries. With M3's `captured_at` event-time watermarks now on `main`
(`operators/watermark.py`, via !24), the windowed aggregation is unblocked.

`guidelines.md §8` calls out geohash precision as a decision that belongs in an ADR, and the
window type / window length are likewise contract-shaping (they set the granularity the
Backend and frontend consume), so they are recorded here.

---

## Decision

**Bucketing — geohash precision 5 (`operators/geo_bucket.py`).** Each cleaned event's
`latitude`/`longitude` is encoded with `pygeohash` to a precision-5 geohash (~4.9 km cells).
Precision 5 is coarse enough to group neighbouring readings into a meaningful blob yet fine
enough to localise a hotspot. Overridable via `FLINK_GEOHASH_PRECISION` for tuning. The
geohash is an internal aggregation key added on the aggregation branch only — it is **not**
part of the `radiation.clean` contract.

**Windows — tumbling event-time, default 60 s (`operators/geo_window.py`).** Events are
keyed by geohash and aggregated over epoch-aligned `TumblingEventTimeWindows` on the
watermarked stream; window length is set by `FLINK_AGG_WINDOW_SECONDS` (default 60). Tumbling
(not session) windows give deterministic, non-overlapping blobs that are trivial to render
and to test, and align naturally with a live "last N seconds" map view.

**Output — rich-stats blob (`schemas/radiation_aggregated.json`).** One record per
(geohash, window): `count`, `cpm_avg`/`cpm_max`/`cpm_min`, `class_counts`
(`SAFE`/`WARN`/`DANGER`), `worst_classification`, and the cell centroid. The pure reducer
`operators/geo_aggregation.py::aggregate_bucket` computes these; `GeoBucketWindowFunction`
supplies the window bounds. Field names mirror the schema so the Backend (M4) parses output
as-is. Published to `radiation.aggregated` with `AT_LEAST_ONCE`, as a parallel branch off the
classified `clean_stream` — the `radiation.clean` sink is unaffected.

---

## Consequences

- `radiation.aggregated` carries compact per-cell/per-window summaries the frontend can render
  as a density/heat overlay and filter by region/timespan, instead of per-event markers.
- Precision and window length are tunable per environment via env vars without a code change.
  Coarser precision / longer windows = fewer, larger blobs; finer / shorter = more detail.
- Tumbling windows fire on watermark progress; a sparse cell only emits once enough watermark
  advances (idle-source handling in `operators/watermark.py` keeps sparse sensors from stalling
  windows). Late events beyond the 30 s bounded-out-of-orderness are dropped, not re-aggregated.
- The pure reducer is unit-tested without Flink; event-time membership under a shuffled
  `captured_at` stream is covered by `tests/integration/test_event_time_windows.py`.

---

## Alternatives considered

- **Precision 6 (~1.2 km cells):** finer hotspot localisation but ~25× more cells and noisier
  blobs at the current sample rate. Kept available via `FLINK_GEOHASH_PRECISION`; revisit if
  the map needs finer resolution.
- **Session windows:** better for bursty per-sensor activity, but overlapping/variable windows
  are harder to render deterministically and to test. Deferred — can be added behind a toggle
  if a use case appears (Week-6 rolling-average/anomaly work may want it).
- **`aggregate()` incremental reducer instead of `process()`:** lower memory (no buffering of
  window elements) but cannot emit window-start/end bounds or compute the centroid as cleanly.
  Chosen `ProcessWindowFunction` for the richer blob; window sizes here are small enough that
  buffering is not a concern.

---

## Correction (2026-06-24) — watermark propagation past the broadcast classifier

The first cut of this aggregation windowed `clean_stream` directly. That stream is the output of
the classifier's broadcast connect (`events.connect(config_stream.broadcast(...)).process(...)`),
and the `config.updates` side uses `WatermarkStrategy.no_watermarks()`. A two-input operator emits
the **minimum** of its inputs' watermarks, so the broadcast input pins the propagated watermark at
`Long.MIN_VALUE` — the tumbling event-time windows never advance and `radiation.aggregated` stays
empty. The `radiation.clean` sink is unaffected (it is a per-event map→sink with no windows), and
the unit/integration tests run without a Flink runtime, so nothing caught this.

**Fix:** the aggregation re-assigns event-time watermarks on the dict stream immediately before
windowing — `clean_stream.assign_timestamps_and_watermarks(build_dict_watermark_strategy())` in
`operators/geo_window.build_geo_aggregation`, re-deriving event time from `captured_at` with the
same 30 s bounded-out-of-orderness and 60 s idleness as the source. This detaches the aggregation
from the stalled broadcast input without touching the `radiation.clean` path or the classifier.

**Alternative considered (rejected):** make `config_stream` stop holding the watermark back (mark it
idle or emit `MAX_WATERMARK`). It would cover any future windowed operator off `clean_stream`, but
it still stalls at startup before idleness trips and edits wiring that M3 also depends on. The
localized re-assignment is lower-risk and scoped to M2's branch.

---

## Correction (2026-06-26) — `.with_idleness()` order dropped the timestamp assigner

After the re-assignment landed, the windows fired but bucketed events by **Kafka ingestion time**,
not `captured_at` — a silent H8 violation. Root-caused on a real `flink run` session cluster: in
PyFlink, building the strategy as
`for_bounded_out_of_orderness(...).with_timestamp_assigner(a).with_idleness(d)` **drops the
timestamp assigner** when `.with_idleness()` is chained *after* `.with_timestamp_assigner()`.
Records then carry no event-time timestamp (`Long.MIN_VALUE`); Flink falls back to the source's
Kafka record (ingestion) timestamp, so the windows align to arrival time. A constant assigner was
ignored too, confirming the assigner — not `captured_at` parsing — was being dropped.

**Fix:** in `operators/watermark.py::_with_assigner`, apply `.with_idleness()` **first** and
`.with_timestamp_assigner()` **last**. This fixes both `build_watermark_strategy` (source) and
`build_dict_watermark_strategy` (the geo and alert re-assigns) at once. Verified on the cluster:
window bounds align to `captured_at` only with the assigner applied last (e.g. 2021-dated events
produce 2021 windows, out-of-arrival-order).
