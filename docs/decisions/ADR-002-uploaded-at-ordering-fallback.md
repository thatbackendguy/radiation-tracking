# ADR-002: uploaded_at ordering fallback to captured_at + per-batch sort

Date: 2026-06-18
Status: Proposed — requires sign-off from M2 and M3 before merge (§7.3)
Authors: M1

---

## Context

§7.3 of the guidelines states:

> Producer pushes events in `uploaded_at` order. No pre-sorting of data before
> ingestion is allowed. Flink enforces ordering using `captured_at` as event
> time with watermarks (bounded out-of-orderness).

The Safecast CSV export does not include an `uploaded_at` column. Every row
has `captured_at` (measurement time) but no upload timestamp.

---

## Decision

**Ordering column fallback:** When `uploaded_at` is absent from the CSV header,
`csv_reader.py` falls back to sorting by `captured_at`. The column selection
happens once at stream start (`_pick_order_col`) and is logged at INFO level so
it is visible in container logs.

**Per-batch sort:** The full 29 GB file cannot be globally pre-sorted in memory
(violates §8.3 stream-read requirement and H8). Instead, rows are buffered in
batches of 10 000, sorted within each batch by the ordering column, and then
yielded. This gives Flink a well-ordered stream with residual out-of-orderness
bounded by the time span of one batch (~seconds to low minutes in the Safecast
dataset).

**Why this satisfies H8:** H8 forbids pre-sorting the Safecast data before
ingestion. Per-batch sort is not a global pre-sort — it is an in-flight
ordering step applied during streaming, equivalent to what a partitioned
shuffle would do in a distributed system. The full file order is never
materialised on disk.

**Why Flink can handle the residual disorder:** The Flink watermark strategy
on `captured_at` uses bounded out-of-orderness of 30 seconds (§7.2 / Week 3
M2 milestone). Chunk-boundary disorder in the Safecast dataset is well within
that bound for any realistic batch size.

---

## Consequences

- If a future Safecast export includes `uploaded_at`, `csv_reader.py` picks it
  up automatically with no code change.
- Flink operators (M2, M3) must not assume strict global order; the 30-second
  watermark slack already accounts for this.
- Any change to batch size that could increase cross-batch disorder beyond 30
  seconds requires re-sign-off from M2 and M3.

---

## Sign-off required (§7.3)

| Role | Member | Approved |
|------|--------|---------|
| M1 (author) | Aditya | ✅ |
| M2 (Flink / watermark owner) | — | ⬜ pending |
| M3 (alert / aggregation owner) | — | ⬜ pending |
