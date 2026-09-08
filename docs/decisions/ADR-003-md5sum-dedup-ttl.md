# ADR-003: fixed-sensor dedup keyed on sensor_id by md5sum with state TTL

Date: 2026-06-21
Status: Accepted
Authors: M2 (Sahil)

---

## Context

Week-3 (§11) requires M2 to deduplicate readings from fixed sensors:

> Operators: filter null/empty CPM, range sanity check, fixed-sensor dedup
> (keyed state on `sensor_id` with TTL).

Fixed sensors re-report the same measurement, and a replay of the Safecast CSV (or a
producer retry under `acks=all`, `retries=3`) can re-emit an identical row. Without
dedup these reach `radiation.clean` as duplicate markers, which inflates the map and
any downstream aggregation. The dedup operator sits after the null/range filters and
before the `radiation.clean` sink in `radiation_tracking_job.py`.

The producer (`data-provider/mapper.py`) already emits an `md5sum` field on every event —
Safecast's own checksum when present, otherwise a hash computed over the original CSV
row — explicitly for deduplication.

---

## Decision

**Dedup identity (`operators/sensor_dedup.py::dedup_key`):** prefer `md5sum`. It is a
hash of the whole row, so identical re-reads collide and genuinely different readings do
not. When `md5sum` is absent/empty, fall back to `captured_at`. The stream is keyed by
`sensor_id` upstream (`key_by(...).process(SensorDedupOperator())`), so the fallback can
never conflate two different sensors that happen to share a timestamp — their state is
separate.

**Keyed state:** a per-`sensor_id` `MapState<str, bool>` of seen dedup keys. On each
event, drop if the key is already present, otherwise record it and emit.

**TTL (`StateTtlConfig`):** entries expire `FLINK_DEDUP_TTL_SECONDS` (default **3600 s**)
after their last write — `OnCreateAndWrite` update type, `NeverReturnExpired` visibility,
full-snapshot cleanup. This bounds per-sensor state so a long-running job cannot grow it
without limit. The window is a memory/correctness trade-off: a duplicate re-arriving more
than an hour later (rare for live replay) would pass through.

---

## Consequences

- `radiation.clean` carries at most one event per (`sensor_id`, `md5sum`) within the TTL
  window. The Backend no longer needs to dedup defensively for the common case.
- The TTL is tunable per environment via `FLINK_DEDUP_TTL_SECONDS` without a code change.
- Dedup is **not** exactly-once across a TTL expiry or a state loss; combined with the
  sink's `AT_LEAST_ONCE` guarantee, a vanishingly rare duplicate is possible. Acceptable
  for a visualisation pipeline; revisit if a strict-once downstream consumer appears.
- **M3 insertion point:** the threshold classifier (`SAFE`/`WARN`/`DANGER`) slots between
  `.process(SensorDedupOperator())` and the serialiser/sink, so it classifies the deduped
  stream. `classification` stays `null` until then.

---

## Alternatives considered

- **Dedup on `(sensor_id, captured_at)` only:** simpler, but two distinct readings sharing
  a timestamp (sensor bursts) would be wrongly dropped. Kept only as the md5sum fallback.
- **Window-based dedup:** a tumbling/session window keyed by `md5sum` would also work but
  adds event-time/watermark coupling that belongs to Week-4; keyed state + TTL is lighter
  and watermark-independent.
