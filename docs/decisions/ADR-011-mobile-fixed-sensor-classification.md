# ADR-011: mobile-vs-fixed sensor classification at the producer

Date: 2026-07-02
Status: Proposed — requires sign-off from M2 and M4 before merge (§7.1)
Authors: M1 (Aditya)

---

## Context

The Week-6 plan (approach.md) assigns M1:

> Mobile-vs-fixed sensor classification heuristic in producer or pre-Flink stage.

Safecast data comes from two kinds of device: **fixed** installations that
report from a single location (with a little GPS jitter) and **mobile** bGeigie
units that are carried or driven around. Downstream this distinction matters —
e.g. the Flink `SensorDedupOperator` keys on `sensor_id` assuming a fixed sensor
re-reports from the same place, and the frontend replaces fixed-sensor markers
in place rather than accumulating them. The raw Safecast CSV carries no such
label, so it has to be inferred.

The classification needs **cross-row state** (a single row cannot reveal whether
its sensor moves), which rules out doing it in `mapper.py` (pure per-row). The
producer already iterates the whole stream in order and owns per-run state, so
it is the natural place — and keeps this a pre-Flink concern, as the plan allows.

---

## Decision

**Where:** a new `data-provider/sensor_classifier.py` (`SensorClassifier`),
invoked in `producer.run()` after `row_to_event` and before publish. Each event
gains a `sensor_type` field of `"mobile"` or `"fixed"`.

**Heuristic:** per sensor, maintain a running bounding box of every
latitude/longitude seen. The sensor is `"mobile"` once the great-circle distance
across the box's diagonal exceeds `movement_threshold_m` (default **200 m**,
configurable via `--movement-threshold-m` / `MOVEMENT_THRESHOLD_M`); otherwise
`"fixed"`. 200 m sits comfortably above consumer-GPS jitter (tens of metres) and
far below the distance a mobile unit covers (kilometres).

**Sticky:** once a sensor is seen to move it stays `"mobile"` for the rest of the
run, even if later readings re-cluster. A sensor that stops still moved.

**Schema:** `schemas/radiation_event.json` gains an optional
`sensor_type` enum `["mobile", "fixed", null]`. It is **not** added to
`required`, so pre-existing `radiation.raw` fixtures and any other producer keep
validating (backward compatible). Because the schema is `additionalProperties:
false`, the field must be declared for producer output to validate at all —
hence this ADR under §7.1.

---

## Consequences

- **Streaming limitation (accepted):** a mobile sensor is reported `"fixed"`
  until it has actually moved far enough; early events from a mobile unit carry
  the `"fixed"` label. The label can only sharpen as more readings arrive, never
  regress. Consumers should treat `sensor_type` as a hint, not ground truth.
- **Memory:** one small state record per *distinct* `sensor_id` (four floats +
  a flag). Bounded by device count in the dataset, not stream length —
  consistent with §8.3 streaming constraints.
- **No re-ordering / no pre-sort:** classification is a stateless-per-event tag
  computed in stream order; it does not buffer or reorder, so **H8 is untouched**.
- Flink/Backend may later use `sensor_type` (e.g. skip dedup for mobile sensors);
  that is out of scope here and left to M2/M4.

---

## Alternatives considered

- **Anchor-point distance** (distance from first-seen coordinate) — simpler, but
  a single GPS outlier as the anchor skews every later comparison. The bounding
  box captures the true spread regardless of arrival order.
- **Consecutive-gap threshold** (distance between successive readings) — misses a
  sensor that drifts far in many small steps; the cumulative box does not.
- **Classify in Flink** — possible, but M1 owns the producer and the plan permits
  a "pre-Flink stage"; doing it here keeps `radiation.raw` self-describing.

---

## Sign-off required (§7.1 — schema change)

| Role | Member | Approved |
|------|--------|---------|
| M1 (author) | Aditya | ✅ |
| M2 (Flink / clean-stream consumer) | — | ⬜ pending |
| M4 (Backend / clean-stream consumer) | — | ⬜ pending |
