# ADR-005: sustained-high alerts — DANGER-based sliding windows and cooldown dedup

Date: 2026-06-26
Status: Accepted
Authors: M3 (Yash)

---

## Context

The Week-5 plan (approach.md) assigns M3 the alert operator:

> Alert operator: sustained-high-CPM detection over a sliding window → `radiation.alerts`;
> … refine alert dedup.

The map needs to surface radiation hotspots, not just colour individual markers. The Kafka
topic table describes `radiation.alerts` as "Threshold breaches (e.g. > 300 CPM)". The
broadcast classifier (`operators/classifier.py`, ADR-001/!24) already tags each cleaned event
`SAFE`/`WARN`/`DANGER` from the user-configured `config.updates` thresholds, and the geo
aggregation (ADR-004) established the pattern for a keyed event-time windowed branch off the
classified `clean_stream`. The remaining decisions — what counts as "sustained high", the
window geometry, and how to avoid alert spam — are contract-shaping and recorded here.

The backend already consumes `radiation.alerts` best-effort and fans it out to the map client
as `{"type": "alert"}` (`backend/app/core/kafka_consumer.py`); its test asserts a
`{sensor_id, cpm, reason}` shape. The frontend already ignores `alert` messages as non-markers.
Only the Flink operator was missing.

---

## Decision

**Trigger — reuse the classifier's `DANGER` tag, not a second CPM threshold.** A reading
counts as a breach when its `classification == DANGER`. Because the classifier's thresholds
come from `config.updates` broadcast state, the alert threshold automatically tracks whatever
the user configures — there is no independent alert CPM constant to keep in sync with the
classifier (the topic table's "> 300 CPM" is illustrative; the live value is the user's danger
threshold, default 1000). The alert operator's own knob is only *how many* DANGER readings
within a window count as sustained.

**"Sustained" — ≥ `FLINK_ALERT_MIN_BREACHES` DANGER readings in the window (default 3).** A
one-off DANGER spike is not an alert; sustained means repeated DANGER readings. The pure
reducer `operators/alert_detection.py::detect_sustained_high` counts breaches and, when the
threshold is met, emits one alert carrying the peak CPM, the breach count, the window bounds,
and the latest breaching reading's location (so the map can place the hotspot at its most
recent position).

**Windows — sliding event-time, default 300 s length / 60 s slide
(`operators/alert_window.py`).** Events are keyed by `sensor_id` and evaluated over
`SlidingEventTimeWindows` on the watermarked stream. *Sliding* (vs the aggregation's tumbling)
windows catch a burst promptly and re-evaluate every slide, so an alert fires within ~one slide
of a hotspot developing rather than waiting for a tumbling boundary. Length/slide are tunable
via `FLINK_ALERT_WINDOW_SECONDS` / `FLINK_ALERT_SLIDE_SECONDS`.

**Dedup — per-sensor event-time cooldown (`operators/alert_dedup.py`).** Overlapping sliding
windows re-fire the same burst (a 300 s/60 s window emits up to 5 candidates per burst), so a
keyed `AlertCooldownOperator` keeps the last *emitted* alert's `window_end` per sensor in
`ValueState` and suppresses any later candidate whose window ends within
`FLINK_ALERT_COOLDOWN_SECONDS` (default 600) of it. Cooldown is measured in **event time**
(the alerts' window bounds), not wall-clock, so it behaves identically on replay/backfill
regardless of producer speed. State carries a TTL (≥ 10× cooldown) so idle sensors' state
cannot grow unbounded — mirroring `SensorDedupOperator` (ADR-003).

**Output — `schemas/radiation_alert.json`, published `AT_LEAST_ONCE`** as a parallel branch
off the classified `clean_stream`; the `radiation.clean` and `radiation.aggregated` sinks are
unaffected. Field names mirror the schema so the backend parses output as-is.

---

## Consequences

- `radiation.alerts` carries one record per sensor per sustained hotspot per cooldown, ready
  for the map's alert toast/sidebar (M5, Week 6), instead of a stream of duplicate breaches.
- Trigger, breach count, window geometry, and cooldown are all tunable per environment via env
  vars without a code change.
- Like the geo aggregation, the alert windows re-assign watermarks on the dict stream before
  windowing (`build_dict_watermark_strategy`) to detach from the broadcast classifier's pinned
  watermark — see ADR-004's correction note. Late events beyond the 30 s bounded
  out-of-orderness are dropped, not re-alerted.
- The pure reducer and the cooldown helpers are unit-tested without Flink; sliding-window
  event-time membership over a shuffled `captured_at` stream is covered by
  `tests/integration/test_alert_window.py`.

---

## Alternatives considered

- **Independent fixed CPM threshold (e.g. > 300):** matches the topic table literally but adds
  a second threshold that drifts out of sync with the user-configured classifier thresholds.
  Reusing the `DANGER` tag keeps a single source of truth and makes alerts honour live config
  changes for free.
- **Tumbling windows:** simpler and reused from the aggregation, but a burst straddling a
  tumbling boundary could be split below the breach threshold on both sides and missed; sliding
  windows give continuous coverage. The overlap they introduce is handled by the cooldown dedup.
- **Wall-clock / processing-time cooldown:** simpler (a timer), but would behave differently at
  1× vs 100× producer speed and on backfill. Event-time cooldown keeps alert cadence tied to
  the data, consistent with the rest of the event-time pipeline.
- **Stateful CEP (`flink-cep` pattern API):** expressive for "N breaches then …" patterns, but
  heavier to run and test and unavailable in the no-PyFlink CI path. The keyed window + pure
  reducer split keeps the logic unit-testable without a Flink runtime.

---

## Follow-up (not in this change)

The multi-stream **alerts × `radiation.aggregated` join** for "hot region" labels (enrich each
alert with its geohash cell's stats) is scoped as a separate MR — a `KeyedCoProcessFunction`
keyed by geohash holding recent aggregated cell stats in state. Recorded here so the alert
schema can be extended additively (e.g. a `hot_region` block) without breaking consumers.
