# ADR-006: area + timespan filter as a broadcast operator on the map-facing branches

Date: 2026-06-26
Status: Accepted
Authors: M2 (Sahil)

---

## Context

The Week-5 deliverable (approach.md) is: "User can filter to a region + timespan; map shows
blobs and alerts only inside that selection." `schemas/config_update.json` already locks the
contract — an optional `area` (lat/lon bounding box) and `timespan` (ISO event-time window),
documented as "Flink operators discard events outside this box/window" — and the Backend (M4)
publishes it on `config.updates`. Nothing in the Flink job consumed it yet; the classifier
reads the same topic but only for thresholds.

H9 forbids any data processing in the frontend, so the discard must happen in Flink (or the
backend), not the map client.

---

## Decision

**A broadcast operator (`operators/region_filter.py`).** The filter reads `area`/`timespan`
from the `config.updates` broadcast stream into broadcast state and drops events outside the
box/window. This reuses the classifier's pattern (`BroadcastProcessFunction` + a
`MapStateDescriptor`, malformed-message guard that keeps the last-good filter) so a user
re-drawing the region retunes the live stream with no shuffle. The same `config.updates`
stream is broadcast to both the classifier and this filter.

**Applied to the map-facing branches only.** `apply_region_filter` runs on the classified
`clean_stream`; its output feeds the geo aggregation **and** the alert branch, so
`radiation.aggregated` and `radiation.alerts` only cover the selection. The `radiation.clean`
sink stays on the unfiltered stream, so the Backend's full feed / `/recent` is unaffected —
the filter is a *view* concern for the map, not a global drop. With no `area`/`timespan` set
the operator passes everything through (filtering is opt-in).

**Pure predicates, event-time timespan.** `in_area` / `in_timespan` / `event_passes` are pure
(no PyFlink) and unit-tested directly. `in_timespan` compares in epoch millis via the same
`captured_at` parsing the watermarks use (`model._parse_timestamp` + `timestamps.to_epoch_millis`),
start-inclusive / end-exclusive, so the window lines up with event time regardless of the
worker clock.

---

## Consequences

- A user-drawn region + timespan narrows the blobs and alerts on the map within one config
  message, with the Backend keeping the complete `radiation.clean` feed.
- The filter sits on a broadcast `connect()`, so its output watermark is pinned by the
  `no_watermarks()` config side — but the geo-aggregation and alert branches already re-assign
  watermarks before windowing (ADR-004 / ADR-005), so this changes nothing downstream.
- The timespan filter is correct by `captured_at` even though the windowing currently buckets
  by ingestion time (separate, tracked event-time issue) — the filter reads `captured_at`
  directly, not the window timestamp.

## Alternatives considered

- **Filter `radiation.clean` globally** (the literal "discard everywhere" reading): rejected —
  it would hide events from the Backend's `/recent` and any non-map consumer; the deliverable
  is specifically about the map's blobs/alerts.
- **Filter in the Backend** (M4) instead of Flink: viable, but the contract already says "Flink
  operators discard"; doing it in Flink keeps the aggregation/alert volume down at the source.
- **Fold area/timespan into the classifier operator**: rejected — overloads a single operator
  with two unrelated concerns (classification vs view filtering); a dedicated operator keeps
  each testable and lets the filter sit precisely where the map branches fork.
