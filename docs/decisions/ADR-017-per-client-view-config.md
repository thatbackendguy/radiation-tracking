# ADR-017: Per-client view config (REMAP model) — view filters split from pipeline config

**Date:** 2026-07-09
**Status:** Accepted
**Author:** M3 (Yash)
**Affects:** `frontend/src/App.jsx`, `frontend/src/components/ControlPanel.jsx`,
`backend/app/core/view_filter.py`, `backend/app/core/broadcaster.py`,
`backend/app/core/ring_buffer.py`, `backend/app/routers/recent.py`,
`backend/app/routers/stream.py`
**Amends:** ADR-006 (the user-facing half), ADR-013 (clarifies what the token gates)

---

## Context

The frontend's filter form POSTed its `area`/`timespan` selection to `POST /config`,
which publishes to `config.updates` and lands in Flink broadcast state (ADR-006).
The result: **one visitor's view preference rewrote the global pipeline state for
every client** — drawing a rectangle over Japan dropped everyone else's events
outside Japan, and the data was gone from the stream, not merely hidden.

The EU JRC REMAP map (<https://remap.jrc.ec.europa.eu/Advanced.aspx>), the
reference UX for this project, treats every filter (time range, station/layer
selection, colour scale) as a **per-session view parameter** on the read path:
the server always computes the full dataset, each browser narrows only what it
requests and renders, and views are shareable via URL. Operator configuration is
a separate, privileged concern.

---

## Decision

Split the old config payload by nature:

| Concern | Nature | Where it now lives |
|---|---|---|
| `area`, `timespan` | Per-user view preference | Client state + read-path filters |
| `cpm_warn_threshold`, `cpm_danger_threshold` | System alerting semantics | `POST /config` (admin, token-gated, ADR-013) |

**Frontend (view state):** filters live in browser state, persisted to
`localStorage` and mirrored into URL query params (shareable views). A local
"colour scale" can recolour markers by CPM without touching the pipeline's
classification. Only the clearly-labelled admin section still writes to
`POST /config`, and its payload is thresholds-only.

**Backend (read path):** filtering happens server-side, per client — H9-friendly
(the frontend selects what to display; the backend does the processing):

* `GET /recent` accepts optional `min_lat`/`max_lat`/`min_lon`/`max_lon`
  (all-or-none) and `start`/`end` (both-or-none) query params. The ring buffer
  filters before windowing, so cursor pagination stays consistent under filters.
* `/ws/stream` accepts `{"type": "subscribe", "area": {...}, "timespan": {...}}`
  at any time. The filter is installed on that connection's `Subscriber` and
  applied at `offer()` time, so filtered events never occupy buffer space or
  cross the wire. Nulls widen the view back; malformed messages are logged and
  ignored (previous filter kept). The frontend re-sends its subscribe on every
  (re)connect and on filter change.
* Matching is **fail-open**: an event missing the fields a rule needs is
  delivered rather than hidden — fail-closed could silently swallow alerts on a
  payload-shape drift. The client applies the same filters locally (it must
  anyway: the catch-up snapshot is sent before any subscribe can arrive), so
  fail-open never shows wrong data.

**Pipeline (unchanged wire contract):** `config.updates` and
`operators/region_filter.py` (ADR-006) keep accepting optional `area`/`timespan`
— the schema is untouched and backward compatible. Their meaning narrows to an
**operator-set ingest bound** (e.g. constrain the whole deployment to a demo
region), no longer driven by end-user interaction. With the frontend no longer
sending them, the pipeline default is "process everything". Removing the fields
outright is deliberately out of scope (M2's module; needs schema + operator
changes together).

---

## Consequences

* Clients can no longer clobber each other's maps; the pipeline processes the
  full stream, so a late-joining client isn't missing data someone else filtered
  away upstream.
* The ADR-013 write token now guards something genuinely privileged (global
  alerting thresholds) instead of gating a normal user interaction.
* View state is bookmarkable/shareable via URL, matching REMAP.
* Per-subscriber filtering costs one bounding-box + timestamp check per event
  per socket — negligible at our event rates, and it *reduces* send bandwidth.
* The unfiltered catch-up snapshot slightly over-delivers on connect; the client
  filters it locally. Filtering the snapshot server-side would require the
  subscribe to arrive before the snapshot, a protocol change not worth the race
  handling.
