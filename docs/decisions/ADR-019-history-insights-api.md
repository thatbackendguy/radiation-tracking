# ADR-019: /history read API + frontend Insights over the aggregated archive

**Date:** 2026-07-10
**Status:** Accepted
**Author:** M3 (Yash)
**Affects:** `backend/app/core/history_db.py`, `backend/app/routers/history.py`,
`backend/app/core/settings.py`, `backend/app/main.py`, `backend/requirements.txt`,
`docker-compose.yml`, `frontend/src/components/insights/*`
**Builds on:** ADR-018 (the aggregated Postgres sink — the *write* half)

---

## Context

ADR-018 added the aggregated-blob archive in Postgres, but nothing read it — it was
a write-only sink with no consumer, so it gave the frontend no value (the map still
showed only the transient live window). To turn the archive into **meaningful
insight**, the backend needs a read path and the frontend needs visualizations that
answer questions the live stream can't: *how did CPM trend over time? where was it
worst? when did anomalies fire?*

## Decision

**Backend — a read-only `/history` API** over the archive:
- `GET /history/summary` — archive-wide totals (rows, distinct cells, anomalies,
  first/last window, peak CPM).
- `GET /history/timeseries` — per-window aggregate for the trend chart:
  **count-weighted** mean CPM (`sum(cpm_avg*count)/sum(count)`), peak CPM, danger
  count, and anomaly-cell count, grouped by `window_start`.
- `GET /history/hotspots` — top geohash cells by peak CPM (with centroid, so the
  frontend can zoom the map to a cell).

All three accept the **same per-client bounding-box + time-range filters as
/recent** (ADR-017), so insights follow what the user is looking at. Access is via
**asyncpg** (async pool created in the lifespan). It is **degraded-safe**: if the
pool can't be created (Postgres down, or `history_enabled=false`) the rest of the
backend is unaffected and `/history` returns **503** — the same "never restart-loop
the stack" contract as the Kafka components. Query building (`_where`, `BBox`) is
pure and unit-tested; the SQL is exercised end-to-end against the Dockerised
Postgres.

**Frontend — an "Insights" control-panel section** (`components/insights/`):
- **Summary stat tiles** (hero numbers, not a chart) — windows archived, cells,
  anomalies, peak CPM, archived span.
- **CPM-over-time trend chart** — a max-CPM envelope behind the count-weighted
  avg-CPM line, anomaly windows marked. One measure, one y-axis (no dual-axis);
  legend + direct labels so identity is never colour-alone (dataviz rules).
- **Hotspots bar ranking** — top cells by peak CPM, bars coloured by the
  safe/warn/danger band (the map's status semantics, a real second signal), each a
  button that flies the map to the cell's centroid.
- Inline SVG/HTML, no chart dependency (matches the hand-built timeline); reuses the
  app's existing design tokens; polls every 15 s and refetches when the view
  changes; renders explicit degraded / empty / loading states.

## Consequences

- The archive now delivers UI value: the project answers historical/trend questions,
  not just "what's live right now" — closing the "data stays live then it's gone" gap.
- New backend dependency (`asyncpg`, prebuilt wheels → cross-platform) and a Postgres
  read pool; both optional/degraded-safe so local dev and the droplet are unaffected
  if the archive is absent.
- **Out of scope (follow-ups):** temporal *playback* (animating the map through
  archived windows — the read path now exists to support it), server-side downsampling
  for very long ranges (the `history_max_rows` cap guards it for now), and a table/CSV
  export of the archive.

## Verification

Via Docker (down → up), with the sink populating the archive:
```bash
curl "http://localhost/history/summary"
curl "http://localhost/history/timeseries?start=…&end=…" | jq length
curl "http://localhost/history/hotspots?limit=8" | jq '.[0]'
```
The Insights panel renders the stat tiles, trend chart, and hotspot bars from live
archived data; clicking a hotspot flies the map to it. With Postgres stopped, the
panel shows "archive unavailable" and the rest of the app keeps working (503).
