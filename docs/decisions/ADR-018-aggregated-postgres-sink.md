# ADR-018: Postgres sink for aggregated data (queryable historical archive)

**Date:** 2026-07-10
**Status:** Accepted
**Author:** M3 (Yash)
**Affects:** `aggregated_sink/` (new service), `docker-compose.yml`,
`docker-compose.prod.yml`, `.env.example`

---

## Context

Until now the project persisted **nothing** queryable: `radiation.aggregated`
blobs flow Kafka → backend → WebSocket and live only in bounded in-memory
buffers (backend ring buffer of 500 clean events; frontend caps). Kafka retains
the topic for a capped window (1 GiB / 24 h) but is a log, not an analytical
store. So "what were the aggregated readings for region X last Tuesday?" was
unanswerable — the data is gone once it scrolls out of the live window.

We want a **queryable historical archive of the aggregated blobs** without
disturbing the real-time path.

## Decision

Add a **dedicated consumer service** (`aggregated_sink/`, same standalone-Python
idiom as `data-provider`) that reads `radiation.aggregated` and upserts each blob
into **Postgres**.

**Why a separate service** (over the two alternatives considered):
- **vs. a Flink JDBC sink in the job** — PyFlink would need the Postgres JDBC jar
  + Table API wiring on the `amd64`-pinned, `pemja`-sensitive flink image; highest
  complexity/risk and it touches M2's job graph. A downstream consumer keeps the
  Flink image untouched.
- **vs. writing from the backend consumer** — would couple durable persistence to
  the latency-sensitive WebSocket/API service and add a DB failure mode on the
  live path. The sink is isolated: if Postgres is down, only the archive lags;
  the map keeps streaming.

**Schema & idempotency.** One table `aggregated_blobs` keyed on
`(geohash, window_start)` — exactly one aggregated record exists per geohash cell
per tumbling window, so Flink's **AT_LEAST_ONCE** re-delivery and late
window re-fires collapse onto the same row via `INSERT … ON CONFLICT … DO UPDATE`
(latest-wins). Columns mirror `schemas/radiation_aggregated.json` (counts, cpm
avg/max/min, per-class counts, centroid, rolling_cpm_avg, cpm_zscore, anomaly),
plus `ingested_at`. Indexes on `window_start` (time-range scans) and a partial
index on `anomaly` (spike/dip queries).

**Delivery contract.** `enable_auto_commit=false`; the Kafka offset commit happens
only **after** the Postgres batch commits, so a crash in between replays the batch
onto the same idempotent rows — effectively-once for the table. Upserts are
batched (`AGG_SINK_BATCH_SIZE`, default 200) to bound DB round-trips under the
100× replay.

**Cross-platform.** `postgres:16-alpine` is multi-arch (runs natively on arm64
macs and the amd64 droplet — no platform pin). `psycopg2-binary` ships prebuilt
wheels for macOS/Windows/linux-amd64, so the build stays portable (H10).

## Consequences

- The project gains a durable, SQL-queryable archive of the aggregated data,
  independent of the live buffers and Kafka retention.
- New stateful service + volume (`pgdata`) on the droplet: ~+384 MB (Postgres) +
  ~192 MB (sink) against the 8 GB budget — added to the OOM-watch risk row; resize
  to 16 GB is the fallback if the soak proves tight.
- The host-published Postgres port defaults to **5433** (container is 5432) to
  avoid clashing with a developer's local Postgres; loopback-only on the droplet.
- **Out of scope (follow-ups):** a backend read API over the archive (e.g.
  `GET /history?geohash=…&from=…&to=…`), a retention/rollup policy for the table,
  and sinks for `radiation.clean` / `radiation.alerts` if raw/alert history is
  later wanted.

## Verification

`docker compose up --build` brings up `postgres` + `aggregated-sink`; once the
pipeline produces blobs, rows appear:

```sql
-- psql -h localhost -p 5433 -U radiation -d radiation
SELECT count(*) FROM aggregated_blobs;
SELECT geohash, window_start, count, cpm_avg, anomaly
FROM aggregated_blobs ORDER BY window_start DESC LIMIT 5;
```

Re-consuming the same offsets (restart the sink) leaves the row count unchanged —
confirming the upsert is idempotent.
