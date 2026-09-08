"""Postgres schema + row mapping for the aggregated sink (ADR-018).

The pure helpers (``record_to_row``, ``build_rows``) carry no DB dependency so
they unit-test without a database. The natural key is ``(geohash, window_start)``
— exactly one aggregated record exists per geohash cell per tumbling window, so
Flink's AT_LEAST_ONCE re-delivery and late window re-fires collapse onto the same
row via ``ON CONFLICT ... DO UPDATE`` (latest-wins, idempotent).
"""

from __future__ import annotations

import logging
import time
from typing import Iterable

logger = logging.getLogger(__name__)

TABLE = "aggregated_blobs"

# Insert column order — record_to_row() yields values in exactly this order.
COLUMNS = (
    "geohash",
    "window_start",
    "window_end",
    "precision",
    "count",
    "cpm_avg",
    "cpm_max",
    "cpm_min",
    "safe_count",
    "warn_count",
    "danger_count",
    "worst_classification",
    "centroid_latitude",
    "centroid_longitude",
    "rolling_cpm_avg",
    "cpm_zscore",
    "anomaly",
)

_KEY_COLUMNS = ("geohash", "window_start")

SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    geohash              TEXT             NOT NULL,
    window_start         TIMESTAMPTZ      NOT NULL,
    window_end           TIMESTAMPTZ,
    precision            SMALLINT,
    count                INTEGER          NOT NULL,
    cpm_avg              DOUBLE PRECISION,
    cpm_max              DOUBLE PRECISION,
    cpm_min              DOUBLE PRECISION,
    safe_count           INTEGER,
    warn_count           INTEGER,
    danger_count         INTEGER,
    worst_classification TEXT,
    centroid_latitude    DOUBLE PRECISION,
    centroid_longitude   DOUBLE PRECISION,
    rolling_cpm_avg      DOUBLE PRECISION,
    cpm_zscore           DOUBLE PRECISION,
    anomaly              BOOLEAN,
    ingested_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (geohash, window_start)
);
-- Time-range scans (e.g. history for a region over a window) are the main query.
CREATE INDEX IF NOT EXISTS idx_{TABLE}_window_start ON {TABLE} (window_start);
CREATE INDEX IF NOT EXISTS idx_{TABLE}_anomaly ON {TABLE} (anomaly) WHERE anomaly;
"""

# INSERT ... VALUES %s  (execute_values fills the tuples), upserting on the key.
_UPDATE_COLUMNS = [c for c in COLUMNS if c not in _KEY_COLUMNS]
_SET_CLAUSE = ", ".join(f"{c} = EXCLUDED.{c}" for c in _UPDATE_COLUMNS)
UPSERT_SQL = (
    f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES %s "
    f"ON CONFLICT ({', '.join(_KEY_COLUMNS)}) "
    f"DO UPDATE SET {_SET_CLAUSE}, ingested_at = now()"
)


def record_to_row(record: dict) -> tuple:
    """Map one radiation.aggregated JSON object to a column-ordered value tuple.

    Raises ValueError if a required key (geohash / window_start / count) is
    missing, so the caller can skip a malformed record without aborting the batch.
    """
    if not isinstance(record, dict):
        raise ValueError("aggregated record is not an object")
    geohash = record.get("geohash")
    window_start = record.get("window_start")
    count = record.get("count")
    if not geohash or not window_start or count is None:
        raise ValueError("aggregated record missing geohash/window_start/count")

    class_counts = record.get("class_counts") or {}
    if not isinstance(class_counts, dict):
        class_counts = {}

    return (
        geohash,
        window_start,
        record.get("window_end"),
        record.get("precision"),
        count,
        record.get("cpm_avg"),
        record.get("cpm_max"),
        record.get("cpm_min"),
        class_counts.get("SAFE"),
        class_counts.get("WARN"),
        class_counts.get("DANGER"),
        record.get("worst_classification"),
        record.get("centroid_latitude"),
        record.get("centroid_longitude"),
        record.get("rolling_cpm_avg"),
        record.get("cpm_zscore"),
        record.get("anomaly"),
    )


def build_rows(records: Iterable[dict]) -> list[tuple]:
    """Map a batch of records to rows, skipping (and logging) malformed ones."""
    rows: list[tuple] = []
    for record in records:
        try:
            rows.append(record_to_row(record))
        except ValueError as exc:
            logger.warning("Skipping malformed aggregated record: %s", exc)
    return rows


def connect_with_retry(dsn: str, retries: int, backoff_s: float):
    """Connect to Postgres, retrying while the container comes up. Returns a conn."""
    import psycopg2

    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(dsn)
            conn.autocommit = False
            logger.info("Connected to Postgres (attempt %d).", attempt)
            return conn
        except psycopg2.OperationalError as exc:  # DB not ready yet
            last_exc = exc
            logger.info("Postgres not ready (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(backoff_s)
    raise RuntimeError(f"Could not connect to Postgres after {retries} attempts") from last_exc


def ensure_schema(conn) -> None:
    """Create the table + indexes if they do not exist (idempotent)."""
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def upsert_batch(conn, rows: list[tuple]) -> int:
    """Idempotently upsert a batch of rows; returns the number of rows sent."""
    if not rows:
        return 0
    from psycopg2.extras import execute_values

    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows)
    conn.commit()
    return len(rows)
