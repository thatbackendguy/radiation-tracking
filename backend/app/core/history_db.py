"""Read-only access to the aggregated-blob archive in Postgres (ADR-019).

The archive is written by the standalone ``aggregated_sink`` service (ADR-018);
here the backend only *reads* it to power the /history insight endpoints. It is
**degraded-safe**: if the pool can't be created (Postgres down, or
``history_enabled`` off) the rest of the backend is unaffected and the /history
routes return 503 — the same "never restart-loop the stack" philosophy as the
Kafka components.

Query builders are pure (``_where``) and unit-tested without a database; the
actual queries are exercised end-to-end against the Dockerised Postgres.
"""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


class ArchiveQueryError(Exception):
    """The pool exists but a query against it failed (Postgres died mid-session).

    Distinct from "pool never created" (``HistoryDB.available`` is False): this
    covers the pool-exists-but-unreachable case so the router can still answer
    with the documented 503 instead of leaking a raw 500.
    """


class BBox:
    """An optional lat/lon bounding box; all-or-none of the four bounds."""

    __slots__ = ("min_lat", "max_lat", "min_lon", "max_lon")

    def __init__(self, min_lat, max_lat, min_lon, max_lon):
        vals = (min_lat, max_lat, min_lon, max_lon)
        if any(v is not None for v in vals) and not all(v is not None for v in vals):
            raise ValueError("bounding box needs all of min_lat, max_lat, min_lon, max_lon")
        if min_lat is not None:
            if not (min_lat < max_lat):
                raise ValueError("min_lat must be < max_lat")
            if not (min_lon < max_lon):
                raise ValueError("min_lon must be < max_lon")
        self.min_lat, self.max_lat, self.min_lon, self.max_lon = vals

    @property
    def is_set(self) -> bool:
        return self.min_lat is not None


def _where(bbox: BBox, start, end, params: list) -> str:
    """Build a WHERE clause over the current params list (asyncpg $-positional).

    Appends bound values to ``params`` and returns the SQL (or '' for no filter).
    Centroid-null rows fall out of the bbox predicate naturally.
    """
    clauses = []
    if bbox.is_set:
        params.extend([bbox.min_lat, bbox.max_lat, bbox.min_lon, bbox.max_lon])
        n = len(params)
        clauses.append(
            f"centroid_latitude BETWEEN ${n - 3} AND ${n - 2} "
            f"AND centroid_longitude BETWEEN ${n - 1} AND ${n}"
        )
    if start is not None:
        params.append(start)
        clauses.append(f"window_start >= ${len(params)}")
    if end is not None:
        params.append(end)
        clauses.append(f"window_start <= ${len(params)}")
    return (" WHERE " + " AND ".join(clauses)) if clauses else ""


class HistoryDB:
    """Module-singleton asyncpg pool + read queries over aggregated_blobs."""

    def __init__(self) -> None:
        self._pool = None

    @property
    def available(self) -> bool:
        return self._pool is not None

    async def connect(self, settings) -> None:
        """Best-effort pool creation; leaves the pool None (degraded) on failure.

        Retries a bounded number of times: docker-compose deliberately starts the
        backend as soon as Postgres's *container* starts (not once it's healthy),
        so a cold `docker compose up` can race the pool-creation attempt against
        Postgres still initializing. Retrying here (like aggregated_sink's own
        connect_with_retry) turns that race into a short startup delay instead of
        a permanently degraded archive that only a manual restart fixes.
        """
        if not settings.history_enabled:
            logger.info("History archive disabled (history_enabled=false).")
            return

        import asyncpg

        attempts = max(1, settings.history_connect_retry_attempts)
        for attempt in range(1, attempts + 1):
            try:
                self._pool = await asyncpg.create_pool(
                    dsn=settings.postgres_dsn(),
                    min_size=settings.history_pool_min_size,
                    max_size=settings.history_pool_max_size,
                    timeout=settings.history_connect_timeout_seconds,
                    command_timeout=settings.history_connect_timeout_seconds,
                )
                logger.info("Connected to the aggregated-blob archive (Postgres).")
                return
            except Exception as exc:  # pragma: no cover - exercised via docker
                self._pool = None
                if attempt < attempts:
                    logger.info(
                        "History archive not ready (attempt %d/%d): %s", attempt, attempts, exc
                    )
                    await asyncio.sleep(settings.history_connect_retry_backoff_seconds)
                else:
                    logger.warning(
                        "History archive unavailable (degraded) after %d attempts: %s",
                        attempts,
                        exc,
                    )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def _fetch(self, sql: str, params: list) -> list[dict]:
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
        except Exception as exc:
            logger.warning("History archive query failed (degraded): %s", exc)
            raise ArchiveQueryError(str(exc)) from exc
        return [dict(r) for r in rows]

    async def summary(self) -> dict:
        sql = """
            SELECT count(*)::bigint                              AS rows,
                   count(DISTINCT geohash)::bigint               AS cells,
                   count(*) FILTER (WHERE anomaly)::bigint       AS anomalies,
                   min(window_start)                             AS first_window,
                   max(window_start)                             AS last_window,
                   max(cpm_max)                                  AS peak_cpm
            FROM aggregated_blobs
        """
        rows = await self._fetch(sql, [])
        return rows[0] if rows else {}

    async def timeseries(self, bbox: BBox, start, end, limit: int) -> list[dict]:
        """Per-window aggregate (count-weighted mean CPM, peak, danger, anomalies)."""
        params: list = []
        where = _where(bbox, start, end, params)
        params.append(limit)
        # Windowed DESC + LIMIT first so a capped result keeps the *most recent*
        # windows (the trend chart's whole point); re-sorted ASC for the chart.
        # danger_count is nullable (no DANGER-classified rows in a quiet window),
        # so sum() over an all-NULL group would otherwise surface a bare NULL.
        sql = f"""
            SELECT * FROM (
                SELECT window_start,
                       sum(count)::bigint                               AS total_count,
                       sum(cpm_avg * count) / NULLIF(sum(count), 0)     AS avg_cpm,
                       max(cpm_max)                                     AS max_cpm,
                       COALESCE(sum(danger_count), 0)::bigint           AS danger,
                       count(*) FILTER (WHERE anomaly)::bigint          AS anomaly_cells
                FROM aggregated_blobs{where}
                GROUP BY window_start
                ORDER BY window_start DESC
                LIMIT ${len(params)}
            ) recent_windows
            ORDER BY window_start
        """
        return await self._fetch(sql, params)

    async def hotspots(self, bbox: BBox, start, end, limit: int) -> list[dict]:
        """Top geohash cells by peak CPM over the range (with centroid for the map)."""
        params: list = []
        where = _where(bbox, start, end, params)
        params.append(limit)
        sql = f"""
            SELECT geohash,
                   avg(centroid_latitude)                          AS latitude,
                   avg(centroid_longitude)                         AS longitude,
                   max(cpm_max)                                    AS peak_cpm,
                   sum(cpm_avg * count) / NULLIF(sum(count), 0)    AS avg_cpm,
                   sum(count)::bigint                              AS total_count,
                   count(*)::bigint                                AS windows,
                   count(*) FILTER (WHERE anomaly)::bigint         AS anomaly_windows
            FROM aggregated_blobs{where}
            GROUP BY geohash
            ORDER BY peak_cpm DESC NULLS LAST
            LIMIT ${len(params)}
        """
        return await self._fetch(sql, params)


# Module-level singleton — same pattern as _buffer / _broadcaster.
_history = HistoryDB()
