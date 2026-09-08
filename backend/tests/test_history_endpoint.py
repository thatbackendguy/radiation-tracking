"""Tests for the /history read layer (ADR-019).

The pure query-builder + BBox validation are unit-tested here; the actual SQL is
exercised end-to-end against the Dockerised Postgres. The endpoints are tested in
their degraded (no-pool) state, which is the default in the hermetic test app.
"""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.core.history_db import ArchiveQueryError, BBox, HistoryDB, _where


class TestBBox:
    def test_all_none_is_unset(self):
        assert BBox(None, None, None, None).is_set is False

    def test_full_box_is_set(self):
        assert BBox(30.0, 40.0, 130.0, 145.0).is_set is True

    def test_partial_box_rejected(self):
        with pytest.raises(ValueError, match="all of"):
            BBox(30.0, None, 130.0, 145.0)

    def test_inverted_lat_rejected(self):
        with pytest.raises(ValueError, match="min_lat"):
            BBox(40.0, 30.0, 130.0, 145.0)

    def test_inverted_lon_rejected(self):
        with pytest.raises(ValueError, match="min_lon"):
            BBox(30.0, 40.0, 145.0, 130.0)


class TestWhereBuilder:
    def test_no_filters_is_empty(self):
        params: list = []
        assert _where(BBox(None, None, None, None), None, None, params) == ""
        assert params == []

    def test_bbox_only(self):
        params: list = []
        sql = _where(BBox(30.0, 40.0, 130.0, 145.0), None, None, params)
        assert "centroid_latitude BETWEEN $1 AND $2" in sql
        assert "centroid_longitude BETWEEN $3 AND $4" in sql
        assert params == [30.0, 40.0, 130.0, 145.0]

    def test_time_range_positions_after_bbox(self):
        params: list = []
        start = datetime(2026, 6, 1, tzinfo=timezone.utc)
        end = datetime(2026, 7, 1, tzinfo=timezone.utc)
        sql = _where(BBox(30.0, 40.0, 130.0, 145.0), start, end, params)
        assert "window_start >= $5" in sql
        assert "window_start <= $6" in sql
        assert params == [30.0, 40.0, 130.0, 145.0, start, end]

    def test_time_only_positions_from_one(self):
        params: list = []
        start = datetime(2026, 6, 1, tzinfo=timezone.utc)
        sql = _where(BBox(None, None, None, None), start, None, params)
        assert sql == " WHERE window_start >= $1"
        assert params == [start]


class TestHistoryEndpointsDegraded:
    """With no archive pool (the hermetic default), /history returns 503."""

    def test_summary_503_when_unavailable(self, client):
        assert client.get("/history/summary").status_code == 503

    def test_timeseries_503_when_unavailable(self, client):
        assert client.get("/history/timeseries").status_code == 503

    def test_hotspots_503_when_unavailable(self, client):
        assert client.get("/history/hotspots").status_code == 503

    def test_partial_bbox_is_422_even_when_degraded(self, client):
        # bbox validation runs before the archive-availability check, so a bad
        # bbox is always a 422 — even with a degraded (no-pool) archive — instead
        # of being masked as a 503 "archive unavailable".
        resp = client.get("/history/timeseries", params={"min_lat": 30})
        assert resp.status_code == 422

    def test_hotspots_partial_bbox_is_422_even_when_degraded(self, client):
        resp = client.get("/history/hotspots", params={"min_lat": 30})
        assert resp.status_code == 422


class TestArchiveQueryFailure:
    """Pool exists (available=True) but a live query fails — must still be 503,
    not a raw 500, and must not be confused with the "never connected" case."""

    @pytest.fixture(autouse=True)
    def _pool_available(self, monkeypatch):
        import app.core.history_db as history_db_module

        monkeypatch.setattr(history_db_module._history, "_pool", object())
        yield
        monkeypatch.setattr(history_db_module._history, "_pool", None)

    def test_summary_query_failure_is_503(self, client, monkeypatch):
        import app.core.history_db as history_db_module

        monkeypatch.setattr(
            history_db_module._history, "summary", AsyncMock(side_effect=ArchiveQueryError("down"))
        )
        assert client.get("/history/summary").status_code == 503

    def test_timeseries_query_failure_is_503(self, client, monkeypatch):
        import app.core.history_db as history_db_module

        monkeypatch.setattr(
            history_db_module._history,
            "timeseries",
            AsyncMock(side_effect=ArchiveQueryError("down")),
        )
        assert client.get("/history/timeseries").status_code == 503

    def test_hotspots_query_failure_is_503(self, client, monkeypatch):
        import app.core.history_db as history_db_module

        monkeypatch.setattr(
            history_db_module._history,
            "hotspots",
            AsyncMock(side_effect=ArchiveQueryError("down")),
        )
        assert client.get("/history/hotspots").status_code == 503


class _FakeConn:
    def __init__(self, capture):
        self._capture = capture

    async def fetch(self, sql, *params):
        self._capture["sql"] = sql
        self._capture["params"] = params
        return self._capture.get("rows", [])


class _FakeAcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc_info):
        return False


class _FakePool:
    def __init__(self, capture):
        self._capture = capture

    def acquire(self):
        return _FakeAcquireCtx(_FakeConn(self._capture))


class TestTimeseriesQuery:
    """Wiring/SQL-shape checks without a real Postgres (mirrors the _where tests)."""

    def test_null_danger_is_coalesced_and_result_kept_recent(self):
        capture = {"rows": []}
        db = HistoryDB()
        db._pool = _FakePool(capture)

        asyncio.run(db.timeseries(BBox(None, None, None, None), None, None, 5000))

        sql = capture["sql"]
        # A window with no DANGER-classified rows has danger_count = NULL in every
        # row of its group; sum() over an all-NULL group is NULL, not 0 — COALESCE
        # is required so TimeseriesPoint's non-Optional `danger: int` doesn't 500.
        assert "COALESCE(sum(danger_count), 0)" in sql
        # DESC + LIMIT inside, re-sorted ASC outside: a capped result keeps the
        # *most recent* windows instead of the oldest ones once the archive grows
        # past `limit` distinct windows.
        assert "ORDER BY window_start DESC" in sql
        assert sql.strip().endswith("ORDER BY window_start")


class TestHistoryDBConnectRetry:
    """connect() must survive a cold-start race against Postgres, not just its
    permanent absence — docker-compose intentionally doesn't wait for Postgres's
    healthcheck before starting the backend."""

    def _settings(self, **overrides):
        base = dict(
            history_enabled=True,
            postgres_dsn=lambda: "postgresql://x/y",
            history_pool_min_size=1,
            history_pool_max_size=1,
            history_connect_timeout_seconds=1.0,
            history_connect_retry_attempts=3,
            history_connect_retry_backoff_seconds=0,
        )
        base.update(overrides)
        return SimpleNamespace(**base)

    def test_retries_then_succeeds(self, monkeypatch):
        calls = {"n": 0}

        async def fake_create_pool(**kwargs):
            calls["n"] += 1
            if calls["n"] < 2:
                raise ConnectionError("Postgres not ready yet")
            return object()

        monkeypatch.setattr("asyncpg.create_pool", fake_create_pool)

        db = HistoryDB()
        asyncio.run(db.connect(self._settings()))

        assert db.available is True
        assert calls["n"] == 2

    def test_gives_up_after_max_attempts_and_stays_degraded(self, monkeypatch):
        calls = {"n": 0}

        async def always_fails(**kwargs):
            calls["n"] += 1
            raise ConnectionError("Postgres never came up")

        monkeypatch.setattr("asyncpg.create_pool", always_fails)

        db = HistoryDB()
        asyncio.run(db.connect(self._settings(history_connect_retry_attempts=3)))

        assert db.available is False
        assert calls["n"] == 3
