"""/history — read-only insight queries over the aggregated-blob archive (ADR-019).

Powers the frontend Insights panel: a summary, a per-window CPM time-series, and a
hotspots ranking. All three accept an optional per-client bounding box + time range
(same view filters as /recent, ADR-017) so the insights follow what the user is
looking at. Returns 503 when the archive is unavailable (degraded-safe).
"""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.core.history_db import ArchiveQueryError, BBox, _history
from app.core.settings import settings

router = APIRouter(prefix="/history", tags=["history"])


class SummaryResponse(BaseModel):
    rows: int = 0
    cells: int = 0
    anomalies: int = 0
    first_window: Optional[datetime] = None
    last_window: Optional[datetime] = None
    peak_cpm: Optional[float] = None


class TimeseriesPoint(BaseModel):
    window_start: datetime
    total_count: int
    avg_cpm: Optional[float] = None
    max_cpm: Optional[float] = None
    danger: int = 0
    anomaly_cells: int = 0


class Hotspot(BaseModel):
    geohash: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    peak_cpm: Optional[float] = None
    avg_cpm: Optional[float] = None
    total_count: int = 0
    windows: int = 0
    anomaly_windows: int = 0


def _unavailable() -> HTTPException:
    return HTTPException(
        status_code=503,
        detail="aggregated archive unavailable (Postgres down or history disabled)",
    )


def _require_archive() -> None:
    if not _history.available:
        raise _unavailable()


def _bbox(min_lat, max_lat, min_lon, max_lon) -> BBox:
    # Validated before _require_archive() at every call site: a malformed bbox is
    # always a 422, whether or not the archive happens to be up right now — an
    # outage shouldn't mask a client-side input bug as "service unavailable".
    try:
        return BBox(min_lat, max_lat, min_lon, max_lon)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/summary", response_model=SummaryResponse, summary="Archive-wide totals")
async def get_summary() -> SummaryResponse:
    _require_archive()
    try:
        return SummaryResponse(**(await _history.summary()))
    except ArchiveQueryError as exc:
        raise _unavailable() from exc


# NOTE: each parameter needs its OWN Query() instance — a shared Query object
# reused across params makes FastAPI mis-bind them (min/max swap → false 422s).
@router.get(
    "/timeseries",
    response_model=List[TimeseriesPoint],
    summary="Per-window CPM time-series (count-weighted) for the trend chart",
)
async def get_timeseries(
    min_lat: Optional[float] = Query(default=None, ge=-90.0, le=90.0),
    max_lat: Optional[float] = Query(default=None, ge=-90.0, le=90.0),
    min_lon: Optional[float] = Query(default=None, ge=-180.0, le=180.0),
    max_lon: Optional[float] = Query(default=None, ge=-180.0, le=180.0),
    start: Optional[datetime] = Query(default=None),
    end: Optional[datetime] = Query(default=None),
) -> List[TimeseriesPoint]:
    bbox = _bbox(min_lat, max_lat, min_lon, max_lon)
    _require_archive()
    try:
        rows = await _history.timeseries(bbox, start, end, settings.history_max_rows)
    except ArchiveQueryError as exc:
        raise _unavailable() from exc
    return [TimeseriesPoint(**r) for r in rows]


@router.get(
    "/hotspots",
    response_model=List[Hotspot],
    summary="Top geohash cells by peak CPM over the range",
)
async def get_hotspots(
    min_lat: Optional[float] = Query(default=None, ge=-90.0, le=90.0),
    max_lat: Optional[float] = Query(default=None, ge=-90.0, le=90.0),
    min_lon: Optional[float] = Query(default=None, ge=-180.0, le=180.0),
    max_lon: Optional[float] = Query(default=None, ge=-180.0, le=180.0),
    start: Optional[datetime] = Query(default=None),
    end: Optional[datetime] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
) -> List[Hotspot]:
    bbox = _bbox(min_lat, max_lat, min_lon, max_lon)
    _require_archive()
    try:
        rows = await _history.hotspots(bbox, start, end, limit)
    except ArchiveQueryError as exc:
        raise _unavailable() from exc
    return [Hotspot(**r) for r in rows]
