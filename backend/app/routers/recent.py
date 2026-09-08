from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.core import ring_buffer
from app.core.settings import settings
from app.core.view_filter import ViewFilter
from app.models import RadiationEvent

router = APIRouter(prefix="/recent", tags=["recent"])


class RecentResponse(BaseModel):
    count: int
    events: List[RadiationEvent]
    next_cursor: Optional[int] = Field(
        default=None,
        description="Pass back as ?cursor to fetch the next older page; null on the last page.",
        examples=[412],
    )
    has_more: bool = Field(default=False, description="Whether older events are still buffered.")


@router.get(
    "",
    response_model=RecentResponse,
    summary="Newest buffered clean events (cursor-paginated, per-client filters)",
    description=(
        "Returns the newest `radiation.clean` events from the in-memory ring buffer "
        "for seeding the map. Paging walks backwards through the buffered window "
        "via the opaque `next_cursor`; events evicted from the buffer before a "
        "client pages back to them are gone (best-effort by design). `limit` is "
        "clamped, never rejected, so the map always gets a usable response. "
        "The optional bounding-box (`min_lat`/`max_lat`/`min_lon`/`max_lon`, "
        "all-or-none) and time-range (`start`/`end`, both-or-none) parameters are "
        "**per-client view filters** (ADR-017): they narrow only this response, "
        "never the pipeline or other clients."
    ),
    responses={
        422: {
            "description": (
                "Invalid view filter — the bounding box needs all four bounds with "
                "min < max, and a time range needs start < end."
            )
        },
    },
)
async def get_recent(
    limit: Optional[int] = Query(default=None, description="Max events to return"),
    cursor: Optional[int] = Query(
        default=None,
        description=(
            "Opaque cursor from a previous response's next_cursor. Returns events "
            "older than the cursor; omit it for the newest page. Paging is bounded "
            "to the in-memory window."
        ),
    ),
    min_lat: Optional[float] = Query(default=None, ge=-90.0, le=90.0),
    max_lat: Optional[float] = Query(default=None, ge=-90.0, le=90.0),
    min_lon: Optional[float] = Query(default=None, ge=-180.0, le=180.0),
    max_lon: Optional[float] = Query(default=None, ge=-180.0, le=180.0),
    start: Optional[datetime] = Query(
        default=None, description="ISO-8601 lower bound on captured_at (per-client)"
    ),
    end: Optional[datetime] = Query(
        default=None, description="ISO-8601 upper bound on captured_at (per-client)"
    ),
) -> RecentResponse:
    # Clamp instead of rejecting — the map should always get a usable response.
    if limit is None:
        limit = settings.recent_default_limit
    limit = max(1, min(limit, settings.recent_max_limit))

    try:
        view = ViewFilter(
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            start=start,
            end=end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    matches = None if view.is_noop else view.matches_event
    page = await ring_buffer._buffer.page(cursor, limit, matches=matches)
    return RecentResponse(
        count=len(page.events),
        events=page.events,
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )
