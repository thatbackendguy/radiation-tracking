"""GET /metrics — Prometheus exposition for the backend service.

Counterpart to the data-provider's metrics endpoint (port 8001): scrape both to
watch the whole pipeline edge-to-edge. Counters are recorded where the work
happens (consumer handlers, broadcaster, config producer); the two gauges are
refreshed here at scrape time so they always reflect the live state without a
background sampler task.
"""

from __future__ import annotations

from fastapi import APIRouter, Response

from app.core.broadcaster import _broadcaster
from app.core.metrics import _metrics
from app.core.ring_buffer import _buffer

router = APIRouter()


@router.get(
    "/metrics",
    tags=["metrics"],
    summary="Prometheus metrics (text exposition)",
    response_class=Response,
    responses={200: {"content": {"text/plain": {}}, "description": "Prometheus text format."}},
)
async def metrics() -> Response:
    _metrics.set_ws_clients(_broadcaster.subscriber_count)
    _metrics.set_ring_buffer_size(len(_buffer))
    payload, content_type = _metrics.exposition()
    return Response(content=payload, media_type=content_type)
