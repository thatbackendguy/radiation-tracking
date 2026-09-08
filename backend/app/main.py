from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.broadcaster import _broadcaster
from app.core.history_db import _history
from app.core.kafka_admin import ensure_topics
from app.core.kafka_consumer import start_consumer, stop_consumer
from app.core.kafka_producer import _producer
from app.core.ring_buffer import _buffer
from app.core.settings import settings
from app.routers import config, health, history, metrics, recent, stream


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await ensure_topics(settings)
    await _producer.start(settings)
    consumer_handle = await start_consumer(settings, _buffer, _broadcaster)
    # Read-only pool over the aggregated archive (ADR-019); degraded-safe.
    await _history.connect(settings)
    yield
    await _history.close()
    await stop_consumer(consumer_handle)
    await _producer.stop()


# WebSocket endpoints live outside the OpenAPI spec, so the /ws/stream contract
# is documented here where every spec consumer will see it.
_DESCRIPTION = """\
Backend service for the TUHH Big Data radiation tracking project (Topic C).

Consumes the processed Kafka streams (`radiation.clean`, `radiation.alerts`,
`radiation.aggregated`), serves them to the map frontend, and publishes the
global pipeline thresholds back to Flink via the `config.updates` topic.
Area/timespan filtering is **per client** on the read path (ADR-017): `/recent`
query params and the `/ws/stream` subscribe message below — one client's
filters never affect another client or the pipeline.

## WebSocket: `/ws/stream`

Live push of processed events (not part of the OpenAPI paths below — WebSockets
are outside the spec):

* **Origin policy** — auth-less but origin-checked: a browser `Origin` header
  must be in the `CORS_ORIGINS` allowlist (`*` allows all; a missing header —
  curl, tests — is allowed). Disallowed origins are closed with code `1008`
  before the upgrade.
* **On connect** the client receives a catch-up snapshot of recent
  `radiation.clean` events, then the live stream.
* **Message envelope** — every frame is
  `{"type": "clean" | "alert" | "aggregated", "data": {...}}`; for `clean`,
  `data` is a `RadiationEvent`. One socket multiplexes all three streams.
* **Per-client view filter (ADR-017)** — the client may send
  `{"type": "subscribe", "area": {"min_lat", "max_lat", "min_lon", "max_lon"},
  "timespan": {"start", "end"}}` at any time; both parts optional, nulls widen
  the view back. Only this connection's live stream is narrowed (the catch-up
  snapshot is unfiltered — clients filter it locally). Malformed subscribe
  frames are ignored and the previous filter kept.
* **Backpressure** — per client, `clean`/`aggregated` state coalesces
  latest-wins per key; `alert` frames are FIFO and never coalesced, and each
  flush delivers alerts first.
"""

_OPENAPI_TAGS = [
    {"name": "health", "description": "Liveness + per-component status (always HTTP 200)."},
    {"name": "metrics", "description": "Prometheus text exposition (namespace `backend`)."},
    {
        "name": "config",
        "description": (
            "Global pipeline thresholds (admin, token-gated); POST publishes to "
            "`config.updates` for Flink. Per-client area/timespan view filters "
            "live on the read path instead (ADR-017)."
        ),
    },
    {
        "name": "recent",
        "description": (
            "Recent clean events for map seeding (cursor-paginated, with optional "
            "per-client area/timespan filters)."
        ),
    },
    {
        "name": "history",
        "description": (
            "Read-only insights over the aggregated-blob archive in Database "
            "Summary, per-window CPM time-series, and hotspots. "
            "503 when the archive is unavailable."
        ),
    },
]

app = FastAPI(
    title="Radiation Tracking Backend",
    description=_DESCRIPTION,
    version="1.0.0",
    openapi_tags=_OPENAPI_TAGS,
    lifespan=lifespan,
)

_origins = settings.allowed_origins()
# The CORS spec forbids credentials:true with a wildcard origin — browsers reject it.
_allow_credentials = "*" not in _origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(metrics.router)
app.include_router(config.router)
app.include_router(recent.router)
app.include_router(history.router)
app.include_router(stream.router)
