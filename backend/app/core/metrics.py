"""Backend service metrics, exposed at GET /metrics.

Mirrors the data-provider's ProducerMetrics design (data-provider/metrics.py):

  * Counters/gauges are always tracked in plain Python (see `snapshot()`), so
    the module is fully testable and useful for logging without Prometheus.
  * Prometheus exposition is best-effort: if `prometheus_client` is missing the
    backend keeps running and /metrics serves the plain snapshot instead. This
    preserves H10 (`docker compose up` must work) — metrics never crash the app.
  * The instance owns its own CollectorRegistry, so constructing several
    BackendMetrics (e.g. in tests) never collides on the global registry.

Exposed series (namespace `backend`):
  events_consumed_total{stream=...}   counter  events accepted off Kafka (clean/alerts/aggregated)
  events_malformed_total{stream=...}  counter  events skipped as malformed by the handlers
  ws_messages_coalesced_total         counter  pending WS updates superseded by newer state
  ws_messages_dropped_total           counter  pending WS messages shed by the safety valves
  config_publishes_total              counter  successful config.updates publishes
  config_publish_failures_total       counter  failed config.updates publishes (503 path)
  ws_clients_connected                gauge    currently connected /ws/stream clients
  ring_buffer_size                    gauge    events currently buffered for /recent

Label values are the logical stream names ("clean", "alerts", "aggregated"),
not the env-configurable topic names, so dashboards stay stable across
deployments.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

_NAMESPACE = "backend"

# Logical stream names used as label values (stable across topic renames).
STREAM_CLEAN = "clean"
STREAM_ALERTS = "alerts"
STREAM_AGGREGATED = "aggregated"
_STREAMS = (STREAM_CLEAN, STREAM_ALERTS, STREAM_AGGREGATED)

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        CollectorRegistry,
        Counter,
        Gauge,
        generate_latest,
    )

    _PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without the optional dep
    _PROMETHEUS_AVAILABLE = False
    CONTENT_TYPE_LATEST = "text/plain; charset=utf-8"


@dataclass(frozen=True)
class MetricsSnapshot:
    """Point-in-time view of the backend counters (used for logging/tests)."""

    consumed: dict[str, int] = field(default_factory=dict)
    malformed: dict[str, int] = field(default_factory=dict)
    ws_coalesced: int = 0
    ws_dropped: int = 0
    config_publishes: int = 0
    config_publish_failures: int = 0
    ws_clients: int = 0
    ring_buffer_size: int = 0


class BackendMetrics:
    """Metrics holder for the backend.

    All mutations happen on the single asyncio event loop (consumer handlers,
    broadcaster offers, config publishes), so plain int increments are safe.
    """

    def __init__(self) -> None:
        self._consumed: dict[str, int] = {stream: 0 for stream in _STREAMS}
        self._malformed: dict[str, int] = {stream: 0 for stream in _STREAMS}
        self._ws_coalesced = 0
        self._ws_dropped = 0
        self._config_publishes = 0
        self._config_publish_failures = 0
        self._ws_clients = 0
        self._ring_buffer_size = 0

        self._registry = None
        self._c_consumed = self._c_malformed = None
        self._c_ws_coalesced = self._c_ws_dropped = None
        self._c_publishes = self._c_publish_failures = None
        self._g_ws_clients = self._g_ring_buffer = None

        if _PROMETHEUS_AVAILABLE:
            self._registry = CollectorRegistry()
            self._c_consumed = Counter(
                "events_consumed_total",
                "Events accepted off Kafka by the backend consumer",
                labelnames=("stream",),
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            self._c_malformed = Counter(
                "events_malformed_total",
                "Events skipped as malformed by the consumer handlers",
                labelnames=("stream",),
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            self._c_ws_coalesced = Counter(
                "ws_messages_coalesced_total",
                "Pending WebSocket updates superseded by newer per-key state",
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            self._c_ws_dropped = Counter(
                "ws_messages_dropped_total",
                "Pending WebSocket messages shed by the backpressure safety valves",
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            self._c_publishes = Counter(
                "config_publishes_total",
                "Successful config.updates publishes",
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            self._c_publish_failures = Counter(
                "config_publish_failures_total",
                "Failed config.updates publishes (POST /config 503 path)",
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            self._g_ws_clients = Gauge(
                "ws_clients_connected",
                "Currently connected /ws/stream clients",
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            self._g_ring_buffer = Gauge(
                "ring_buffer_size",
                "Events currently buffered for GET /recent",
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            # Pre-register every label combination so series appear at zero
            # instead of materialising only after the first event.
            for stream in _STREAMS:
                self._c_consumed.labels(stream=stream)
                self._c_malformed.labels(stream=stream)

    # -- recording -----------------------------------------------------------

    def record_consumed(self, stream: str) -> None:
        self._consumed[stream] = self._consumed.get(stream, 0) + 1
        if self._c_consumed is not None:
            self._c_consumed.labels(stream=stream).inc()

    def record_malformed(self, stream: str) -> None:
        self._malformed[stream] = self._malformed.get(stream, 0) + 1
        if self._c_malformed is not None:
            self._c_malformed.labels(stream=stream).inc()

    def record_ws_coalesced(self) -> None:
        self._ws_coalesced += 1
        if self._c_ws_coalesced is not None:
            self._c_ws_coalesced.inc()

    def record_ws_dropped(self) -> None:
        self._ws_dropped += 1
        if self._c_ws_dropped is not None:
            self._c_ws_dropped.inc()

    def record_config_publish(self) -> None:
        self._config_publishes += 1
        if self._c_publishes is not None:
            self._c_publishes.inc()

    def record_config_publish_failure(self) -> None:
        self._config_publish_failures += 1
        if self._c_publish_failures is not None:
            self._c_publish_failures.inc()

    # -- gauges (refreshed at scrape time by the /metrics endpoint) -----------

    def set_ws_clients(self, count: int) -> None:
        self._ws_clients = count
        if self._g_ws_clients is not None:
            self._g_ws_clients.set(count)

    def set_ring_buffer_size(self, size: int) -> None:
        self._ring_buffer_size = size
        if self._g_ring_buffer is not None:
            self._g_ring_buffer.set(size)

    # -- reading ---------------------------------------------------------------

    def snapshot(self) -> MetricsSnapshot:
        return MetricsSnapshot(
            consumed=dict(self._consumed),
            malformed=dict(self._malformed),
            ws_coalesced=self._ws_coalesced,
            ws_dropped=self._ws_dropped,
            config_publishes=self._config_publishes,
            config_publish_failures=self._config_publish_failures,
            ws_clients=self._ws_clients,
            ring_buffer_size=self._ring_buffer_size,
        )

    # -- exposition --------------------------------------------------------------

    @property
    def prometheus_available(self) -> bool:
        return self._registry is not None

    def exposition(self) -> tuple[bytes, str]:
        """Render the registry in Prometheus text format.

        Falls back to a plain-text snapshot rendering when prometheus_client is
        not installed, so /metrics always answers (H10: never crash the app).
        """
        if self._registry is not None:
            return generate_latest(self._registry), CONTENT_TYPE_LATEST
        lines = [f"# backend metrics (prometheus_client unavailable)\n{self.snapshot()}\n"]
        return "".join(lines).encode("utf-8"), CONTENT_TYPE_LATEST


# Module-level singleton — same pattern as _broadcaster (broadcaster.py) and
# _buffer (ring_buffer.py).
_metrics = BackendMetrics()
