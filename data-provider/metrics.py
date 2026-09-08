"""
metrics.py — producer metrics for the Data Provider.

Tracks throughput and health counters for the radiation.raw producer and,
when enabled, exposes them as Prometheus metrics over HTTP at /metrics.

Design notes:
  * Counters/gauges are always tracked in plain Python (see `snapshot()`), so
    the module is fully testable and useful for logging without Prometheus.
  * Prometheus exposition is optional and best-effort: if `prometheus_client`
    is missing or the HTTP server can't bind, the producer keeps running and
    falls back to log-only metrics. This preserves H10 (`docker compose up`
    must work) — metrics never crash the producer.
  * Each instance owns its own CollectorRegistry, so constructing several
    ProducerMetrics (e.g. in tests) never collides on the global registry.

Exposed series (namespace `data_provider`):
  events_sent_total            counter  events published to Kafka
  events_discarded_total       counter  rows dropped by the mapper pre-filter
  send_errors_total            counter  failed sends / delivery errors
  bytes_sent_total             counter  serialized value bytes published
  events_per_second            gauge    rolling throughput (sent / elapsed)
  last_event_timestamp_seconds gauge    uploaded_at of the most recent event
                                        (replay progress through the dataset)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from time import monotonic

logger = logging.getLogger(__name__)

_NAMESPACE = "data_provider"

try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

    _PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without the optional dep
    _PROMETHEUS_AVAILABLE = False


@dataclass(frozen=True)
class MetricsSnapshot:
    """Point-in-time view of the producer counters (used for logging/tests)."""

    sent: int
    discarded: int
    errors: int
    bytes_sent: int
    elapsed_seconds: float
    events_per_second: float
    last_event_timestamp: float | None


class ProducerMetrics:
    """
    Thread-aware-ish metrics holder for the producer.

    Counter mutations happen in the producer's calling thread and in
    kafka-python's serializer/errback callbacks. Increments rely on the GIL,
    which is sufficient for monotonic counters used purely as metrics.
    """

    def __init__(self) -> None:
        self._sent = 0
        self._discarded = 0
        self._errors = 0
        self._bytes = 0
        self._last_event_ts: float | None = None
        self._start = monotonic()

        self._registry = None
        self._c_sent = self._c_discarded = self._c_errors = self._c_bytes = None
        self._g_rate = self._g_last_ts = None

        if _PROMETHEUS_AVAILABLE:
            self._registry = CollectorRegistry()
            self._c_sent = Counter(
                "events_sent_total",
                "Events published to Kafka",
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            self._c_discarded = Counter(
                "events_discarded_total",
                "Rows dropped by the mapper pre-filter",
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            self._c_errors = Counter(
                "send_errors_total",
                "Failed sends or delivery errors",
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            self._c_bytes = Counter(
                "bytes_sent_total",
                "Serialized value bytes published",
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            self._g_rate = Gauge(
                "events_per_second",
                "Rolling throughput (sent / elapsed)",
                namespace=_NAMESPACE,
                registry=self._registry,
            )
            self._g_last_ts = Gauge(
                "last_event_timestamp_seconds",
                "uploaded_at of the most recently produced event",
                namespace=_NAMESPACE,
                registry=self._registry,
            )

    # -- recording ---------------------------------------------------------

    def record_sent(self, uploaded_ts: float | None = None) -> None:
        self._sent += 1
        if self._c_sent is not None:
            self._c_sent.inc()
        if uploaded_ts is not None:
            self._last_event_ts = uploaded_ts
            if self._g_last_ts is not None:
                self._g_last_ts.set(uploaded_ts)

    def record_discarded(self) -> None:
        self._discarded += 1
        if self._c_discarded is not None:
            self._c_discarded.inc()

    def record_error(self, _exc: object = None) -> None:
        # Signature accepts an arg so it can be used directly as a kafka-python
        # errback (future.add_errback(metrics.record_error)).
        self._errors += 1
        if self._c_errors is not None:
            self._c_errors.inc()

    def record_bytes(self, n: int) -> None:
        self._bytes += n
        if self._c_bytes is not None:
            self._c_bytes.inc(n)

    # -- reading -----------------------------------------------------------

    @property
    def sent(self) -> int:
        return self._sent

    def elapsed_seconds(self) -> float:
        return monotonic() - self._start

    def events_per_second(self) -> float:
        elapsed = self.elapsed_seconds()
        return self._sent / elapsed if elapsed > 0 else 0.0

    def refresh_rate_gauge(self) -> None:
        """Push the current throughput into the Prometheus gauge."""
        if self._g_rate is not None:
            self._g_rate.set(self.events_per_second())

    def snapshot(self) -> MetricsSnapshot:
        return MetricsSnapshot(
            sent=self._sent,
            discarded=self._discarded,
            errors=self._errors,
            bytes_sent=self._bytes,
            elapsed_seconds=self.elapsed_seconds(),
            events_per_second=self.events_per_second(),
            last_event_timestamp=self._last_event_ts,
        )

    # -- exposition --------------------------------------------------------

    def start_http_server(self, port: int) -> bool:
        """
        Start the Prometheus /metrics endpoint on `port`.

        Returns True if the endpoint is now serving, False otherwise (metrics
        disabled, prometheus_client missing, or the port could not be bound).
        Never raises — metrics must not take down the producer.
        """
        if port <= 0:
            logger.info("Producer metrics HTTP endpoint disabled (port=%d)", port)
            return False
        if not _PROMETHEUS_AVAILABLE:
            logger.warning(
                "prometheus_client not installed — metrics tracked in-process only, "
                "no /metrics endpoint. Install it to expose port %d.",
                port,
            )
            return False
        try:
            start_http_server(port, registry=self._registry)
        except OSError as exc:
            logger.warning("Could not start metrics endpoint on port %d: %s", port, exc)
            return False
        logger.info("Producer metrics available at http://0.0.0.0:%d/metrics", port)
        return True
