"""In-process fan-out hub from the Kafka consumer to WebSocket clients.

A single background consumer reads radiation.clean / radiation.alerts /
radiation.aggregated and calls `publish()`; every connected /ws/stream client
holds its own `Subscriber` and drains it independently. Decoupling them this way
means one slow client cannot stall the consumer or the other clients.

Backpressure handling (Week 6) is backpressure-aware rather than a blunt
drop-oldest queue:

* **clean** events coalesce latest-wins per `sensor_id`, and **aggregated**
  events per geo bucket — a slow client sees the freshest state per key instead
  of a growing backlog, which also matches the frontend's in-place marker
  replacement. Superseded updates are counted, not delivered. The lane is bounded
  by distinct-key count (BROADCASTER_COALESCED_MAXSIZE): past the cap the
  least-recently-updated key's pending state is evicted (counted, and re-sent when
  that key next updates), so worst-case memory can't grow one entry per active
  sensor without limit.
* **alerts** (and any keyless message) are queued FIFO and never coalesced —
  threshold breaches must not be merged away. A generous per-client cap bounds
  memory as a last-resort safety valve; only sustained pathological backpressure
  past the cap sheds the oldest, which is counted.

Each flush delivers the **FIFO lane first, then coalesced state**, so
safety-critical alerts lead the batch instead of trailing bulk state within a
window.

The drain side (routers/stream.py) flushes at most once per
BROADCASTER_FLUSH_INTERVAL_MS, so coalescing accumulates within each window and
the send cadence to any one client stays bounded.

No lock is needed: the backend runs a single asyncio event loop, so the set
mutations and per-subscriber operations below never interleave mid-await.
"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict, deque

from app.core.metrics import _metrics
from app.core.settings import settings

logger = logging.getLogger(__name__)


def _coalesce_key(message: dict) -> tuple[str, str] | None:
    """Return a (type, key) coalescing identity, or None for FIFO delivery.

    Only high-volume state streams coalesce: clean by sensor, aggregated by geo
    bucket. Alerts and anything without a stable key fall through to FIFO so they
    are never merged away.
    """
    message_type = message.get("type")
    data = message.get("data") or {}
    if message_type == "clean":
        sensor_id = data.get("sensor_id")
        return ("clean", sensor_id) if sensor_id is not None else None
    if message_type == "aggregated":
        bucket = data.get("geohash") or data.get("bucket")
        return ("aggregated", bucket) if bucket is not None else None
    return None


class Subscriber:
    """A single client's pending-message buffer with coalescing + FIFO lanes."""

    def __init__(self, max_pending: int, coalesced_max: int = 5000) -> None:
        self._max_pending = max_pending
        self._coalesced_max = coalesced_max
        # Insertion order tracks last-update recency (updates move to the end), so
        # the oldest-updated key is evicted first when the lane hits its cap.
        self._coalesced: OrderedDict[tuple[str, str], dict] = OrderedDict()
        self._fifo: deque[dict] = deque()
        self._ready = asyncio.Event()
        self.coalesced_count = 0
        self.dropped_count = 0
        self.filtered_count = 0
        # Per-client view filter (ADR-017) — set via the /ws/stream subscribe
        # message; None delivers everything. Applied at offer() time so filtered
        # events never occupy buffer space.
        self.view_filter = None

    def set_view_filter(self, view_filter) -> None:
        """Install (or clear, with None) this client's view filter."""
        self.view_filter = view_filter

    def offer(self, message: dict) -> None:
        """Accept a message, coalescing or queuing it by type."""
        if self.view_filter is not None and not self.view_filter.matches_message(message):
            self.filtered_count += 1
            return
        key = _coalesce_key(message)
        if key is not None:
            if key in self._coalesced:
                # An unsent update for this key is superseded by the newer state.
                self.coalesced_count += 1
                _metrics.record_ws_coalesced()
                self._coalesced[key] = message
                self._coalesced.move_to_end(key)
            else:
                # Safety valve: bound the coalescing lane by distinct-key count so a
                # stalled client can't grow one entry per active sensor without limit.
                if len(self._coalesced) >= self._coalesced_max and self._coalesced:
                    self._coalesced.popitem(last=False)
                    self.dropped_count += 1
                    _metrics.record_ws_dropped()
                    logger.debug("Subscriber coalesced lane over cap; evicting oldest key.")
                self._coalesced[key] = message
        else:
            self._fifo.append(message)
            # Safety valve: a client that never drains must not grow without bound.
            while len(self._fifo) > self._max_pending:
                self._fifo.popleft()
                self.dropped_count += 1
                _metrics.record_ws_dropped()
                logger.debug("Subscriber FIFO over cap; dropping oldest message.")
        self._ready.set()

    def drain(self) -> list[dict]:
        """Return all pending messages and clear the buffer.

        FIFO (alerts) leads the batch so safety-critical breaches are delivered
        before bulk coalesced state within a flush window; coalesced state follows.
        """
        batch = list(self._fifo)
        batch.extend(self._coalesced.values())
        self._coalesced.clear()
        self._fifo.clear()
        self._ready.clear()
        return batch

    async def wait(self) -> None:
        """Block until at least one message is pending."""
        await self._ready.wait()


class Broadcaster:
    def __init__(
        self, queue_maxsize: int, flush_interval_ms: int, coalesced_maxsize: int = 5000
    ) -> None:
        self._max_pending = queue_maxsize
        self._coalesced_maxsize = coalesced_maxsize
        self._flush_interval = max(0.0, flush_interval_ms / 1000)
        self._subscribers: set[Subscriber] = set()

    def subscribe(self) -> Subscriber:
        """Register a new client and return its dedicated buffer."""
        subscriber = Subscriber(self._max_pending, self._coalesced_maxsize)
        self._subscribers.add(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: Subscriber) -> None:
        self._subscribers.discard(subscriber)

    def publish(self, message: dict) -> None:
        """Fan `message` out to every subscriber's buffer."""
        for subscriber in self._subscribers:
            subscriber.offer(message)

    @property
    def flush_interval(self) -> float:
        """Throttle window (seconds) the drain loop waits between flushes."""
        return self._flush_interval

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)


# Module-level singleton — same pattern as _buffer (ring_buffer.py) and
# _active_config (routers/config.py).
_broadcaster = Broadcaster(
    queue_maxsize=settings.broadcaster_queue_maxsize,
    flush_interval_ms=settings.broadcaster_flush_interval_ms,
    coalesced_maxsize=settings.broadcaster_coalesced_maxsize,
)
