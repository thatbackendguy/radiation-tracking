"""In-memory ring buffer holding the most recent radiation.clean events.

Backs the REST /recent endpoint (Week 3) and WebSocket catch-up (Week 4). Bounded
by RECENT_BUFFER_SIZE so memory stays flat regardless of stream rate; oldest
events are evicted automatically.

Each event is tagged with a monotonically increasing sequence number on append.
That sequence is the opaque cursor behind /recent pagination (Week 6): timestamps
are not unique or monotonic across sensors, but the append sequence always is.
Because the buffer is bounded, paging is best-effort over the in-memory window —
events evicted before a client pages back to them are simply gone.

No lock is needed: the backend runs a single asyncio event loop, and append and
the snapshot reads below are atomic deque operations with no await point between
read and write.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Callable, Optional

from app.core.settings import settings
from app.models import RadiationEvent


@dataclass(frozen=True)
class BufferedEvent:
    """A ring-buffer entry pairing an event with its append sequence number."""

    seq: int
    event: RadiationEvent


@dataclass(frozen=True)
class Page:
    """A slice of buffered events plus the cursor to fetch the next older page."""

    events: list[RadiationEvent]
    next_cursor: int | None
    has_more: bool


class RingBuffer:
    def __init__(self, maxsize: int) -> None:
        self._events: deque[BufferedEvent] = deque(maxlen=maxsize)
        self._next_seq = 0

    async def append(self, event: RadiationEvent) -> None:
        self._events.append(BufferedEvent(seq=self._next_seq, event=event))
        self._next_seq += 1

    async def recent(self, limit: int) -> list[RadiationEvent]:
        """Return up to `limit` newest events, oldest-first within the slice."""
        if limit <= 0:
            return []
        return [entry.event for entry in list(self._events)[-limit:]]

    async def page(
        self,
        cursor: int | None,
        limit: int,
        matches: Optional[Callable[[RadiationEvent], bool]] = None,
    ) -> Page:
        """Return up to `limit` newest events older than `cursor`, oldest-first.

        `cursor is None` returns the newest page (same window as `recent`). A
        cursor returns the newest `limit` events whose sequence is strictly below
        it, so passing back the previous page's `next_cursor` walks backwards
        through the buffered window. `matches` (per-client view filter, ADR-017)
        narrows the candidate set before windowing, so cursors stay valid across
        filtered and unfiltered requests. `next_cursor` is the oldest sequence in
        the returned slice (or None when no older matching events remain);
        `has_more` reports whether older matching events are still buffered.
        """
        if limit <= 0:
            return Page(events=[], next_cursor=None, has_more=False)

        entries = list(self._events)
        if cursor is not None:
            entries = [entry for entry in entries if entry.seq < cursor]
        if matches is not None:
            entries = [entry for entry in entries if matches(entry.event)]

        window = entries[-limit:]
        if not window:
            return Page(events=[], next_cursor=None, has_more=False)

        has_more = len(entries) > len(window)
        return Page(
            events=[entry.event for entry in window],
            next_cursor=window[0].seq if has_more else None,
            has_more=has_more,
        )

    def __len__(self) -> int:
        return len(self._events)


# Module-level singleton — same pattern as _active_config in routers/config.py.
_buffer = RingBuffer(maxsize=settings.recent_buffer_size)
