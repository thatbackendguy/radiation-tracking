"""WebSocket endpoint /ws/stream — live clean + alert events to the map client.

Auth-less but origin-checked (guidelines.md H4/H9 spirit: no credentials, but
only browsers served from an allowed origin may connect). On connect the client
gets a catch-up snapshot of recent clean events from the ring buffer, then a
live stream fanned out by the broadcaster.

Per-client view filters (ADR-017): the client may send
``{"type": "subscribe", "area": {...}, "timespan": {...}}`` at any time to
narrow (or, with nulls, widen) **its own** live stream — other clients are
unaffected. The catch-up snapshot is sent before any subscribe can arrive, so
it is intentionally unfiltered; the frontend applies the same filters locally.
"""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.broadcaster import Subscriber, _broadcaster
from app.core.ring_buffer import _buffer
from app.core.settings import settings
from app.core.view_filter import ViewFilter

logger = logging.getLogger(__name__)

router = APIRouter()


def _origin_allowed(origin: str | None) -> bool:
    allowed = settings.allowed_origins()
    if "*" in allowed:
        return True
    # No Origin header => non-browser client (curl, tests); allow when not wildcard-only.
    if origin is None:
        return True
    return origin in allowed


async def _drain_to_client(websocket: WebSocket, subscriber: Subscriber) -> None:
    """Forward broadcast messages to the client until cancelled.

    Waits for pending data, then holds for the flush interval so a burst
    coalesces into one batch before sending — bounding the send cadence to any
    one client without starving it.
    """
    flush_interval = _broadcaster.flush_interval
    while True:
        await subscriber.wait()
        if flush_interval > 0:
            await asyncio.sleep(flush_interval)
        for message in subscriber.drain():
            await websocket.send_json(message)


def _apply_subscribe(subscriber: Subscriber, payload: dict) -> None:
    """Install the view filter from a subscribe message (ADR-017).

    A malformed message is logged and ignored — the previous filter stays in
    place. Nothing is sent back: the drain task is the only coroutine allowed
    to call send_json, and the client applies the same filter locally anyway.
    """
    try:
        view_filter = ViewFilter.from_subscribe(payload)
    except ValueError as exc:
        logger.warning("Ignoring invalid /ws/stream subscribe message: %s", exc)
        return
    subscriber.set_view_filter(None if view_filter.is_noop else view_filter)


async def _receive_loop(websocket: WebSocket, subscriber: Subscriber) -> None:
    """Resolve when the client disconnects; handle subscribe messages meanwhile.

    The send loop never calls receive(), so a disconnect frame would otherwise
    sit unread and only surface later as an opaque send failure. Reading here
    detects the disconnect cleanly so the connection can be torn down, and is
    also where per-client view-filter updates (subscribe messages) arrive.
    """
    while True:
        message = await websocket.receive()
        if message["type"] == "websocket.disconnect":
            return
        text = message.get("text")
        if text is None:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("Ignoring non-JSON /ws/stream client message.")
            continue
        if isinstance(payload, dict) and payload.get("type") == "subscribe":
            _apply_subscribe(subscriber, payload)


@router.websocket("/ws/stream")
async def stream(websocket: WebSocket) -> None:
    origin = websocket.headers.get("origin")
    if not _origin_allowed(origin):
        logger.warning("Rejected WebSocket connection from disallowed origin: %s", origin)
        # 1008 = policy violation. Closed before accept, so the client never upgrades.
        await websocket.close(code=1008)
        return

    await websocket.accept()

    # Subscribe BEFORE snapshotting: an event arriving mid-snapshot then lands in
    # the subscriber buffer instead of the gap between snapshot and subscribe. The
    # only overlap is a possible duplicate event, which the client dedupes by
    # sensor_id.
    subscriber = _broadcaster.subscribe()
    try:
        # Catch-up: replay recent clean events so the map paints immediately. Sent
        # before the drain task starts so only one coroutine ever calls send_json.
        snapshot = await _buffer.recent(settings.recent_default_limit)
        for event in snapshot:
            await websocket.send_json({"type": "clean", "data": event.model_dump(mode="json")})

        # Race the live send loop against the receive loop (disconnect detection
        # + subscribe handling); whichever finishes first tears down the other.
        send_task = asyncio.create_task(_drain_to_client(websocket, subscriber))
        recv_task = asyncio.create_task(_receive_loop(websocket, subscriber))
        try:
            done, _ = await asyncio.wait(
                {send_task, recv_task}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            send_task.cancel()
            recv_task.cancel()
            await asyncio.gather(send_task, recv_task, return_exceptions=True)
        # Surface a genuine send failure (disconnect detection returns cleanly).
        for task in done:
            exc = task.exception()
            if exc is not None:
                raise exc
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected.")
    except Exception:
        logger.exception("WebSocket stream terminated unexpectedly.")
    finally:
        _broadcaster.unsubscribe(subscriber)
