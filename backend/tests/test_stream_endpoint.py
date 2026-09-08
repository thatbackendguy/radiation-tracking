import asyncio

import pytest
from starlette.websockets import WebSocketDisconnect

from app.routers.stream import _origin_allowed


def test_origin_allowed_wildcard_allows_any(monkeypatch):
    monkeypatch.setattr("app.core.settings.settings.cors_origins", "*")
    assert _origin_allowed("http://anything.example") is True


def test_origin_allowed_missing_header_when_restricted(monkeypatch):
    # No Origin header (non-browser client) is allowed even under a restricted list.
    monkeypatch.setattr("app.core.settings.settings.cors_origins", "http://allowed.example")
    assert _origin_allowed(None) is True


def test_origin_rejected_when_not_listed(monkeypatch):
    monkeypatch.setattr("app.core.settings.settings.cors_origins", "http://allowed.example")
    assert _origin_allowed("http://evil.example") is False


def test_ws_stream_sends_ring_buffer_catchup(client, make_event):
    """On connect the client receives a snapshot of recent clean events."""
    import app.core.ring_buffer as ring_buffer_module

    async def _fill():
        await ring_buffer_module._buffer.append(make_event(sensor_id="sensor-a"))
        await ring_buffer_module._buffer.append(make_event(sensor_id="sensor-b"))

    asyncio.run(_fill())

    with client.websocket_connect("/ws/stream") as ws:
        first = ws.receive_json()
        second = ws.receive_json()

    assert first["type"] == "clean"
    assert first["data"]["sensor_id"] == "sensor-a"
    assert second["data"]["sensor_id"] == "sensor-b"


def test_ws_stream_empty_buffer_connects_without_catchup(client):
    """A connection with an empty buffer still upgrades cleanly (no snapshot)."""
    with client.websocket_connect("/ws/stream"):
        # Subscriber is registered for live updates even with nothing to replay.
        import app.core.broadcaster as broadcaster_module

        assert broadcaster_module._broadcaster.subscriber_count == 1


def test_ws_stream_allows_listed_origin(client, monkeypatch):
    monkeypatch.setattr("app.core.settings.settings.cors_origins", "http://allowed.example")

    with client.websocket_connect("/ws/stream", headers={"origin": "http://allowed.example"}):
        import app.core.broadcaster as broadcaster_module

        assert broadcaster_module._broadcaster.subscriber_count == 1


def test_ws_stream_rejects_disallowed_origin(client, monkeypatch):
    monkeypatch.setattr("app.core.settings.settings.cors_origins", "http://allowed.example")

    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws/stream", headers={"origin": "http://evil.example"}):
            pass


class _FakeWebSocket:
    """Records send_json payloads for drain-loop tests."""

    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send_json(self, message: dict) -> None:
        self.sent.append(message)


class TestSubscribeViewFilter:
    """Per-client subscribe filtering on /ws/stream (ADR-017)."""

    def _clean(self, sensor_id: str, lat: float, lon: float) -> dict:
        return {
            "type": "clean",
            "data": {
                "sensor_id": sensor_id,
                "latitude": lat,
                "longitude": lon,
                "captured_at": "2026-06-11T10:00:00Z",
            },
        }

    def test_apply_subscribe_installs_filter(self):
        import app.core.broadcaster as broadcaster_module
        from app.routers.stream import _apply_subscribe

        subscriber = broadcaster_module.Subscriber(max_pending=10)
        _apply_subscribe(
            subscriber,
            {
                "type": "subscribe",
                "area": {"min_lat": 30, "max_lat": 40, "min_lon": 130, "max_lon": 145},
            },
        )
        assert subscriber.view_filter is not None

        subscriber.offer(self._clean("tokyo", 35.6, 139.7))
        subscriber.offer(self._clean("berlin", 52.5, 13.4))
        batch = subscriber.drain()
        assert [m["data"]["sensor_id"] for m in batch] == ["tokyo"]
        assert subscriber.filtered_count == 1

    def test_apply_subscribe_noop_clears_filter(self):
        import app.core.broadcaster as broadcaster_module
        from app.routers.stream import _apply_subscribe

        subscriber = broadcaster_module.Subscriber(max_pending=10)
        _apply_subscribe(
            subscriber,
            {
                "type": "subscribe",
                "area": {"min_lat": 30, "max_lat": 40, "min_lon": 130, "max_lon": 145},
            },
        )
        assert subscriber.view_filter is not None
        # An all-null subscribe widens the view back to everything.
        _apply_subscribe(subscriber, {"type": "subscribe", "area": None, "timespan": None})
        assert subscriber.view_filter is None

    def test_apply_subscribe_invalid_keeps_previous_filter(self):
        import app.core.broadcaster as broadcaster_module
        from app.routers.stream import _apply_subscribe

        subscriber = broadcaster_module.Subscriber(max_pending=10)
        _apply_subscribe(
            subscriber,
            {
                "type": "subscribe",
                "area": {"min_lat": 30, "max_lat": 40, "min_lon": 130, "max_lon": 145},
            },
        )
        previous = subscriber.view_filter
        _apply_subscribe(subscriber, {"type": "subscribe", "area": {"min_lat": "bad"}})
        assert subscriber.view_filter is previous

    def test_ws_subscribe_message_reaches_subscriber(self, client):
        """End-to-end: a subscribe frame installs the filter on the live subscriber."""
        import json
        import time

        import app.core.broadcaster as broadcaster_module

        with client.websocket_connect("/ws/stream") as ws:
            ws.send_text(
                json.dumps(
                    {
                        "type": "subscribe",
                        "area": {"min_lat": 30, "max_lat": 40, "min_lon": 130, "max_lon": 145},
                    }
                )
            )
            subscriber = next(iter(broadcaster_module._broadcaster._subscribers))
            for _ in range(50):  # the receive loop runs in the app portal thread
                if subscriber.view_filter is not None:
                    break
                time.sleep(0.02)
            assert subscriber.view_filter is not None

    def test_ws_ignores_malformed_client_frames(self, client):
        """Garbage frames must not kill the connection or install a filter."""
        import time

        import app.core.broadcaster as broadcaster_module

        with client.websocket_connect("/ws/stream") as ws:
            ws.send_text("not-json")
            ws.send_text('{"type": "subscribe", "area": {"min_lat": "bad"}}')
            time.sleep(0.05)
            subscriber = next(iter(broadcaster_module._broadcaster._subscribers))
            assert subscriber.view_filter is None
            assert broadcaster_module._broadcaster.subscriber_count == 1


def test_drain_to_client_sends_coalesced_batch(monkeypatch):
    """The drain loop flushes a coalesced clean state plus FIFO alerts."""
    import app.core.broadcaster as broadcaster_module
    from app.routers.stream import _drain_to_client

    # Zero interval keeps the flush deterministic — no real sleep in the test.
    monkeypatch.setattr(broadcaster_module._broadcaster, "_flush_interval", 0.0)

    async def scenario():
        subscriber = broadcaster_module._broadcaster.subscribe()
        try:
            subscriber.offer({"type": "clean", "data": {"sensor_id": "s-1", "cpm": 1}})
            subscriber.offer({"type": "clean", "data": {"sensor_id": "s-1", "cpm": 2}})
            subscriber.offer({"type": "alert", "data": {"id": "a-1"}})

            ws = _FakeWebSocket()
            task = asyncio.create_task(_drain_to_client(ws, subscriber))
            for _ in range(10):
                await asyncio.sleep(0)
                if ws.sent:
                    break
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            return ws.sent
        finally:
            broadcaster_module._broadcaster.unsubscribe(subscriber)

    sent = asyncio.run(scenario())
    clean = [m for m in sent if m["type"] == "clean"]
    alerts = [m for m in sent if m["type"] == "alert"]
    assert len(clean) == 1 and clean[0]["data"]["cpm"] == 2  # coalesced to latest
    assert len(alerts) == 1
