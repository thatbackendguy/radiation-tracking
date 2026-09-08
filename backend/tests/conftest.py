import asyncio

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers.config import ConfigResponse
import app.routers.config as config_module


@pytest.fixture(autouse=True)
def _ensure_event_loop():
    """Guarantee a current event loop on the main thread.

    On Python 3.9 ``asyncio.Queue()`` binds to the loop at construction via
    ``get_event_loop()``, which raises once a prior ``asyncio.run`` has closed
    the loop. Synchronous broadcaster tests construct queues outside any running
    loop, so we provide one here (a no-op on 3.10+).
    """
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    yield


@pytest.fixture(autouse=True)
def disable_kafka_topic_init(monkeypatch):
    """Keep endpoint tests hermetic — the lifespan must not touch a real broker."""
    monkeypatch.setattr("app.core.settings.settings.kafka_topic_init_enabled", False)


@pytest.fixture(autouse=True)
def disable_kafka_consumer(monkeypatch):
    """Keep endpoint tests hermetic — no background consumer task in tests."""
    monkeypatch.setattr("app.core.settings.settings.kafka_consumer_enabled", False)


@pytest.fixture(autouse=True)
def stub_config_producer(monkeypatch):
    """Replace the config.updates producer with a hermetic stub.

    Endpoint tests never run the lifespan (TestClient isn't entered as a context
    manager), so the singleton stays unavailable and POST /config would
    503. Stub its methods so publish succeeds by default; individual tests can set
    ``publish.side_effect`` to exercise the failure path. Returns the stubbed
    singleton so tests can assert on the published payload.
    """
    from unittest.mock import AsyncMock

    import app.core.kafka_producer as producer_module

    monkeypatch.setattr("app.core.settings.settings.kafka_producer_enabled", False)
    monkeypatch.setattr(producer_module._producer, "start", AsyncMock())
    monkeypatch.setattr(producer_module._producer, "stop", AsyncMock())
    monkeypatch.setattr(producer_module._producer, "publish", AsyncMock())
    return producer_module._producer


@pytest.fixture(autouse=True)
def reset_ring_buffer():
    """Empty the shared ring buffer before and after each test."""
    import app.core.ring_buffer as ring_buffer_module

    ring_buffer_module._buffer._events.clear()
    yield
    ring_buffer_module._buffer._events.clear()


@pytest.fixture(autouse=True)
def reset_broadcaster():
    """Drop any subscriber queues between tests so fan-out stays isolated."""
    import app.core.broadcaster as broadcaster_module

    broadcaster_module._broadcaster._subscribers.clear()
    yield
    broadcaster_module._broadcaster._subscribers.clear()


@pytest.fixture(autouse=True)
def reset_active_config():
    """Reset in-memory config to defaults before each test."""
    from app.core.settings import settings

    original = ConfigResponse(
        cpm_warn_threshold=settings.cpm_warn_threshold,
        cpm_danger_threshold=settings.cpm_danger_threshold,
        area=None,
        timespan=None,
    )
    config_module._active_config = original
    yield
    config_module._active_config = original


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def make_event():
    """Factory for valid RadiationEvent instances with overridable fields."""
    from app.models import RadiationEvent

    def _make(**overrides):
        defaults = {
            "sensor_id": "sensor-1",
            "captured_at": "2026-06-11T10:00:00Z",
            "uploaded_at": "2026-06-11T10:05:00Z",
            "latitude": 35.0,
            "longitude": 139.0,
            "cpm": 42.0,
            "classification": "SAFE",
        }
        defaults.update(overrides)
        return RadiationEvent(**defaults)

    return _make
