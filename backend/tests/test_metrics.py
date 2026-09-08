"""Tests for the backend metrics registry, its wiring, and GET /metrics.

The metrics singleton is process-global (counters only ever increase, like the
Prometheus series they back), so wiring tests assert on snapshot *deltas*
rather than absolute values to stay order-independent.
"""

import asyncio
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.core.broadcaster import Subscriber, _broadcaster
from app.core.kafka_consumer import _handle_alert, _handle_clean
from app.core.kafka_producer import ConfigProducer, ConfigPublishError
from app.core.metrics import STREAM_ALERTS, STREAM_CLEAN, BackendMetrics, _metrics
from app.core.ring_buffer import _buffer


class TestBackendMetrics:
    def test_snapshot_starts_at_zero(self) -> None:
        metrics = BackendMetrics()
        snap = metrics.snapshot()
        assert snap.consumed == {"clean": 0, "alerts": 0, "aggregated": 0}
        assert snap.malformed == {"clean": 0, "alerts": 0, "aggregated": 0}
        assert snap.ws_coalesced == 0
        assert snap.ws_dropped == 0
        assert snap.config_publishes == 0
        assert snap.config_publish_failures == 0

    def test_counters_increment_per_stream(self) -> None:
        metrics = BackendMetrics()
        metrics.record_consumed(STREAM_CLEAN)
        metrics.record_consumed(STREAM_CLEAN)
        metrics.record_malformed(STREAM_ALERTS)
        snap = metrics.snapshot()
        assert snap.consumed["clean"] == 2
        assert snap.malformed["alerts"] == 1

    def test_gauges_track_latest_value(self) -> None:
        metrics = BackendMetrics()
        metrics.set_ws_clients(3)
        metrics.set_ring_buffer_size(7)
        metrics.set_ws_clients(1)
        snap = metrics.snapshot()
        assert snap.ws_clients == 1
        assert snap.ring_buffer_size == 7

    def test_exposition_renders_all_series(self) -> None:
        metrics = BackendMetrics()
        payload, content_type = metrics.exposition()
        text = payload.decode("utf-8")
        for series in (
            "backend_events_consumed_total",
            "backend_events_malformed_total",
            "backend_ws_messages_coalesced_total",
            "backend_ws_messages_dropped_total",
            "backend_config_publishes_total",
            "backend_config_publish_failures_total",
            "backend_ws_clients_connected",
            "backend_ring_buffer_size",
        ):
            assert series in text
        assert "text/plain" in content_type

    def test_own_registry_no_collision(self) -> None:
        # Two instances must not fight over the global prometheus registry.
        BackendMetrics()
        BackendMetrics()


class TestConsumerWiring:
    def test_clean_event_counts_as_consumed(self, make_event) -> None:
        before = _metrics.snapshot().consumed["clean"]
        msg = SimpleNamespace(value=make_event().model_dump_json().encode("utf-8"))
        asyncio.run(_handle_clean(msg, _buffer, _broadcaster))
        assert _metrics.snapshot().consumed["clean"] == before + 1

    def test_malformed_clean_event_counts_as_malformed(self) -> None:
        before = _metrics.snapshot().malformed["clean"]
        msg = SimpleNamespace(value=b'{"not": "an event"}')
        asyncio.run(_handle_clean(msg, _buffer, _broadcaster))
        assert _metrics.snapshot().malformed["clean"] == before + 1

    def test_alert_event_counts_as_consumed(self) -> None:
        before = _metrics.snapshot().consumed["alerts"]
        msg = SimpleNamespace(value=json.dumps({"reason": "sustained-high"}).encode("utf-8"))
        _handle_alert(msg, _broadcaster)
        assert _metrics.snapshot().consumed["alerts"] == before + 1


class TestBroadcasterWiring:
    def test_superseded_update_counts_as_coalesced(self) -> None:
        before = _metrics.snapshot().ws_coalesced
        subscriber = Subscriber(max_pending=10)
        message = {"type": "clean", "data": {"sensor_id": "s1"}}
        subscriber.offer(message)
        subscriber.offer(message)
        assert _metrics.snapshot().ws_coalesced == before + 1

    def test_fifo_overflow_counts_as_dropped(self) -> None:
        before = _metrics.snapshot().ws_dropped
        subscriber = Subscriber(max_pending=1)
        subscriber.offer({"type": "alert", "data": {"n": 1}})
        subscriber.offer({"type": "alert", "data": {"n": 2}})
        assert _metrics.snapshot().ws_dropped == before + 1


class TestProducerWiring:
    def test_unavailable_publish_counts_as_failure(self) -> None:
        before = _metrics.snapshot().config_publish_failures
        producer = ConfigProducer()  # never started -> unavailable
        with pytest.raises(ConfigPublishError):
            asyncio.run(producer.publish({"cpm_warn_threshold": 1}))
        assert _metrics.snapshot().config_publish_failures == before + 1


class TestMetricsEndpoint:
    def test_returns_prometheus_text(self, client: TestClient) -> None:
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "text/plain" in response.headers["content-type"]
        assert "backend_events_consumed_total" in response.text

    def test_gauges_refresh_at_scrape_time(self, client: TestClient, make_event) -> None:
        async def _prepare() -> Subscriber:
            # Subscribe inside the running loop: Subscriber's asyncio.Event needs
            # a current event loop on Python 3.9.
            await _buffer.append(make_event())
            return _broadcaster.subscribe()

        subscriber = asyncio.run(_prepare())
        try:
            client.get("/metrics")
            snap = _metrics.snapshot()
            assert snap.ring_buffer_size == 1
            assert snap.ws_clients == 1
        finally:
            _broadcaster.unsubscribe(subscriber)


class TestHealthDegraded:
    def test_degraded_consumer_reported_with_http_200(
        self, client: TestClient, monkeypatch
    ) -> None:
        monkeypatch.setattr("app.core.kafka_consumer._status", "degraded")
        response = client.get("/health")
        assert response.status_code == 200  # compose gating contract: always 200
        body = response.json()
        assert body["status"] == "degraded"
        assert body["kafka_consumer"] == "degraded"

    def test_degraded_producer_reported_with_http_200(
        self, client: TestClient, monkeypatch
    ) -> None:
        import app.core.kafka_producer as producer_module

        monkeypatch.setattr(producer_module._producer, "status", "degraded")
        body = client.get("/health").json()
        assert body["status"] == "degraded"
        assert body["config_producer"] == "degraded"

    def test_disabled_components_still_ok(self, client: TestClient, monkeypatch) -> None:
        import app.core.kafka_producer as producer_module

        monkeypatch.setattr("app.core.kafka_consumer._status", "disabled")
        monkeypatch.setattr(producer_module._producer, "status", "disabled")
        body = client.get("/health").json()
        assert body["status"] == "ok"  # deliberate env toggle is not degradation
