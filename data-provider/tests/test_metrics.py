"""
Tests for data_provider/metrics.py

These are pure unit tests — no Kafka, no HTTP. They verify the in-process
counters, the snapshot, and (when prometheus_client is installed) the
exposition registry.

Run:
    pytest data-provider/tests/test_metrics.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from metrics import ProducerMetrics  # noqa: E402


# ---------------------------------------------------------------------------
# Counters & snapshot
# ---------------------------------------------------------------------------


def test_new_metrics_start_at_zero():
    snap = ProducerMetrics().snapshot()
    assert (snap.sent, snap.discarded, snap.errors, snap.bytes_sent) == (0, 0, 0, 0)
    assert snap.last_event_timestamp is None


def test_record_sent_increments():
    m = ProducerMetrics()
    m.record_sent()
    m.record_sent()
    assert m.snapshot().sent == 2
    assert m.sent == 2  # cheap accessor matches snapshot


def test_record_discarded_and_error_increment():
    m = ProducerMetrics()
    m.record_discarded()
    m.record_error()
    m.record_error()
    snap = m.snapshot()
    assert snap.discarded == 1
    assert snap.errors == 2


def test_record_error_accepts_errback_argument():
    # kafka-python calls errbacks with the exception instance.
    m = ProducerMetrics()
    m.record_error(ValueError("boom"))
    assert m.snapshot().errors == 1


def test_record_bytes_accumulates():
    m = ProducerMetrics()
    m.record_bytes(120)
    m.record_bytes(80)
    assert m.snapshot().bytes_sent == 200


def test_last_event_timestamp_tracks_most_recent():
    m = ProducerMetrics()
    m.record_sent(uploaded_ts=1000.0)
    m.record_sent(uploaded_ts=2000.0)
    assert m.snapshot().last_event_timestamp == 2000.0


def test_record_sent_without_timestamp_keeps_previous():
    m = ProducerMetrics()
    m.record_sent(uploaded_ts=1500.0)
    m.record_sent(uploaded_ts=None)
    assert m.snapshot().last_event_timestamp == 1500.0


# ---------------------------------------------------------------------------
# Throughput
# ---------------------------------------------------------------------------


def test_events_per_second_zero_before_any_send():
    assert ProducerMetrics().events_per_second() == 0.0


def test_events_per_second_positive_after_sends():
    m = ProducerMetrics()
    for _ in range(10):
        m.record_sent()
    assert m.events_per_second() > 0.0


def test_refresh_rate_gauge_does_not_raise():
    m = ProducerMetrics()
    m.record_sent()
    m.refresh_rate_gauge()  # no-op when prometheus is absent, must not raise


# ---------------------------------------------------------------------------
# HTTP exposition
# ---------------------------------------------------------------------------


def test_start_http_server_disabled_when_port_zero():
    assert ProducerMetrics().start_http_server(0) is False


def test_start_http_server_disabled_when_port_negative():
    assert ProducerMetrics().start_http_server(-1) is False


# ---------------------------------------------------------------------------
# Prometheus registry (only if the optional dependency is present)
# ---------------------------------------------------------------------------


def test_prometheus_registry_reflects_counts():
    pytest.importorskip("prometheus_client")
    from prometheus_client import generate_latest

    m = ProducerMetrics()
    m.record_sent(uploaded_ts=1234.0)
    m.record_discarded()
    m.record_bytes(50)
    m.refresh_rate_gauge()

    exposition = generate_latest(m._registry).decode("utf-8")
    assert "data_provider_events_sent_total 1.0" in exposition
    assert "data_provider_events_discarded_total 1.0" in exposition
    assert "data_provider_bytes_sent_total 50.0" in exposition
    assert "data_provider_last_event_timestamp_seconds 1234.0" in exposition


def test_multiple_instances_do_not_collide_on_registry():
    pytest.importorskip("prometheus_client")
    # Each instance owns a private registry, so constructing several is safe.
    a = ProducerMetrics()
    b = ProducerMetrics()
    a.record_sent()
    assert a.snapshot().sent == 1
    assert b.snapshot().sent == 0
