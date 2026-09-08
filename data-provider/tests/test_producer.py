"""
Unit tests for data_provider/producer.py — speed parsing, throttle behaviour, and the
Kafka producer config wiring (batch.size / linger.ms / compression, ADR-012).

No broker and no real KafkaProducer: producer.py loads under a synthetic package (its
relative imports need a real package), and KafkaProducer is monkeypatched to a spy so the
config-wiring tests never open a socket. The live path is covered by the integration test.

Run:
    pytest data-provider/tests/test_producer.py -q
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest


def _load_producer_module():
    """Import producer.py from the hyphenated data-provider/ dir as a package submodule."""
    dp_dir = Path(__file__).resolve().parent.parent
    pkg_name = "data_provider_pkg"
    if pkg_name not in sys.modules:
        pkg = types.ModuleType(pkg_name)
        pkg.__path__ = [str(dp_dir)]
        sys.modules[pkg_name] = pkg
    spec = importlib.util.spec_from_file_location(f"{pkg_name}.producer", dp_dir / "producer.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


producer = _load_producer_module()


class TestParseSpeed:
    def test_rate_mode(self) -> None:
        assert producer._parse_speed("500") == ("rate", 500.0)

    def test_multiplier_mode(self) -> None:
        assert producer._parse_speed("10x") == ("multiplier", 10.0)
        assert producer._parse_speed("1X") == ("multiplier", 1.0)  # case-insensitive suffix

    def test_max_mode(self) -> None:
        assert producer._parse_speed("max") == ("max", 0.0)
        assert producer._parse_speed(" MAX ") == ("max", 0.0)  # trimmed + case-insensitive

    def test_zero_rate_rejected(self) -> None:
        # Finding (g): --speed 0 used to reach the replay loop and crash with
        # ZeroDivisionError; it must fail fast with a clear message instead.
        with pytest.raises(ValueError, match="positive"):
            producer._parse_speed("0")

    def test_zero_multiplier_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            producer._parse_speed("0x")

    def test_negative_speed_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            producer._parse_speed("-5")

    def test_non_finite_speed_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            producer._parse_speed("inf")
        with pytest.raises(ValueError, match="positive"):
            producer._parse_speed("nan")

    def test_garbage_speed_rejected(self) -> None:
        with pytest.raises(ValueError, match="expected"):
            producer._parse_speed("fast")


class TestApplySpeedThrottle:
    def test_max_mode_never_sleeps(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[float] = []
        monkeypatch.setattr(producer.time, "sleep", lambda s: calls.append(s))
        # Even with a real time gap, max mode must not sleep.
        producer._apply_speed("max", 0.0, "2026-06-19T01:00:10", 1_000.0)
        assert calls == []

    def test_rate_mode_sleeps_inverse_rate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[float] = []
        monkeypatch.setattr(producer.time, "sleep", lambda s: calls.append(s))
        producer._apply_speed("rate", 100.0, None, None)
        assert calls == [pytest.approx(0.01)]


class _SpyProducer:
    """Captures the kwargs KafkaProducer was constructed with (no network)."""

    last_kwargs: dict = {}

    def __init__(self, **kwargs) -> None:
        _SpyProducer.last_kwargs = kwargs


class TestMakeProducerConfig:
    def _make(self, monkeypatch: pytest.MonkeyPatch, **kwargs):
        monkeypatch.setattr(producer, "KafkaProducer", _SpyProducer)
        metrics = producer.ProducerMetrics()
        producer._make_producer("localhost:9092", metrics, **kwargs)
        return _SpyProducer.last_kwargs

    def test_defaults_match_adr_012(self, monkeypatch: pytest.MonkeyPatch) -> None:
        kw = self._make(monkeypatch)
        assert kw["batch_size"] == producer.DEFAULT_BATCH_SIZE == 65_536
        assert kw["linger_ms"] == producer.DEFAULT_LINGER_MS == 20
        assert kw["compression_type"] == "lz4"
        # The delivery contract must be preserved by the tuning.
        assert kw["acks"] == "all"
        assert kw["retries"] == 3

    def test_overrides_are_passed_through(self, monkeypatch: pytest.MonkeyPatch) -> None:
        kw = self._make(monkeypatch, batch_size=131_072, linger_ms=50, compression_type="gzip")
        assert (kw["batch_size"], kw["linger_ms"], kw["compression_type"]) == (
            131_072,
            50,
            "gzip",
        )

    @pytest.mark.parametrize("value", ["none", "", None])
    def test_compression_none_becomes_null(self, monkeypatch: pytest.MonkeyPatch, value) -> None:
        # kafka-python wants None (not the string "none"/"") to disable compression.
        kw = self._make(monkeypatch, compression_type=value)
        assert kw["compression_type"] is None
