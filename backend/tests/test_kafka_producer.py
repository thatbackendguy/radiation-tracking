import asyncio
import json

import pytest
from aiokafka.errors import KafkaConnectionError, KafkaTimeoutError
from unittest.mock import AsyncMock

import app.core.kafka_producer as producer_module
from app.core.kafka_producer import ConfigProducer, ConfigPublishError
from app.core.settings import Settings

SAMPLE = {"cpm_warn_threshold": 50, "cpm_danger_threshold": 500}


def _settings(**overrides) -> Settings:
    base = {
        "kafka_producer_enabled": True,
        "kafka_bootstrap_servers": "kafka:9092",
        "kafka_topic_config": "config.updates",
    }
    base.update(overrides)
    return Settings(**base)


def _patch_aiokafka(monkeypatch) -> AsyncMock:
    """Replace AIOKafkaProducer with an AsyncMock; return the instance it yields."""
    fake = AsyncMock()
    monkeypatch.setattr(producer_module, "AIOKafkaProducer", lambda **kwargs: fake)
    return fake


class TestStart:
    def test_disabled_stays_unavailable(self) -> None:
        prod = ConfigProducer()
        asyncio.run(prod.start(_settings(kafka_producer_enabled=False)))
        assert prod.available is False

    def test_broker_unreachable_degrades(self, monkeypatch) -> None:
        fake = _patch_aiokafka(monkeypatch)
        fake.start.side_effect = KafkaConnectionError("no broker")
        prod = ConfigProducer()
        asyncio.run(prod.start(_settings()))
        assert prod.available is False
        fake.stop.assert_awaited()  # connection attempt is cleaned up

    def test_successful_start(self, monkeypatch) -> None:
        fake = _patch_aiokafka(monkeypatch)
        prod = ConfigProducer()
        asyncio.run(prod.start(_settings()))
        assert prod.available is True
        fake.start.assert_awaited()


class TestPublish:
    def test_publish_when_unavailable_raises(self) -> None:
        prod = ConfigProducer()
        with pytest.raises(ConfigPublishError):
            asyncio.run(prod.publish(SAMPLE))

    def test_publish_encodes_and_sends_to_topic(self, monkeypatch) -> None:
        fake = _patch_aiokafka(monkeypatch)
        prod = ConfigProducer()
        asyncio.run(prod.start(_settings()))
        asyncio.run(prod.publish(SAMPLE))

        fake.send_and_wait.assert_awaited_once()
        topic, value = fake.send_and_wait.await_args.args
        assert topic == "config.updates"
        assert json.loads(value.decode("utf-8")) == SAMPLE

    def test_publish_send_failure_raises(self, monkeypatch) -> None:
        fake = _patch_aiokafka(monkeypatch)
        fake.send_and_wait.side_effect = KafkaTimeoutError("timeout")
        prod = ConfigProducer()
        asyncio.run(prod.start(_settings()))
        with pytest.raises(ConfigPublishError):
            asyncio.run(prod.publish(SAMPLE))


class TestStop:
    def test_stop_idempotent(self, monkeypatch) -> None:
        _patch_aiokafka(monkeypatch)
        prod = ConfigProducer()
        asyncio.run(prod.start(_settings()))
        asyncio.run(prod.stop())
        asyncio.run(prod.stop())  # second stop is a no-op
        assert prod.available is False
