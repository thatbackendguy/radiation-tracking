"""Kafka producer for the config.updates topic.

The backend (M4) is the sole producer of config.updates (guidelines.md §7.2):
user-defined filter settings (thresholds, area, timespan) are published here and
consumed by the Flink ThresholdClassifierFunction (M2/M3) as broadcast state.

Mirrors the degraded-mode philosophy of kafka_admin.py / kafka_consumer.py — an
unreachable broker at startup does not crash the service; the producer simply
stays unavailable. The write endpoint (POST /config) then fails loudly
with 503 rather than letting the backend and Flink silently disagree about the
active settings. This is deliberately stricter than the read paths, which
degrade quietly: a lost *setting* is dangerous, a missing *reading* is not.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaError

from app.core.metrics import _metrics
from app.core.settings import Settings

logger = logging.getLogger(__name__)


class ConfigPublishError(RuntimeError):
    """Raised when a config.updates message cannot be published to Kafka."""


class ConfigProducer:
    """Publishes the merged user config to the config.updates topic."""

    def __init__(self) -> None:
        self._producer: Optional[AIOKafkaProducer] = None
        self._topic: str = ""
        # Lifecycle state surfaced by /health: stopped | disabled | degraded | available.
        self.status: str = "stopped"

    @property
    def available(self) -> bool:
        return self._producer is not None

    async def start(self, settings: Settings) -> None:
        """Start the producer; stay unavailable (no crash) if disabled or broker down."""
        if not settings.kafka_producer_enabled:
            logger.info("Kafka config producer disabled; POST /config will return 503.")
            self.status = "disabled"
            return

        self._topic = settings.kafka_topic_config
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=settings.kafka_producer_client_id,
        )
        try:
            await producer.start()
        except KafkaConnectionError as exc:
            await producer.stop()
            logger.warning(
                "Kafka unreachable; config producer not started (degraded mode): %s", exc
            )
            self.status = "degraded"
            return
        self._producer = producer
        self.status = "available"
        logger.info("Kafka config producer started on topic %s", self._topic)

    async def stop(self) -> None:
        if self._producer is None:
            return
        await self._producer.stop()
        self._producer = None
        self.status = "stopped"
        logger.info("Kafka config producer stopped.")

    async def publish(self, message: dict) -> None:
        """JSON-encode `message` and send it to config.updates, waiting for the broker ack.

        Raises ConfigPublishError when the producer is unavailable or the send fails,
        so the caller can surface a strict 503 without committing local state.
        """
        if self._producer is None:
            _metrics.record_config_publish_failure()
            raise ConfigPublishError("config producer unavailable (broker unreachable at startup)")
        value = json.dumps(message).encode("utf-8")
        try:
            await self._producer.send_and_wait(self._topic, value)
        except KafkaError as exc:
            _metrics.record_config_publish_failure()
            raise ConfigPublishError(f"failed to publish config.updates: {exc}") from exc
        _metrics.record_config_publish()
        logger.info("Published config.updates: %s", message)


# Module-level singleton — same pattern as _broadcaster (broadcaster.py) and
# _buffer (ring_buffer.py).
_producer = ConfigProducer()
