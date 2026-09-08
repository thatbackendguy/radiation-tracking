"""Background consumer for radiation.clean / radiation.alerts.

Clean events feed the in-memory ring buffer (for /recent catch-up) and fan out
to live WebSocket clients via the broadcaster; alert events fan out only.

Mirrors the degraded-mode philosophy of kafka_admin.py: if the broker is
unreachable at startup the backend still serves (with an empty /recent)
instead of crash-looping in docker-compose.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Optional

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from pydantic import ValidationError

from app.core.broadcaster import Broadcaster
from app.core.metrics import STREAM_AGGREGATED, STREAM_ALERTS, STREAM_CLEAN, _metrics
from app.core.ring_buffer import RingBuffer
from app.core.settings import Settings
from app.models import RadiationEvent

logger = logging.getLogger(__name__)


@dataclass
class ConsumerHandle:
    consumer: AIOKafkaConsumer
    task: asyncio.Task


# Lifecycle state surfaced by /health: stopped | disabled | degraded | running.
# Written only by start_consumer/stop_consumer on the single event loop.
_status: str = "stopped"


def status() -> str:
    """Current lifecycle state of the background consumer (for /health)."""
    return _status


async def _handle_clean(msg, buffer: RingBuffer, broadcaster: Broadcaster) -> None:
    try:
        event = RadiationEvent.model_validate_json(msg.value)
    except ValidationError as exc:
        # Flink owns validation, so this should never fire — but a bad
        # event must not kill the consume loop.
        logger.warning("Skipping malformed radiation.clean event: %s", exc)
        _metrics.record_malformed(STREAM_CLEAN)
        return
    _metrics.record_consumed(STREAM_CLEAN)
    await buffer.append(event)
    # Typed envelope so the client can multiplex clean / alert / (later) aggregated
    # streams off one socket without inspecting payload shape.
    broadcaster.publish({"type": "clean", "data": event.model_dump(mode="json")})


def _handle_alert(msg, broadcaster: Broadcaster) -> None:
    # No locked alert schema yet (M3, Week 5) — forward best-effort as-is.
    try:
        payload = json.loads(msg.value)
    except (json.JSONDecodeError, TypeError) as exc:
        logger.warning("Skipping malformed radiation.alerts event: %s", exc)
        _metrics.record_malformed(STREAM_ALERTS)
        return
    _metrics.record_consumed(STREAM_ALERTS)
    broadcaster.publish({"type": "alert", "data": payload})


def _handle_aggregated(msg, broadcaster: Broadcaster) -> None:
    try:
        payload = json.loads(msg.value)
    except (json.JSONDecodeError, TypeError) as exc:
        logger.warning("Skipping malformed radiation.aggregated event: %s", exc)
        _metrics.record_malformed(STREAM_AGGREGATED)
        return
    _metrics.record_consumed(STREAM_AGGREGATED)
    broadcaster.publish({"type": "aggregated", "data": payload})


async def _consume_loop(
    consumer: AIOKafkaConsumer,
    buffer: RingBuffer,
    broadcaster: Broadcaster,
    settings: Settings,
) -> None:
    try:
        async for msg in consumer:
            if msg.topic == settings.kafka_topic_clean:
                await _handle_clean(msg, buffer, broadcaster)
            elif msg.topic == settings.kafka_topic_alerts:
                _handle_alert(msg, broadcaster)
            elif msg.topic == settings.kafka_topic_aggregated:
                _handle_aggregated(msg, broadcaster)
    except asyncio.CancelledError:
        logger.info("Consume loop cancelled; shutting down.")
        raise


async def start_consumer(
    settings: Settings, buffer: RingBuffer, broadcaster: Broadcaster
) -> Optional[ConsumerHandle]:
    """Subscribe to radiation.clean + radiation.alerts and fan events out.

    Returns None when disabled or the broker is unreachable (degraded mode).
    """
    global _status
    if not settings.kafka_consumer_enabled:
        logger.info("Kafka consumer disabled; /recent will serve an empty buffer.")
        _status = "disabled"
        return None

    consumer = AIOKafkaConsumer(
        settings.kafka_topic_clean,
        settings.kafka_topic_alerts,
        settings.kafka_topic_aggregated,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group_id,
        auto_offset_reset="latest",
    )
    try:
        await consumer.start()
    except KafkaConnectionError as exc:
        await consumer.stop()
        logger.warning("Kafka unreachable; consumer not started (degraded mode): %s", exc)
        _status = "degraded"
        return None

    _status = "running"
    task = asyncio.create_task(_consume_loop(consumer, buffer, broadcaster, settings))
    logger.info(
        "Kafka consumer started on topics %s, %s, %s",
        settings.kafka_topic_clean,
        settings.kafka_topic_alerts,
        settings.kafka_topic_aggregated,
    )
    return ConsumerHandle(consumer=consumer, task=task)


async def stop_consumer(handle: Optional[ConsumerHandle]) -> None:
    global _status
    if handle is None:
        return
    _status = "stopped"
    handle.task.cancel()
    try:
        await handle.task
    except asyncio.CancelledError:
        pass
    await handle.consumer.stop()
    logger.info("Kafka consumer stopped.")
