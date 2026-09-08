import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiokafka.errors import KafkaConnectionError

from app.core.broadcaster import Broadcaster
from app.core.kafka_consumer import (
    ConsumerHandle,
    _consume_loop,
    start_consumer,
    stop_consumer,
)
from app.core.ring_buffer import RingBuffer
from app.core.settings import Settings

CLEAN_TOPIC = "radiation.clean"

TEST_SETTINGS = Settings(
    kafka_topic_clean=CLEAN_TOPIC,
    kafka_topic_alerts="radiation.alerts",
    kafka_topic_aggregated="radiation.aggregated",
)

VALID_EVENT = {
    "sensor_id": "sensor-1",
    "captured_at": "2026-06-11T10:00:00Z",
    "uploaded_at": "2026-06-11T10:05:00Z",
    "latitude": 35.0,
    "longitude": 139.0,
    "cpm": 42.0,
    "classification": "WARN",
}


def _broadcaster() -> Broadcaster:
    return Broadcaster(queue_maxsize=10, flush_interval_ms=0)


class FakeConsumer:
    """Async-iterable stand-in for AIOKafkaConsumer yielding fixed messages."""

    def __init__(self, values, topic=CLEAN_TOPIC):
        self._values = list(values)
        self._topic = topic
        self.stop = AsyncMock()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._values:
            raise StopAsyncIteration
        return SimpleNamespace(value=self._values.pop(0), topic=self._topic)


def test_consume_loop_appends_valid_events():
    buffer = RingBuffer(maxsize=10)
    broadcaster = _broadcaster()
    consumer = FakeConsumer([json.dumps(VALID_EVENT).encode()])

    async def scenario():
        # Subscribe inside the loop: the subscriber's Event binds to the running loop.
        subscriber = broadcaster.subscribe()
        await _consume_loop(consumer, buffer, broadcaster, TEST_SETTINGS)
        return subscriber.drain()[0]

    message = asyncio.run(scenario())

    assert len(buffer) == 1
    events = asyncio.run(buffer.recent(1))
    assert events[0].sensor_id == "sensor-1"
    assert events[0].classification == "WARN"
    # Clean events also fan out in a typed envelope.
    assert message["type"] == "clean"
    assert message["data"]["sensor_id"] == "sensor-1"


def test_consume_loop_skips_malformed_events_without_raising():
    buffer = RingBuffer(maxsize=10)
    broadcaster = _broadcaster()
    consumer = FakeConsumer(
        [
            b"not json at all",
            json.dumps({"sensor_id": "x"}).encode(),  # missing required fields
            json.dumps(VALID_EVENT).encode(),
        ]
    )

    asyncio.run(_consume_loop(consumer, buffer, broadcaster, TEST_SETTINGS))  # must not raise

    assert len(buffer) == 1


def test_consume_loop_fans_out_alerts_without_buffering():
    buffer = RingBuffer(maxsize=10)
    broadcaster = _broadcaster()
    alert = {"sensor_id": "sensor-9", "cpm": 5000.0, "reason": "sustained-high"}
    consumer = FakeConsumer([json.dumps(alert).encode()], topic="radiation.alerts")

    async def scenario():
        subscriber = broadcaster.subscribe()
        await _consume_loop(consumer, buffer, broadcaster, TEST_SETTINGS)
        return subscriber.drain()[0]

    message = asyncio.run(scenario())

    # Alerts fan out but are not added to the ring buffer (that backs /recent).
    assert len(buffer) == 0
    assert message["type"] == "alert"
    assert message["data"]["sensor_id"] == "sensor-9"


def test_consume_loop_exits_cleanly_on_cancel():
    buffer = RingBuffer(maxsize=10)

    class BlockingConsumer:
        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.sleep(3600)

    async def scenario():
        task = asyncio.create_task(
            _consume_loop(BlockingConsumer(), buffer, _broadcaster(), TEST_SETTINGS)
        )
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return True
        return False

    assert asyncio.run(scenario()) is True


def test_start_consumer_returns_none_when_disabled():
    settings = Settings(kafka_consumer_enabled=False)
    buffer = RingBuffer(maxsize=10)

    with patch("app.core.kafka_consumer.AIOKafkaConsumer") as consumer_cls:
        result = asyncio.run(start_consumer(settings, buffer, _broadcaster()))

    assert result is None
    consumer_cls.assert_not_called()


def test_start_consumer_degrades_when_broker_unreachable():
    settings = Settings(kafka_consumer_enabled=True)
    buffer = RingBuffer(maxsize=10)
    consumer = AsyncMock()
    consumer.start.side_effect = KafkaConnectionError("boom")

    with patch("app.core.kafka_consumer.AIOKafkaConsumer", return_value=consumer):
        result = asyncio.run(start_consumer(settings, buffer, _broadcaster()))

    assert result is None
    consumer.stop.assert_awaited_once()


def test_stop_consumer_cancels_task_and_stops_client():
    consumer = FakeConsumer([])

    async def scenario():
        task = asyncio.create_task(asyncio.sleep(3600))
        handle = ConsumerHandle(consumer=consumer, task=task)
        await stop_consumer(handle)
        return task.cancelled()

    assert asyncio.run(scenario()) is True
    consumer.stop.assert_awaited_once()


def test_stop_consumer_accepts_none_handle():
    asyncio.run(stop_consumer(None))  # degraded mode — must be a no-op
