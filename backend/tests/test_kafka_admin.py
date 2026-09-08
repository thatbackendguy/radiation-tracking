import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from aiokafka.errors import KafkaConnectionError, TopicAlreadyExistsError

from app.core.kafka_admin import _project_topics, ensure_topics
from app.core.settings import Settings

EXPECTED_TOPICS = {
    "radiation.raw",
    "radiation.clean",
    "radiation.aggregated",
    "radiation.alerts",
    "config.updates",
}


def make_settings(**overrides) -> Settings:
    return Settings(**overrides)


def _admin_mock(list_topics_return=None, create_side_effect=None, start_side_effect=None):
    admin = MagicMock()
    admin.start = AsyncMock(side_effect=start_side_effect)
    admin.close = AsyncMock()
    admin.list_topics = AsyncMock(return_value=list_topics_return or [])
    admin.create_topics = AsyncMock(side_effect=create_side_effect)
    return admin


def test_project_topics_returns_all_five_with_configured_partitions():
    settings = make_settings(kafka_num_partitions=3, kafka_replication_factor=2)
    topics = _project_topics(settings)

    assert {t.name for t in topics} == EXPECTED_TOPICS
    assert all(t.num_partitions == 3 for t in topics)
    assert all(t.replication_factor == 2 for t in topics)


def test_creates_missing_topics_when_broker_reachable():
    settings = make_settings()
    admin = _admin_mock(list_topics_return=[])

    with patch("app.core.kafka_admin.AIOKafkaAdminClient", return_value=admin):
        asyncio.run(ensure_topics(settings))

    admin.start.assert_awaited_once()
    admin.create_topics.assert_awaited_once()
    created = {t.name for t in admin.create_topics.call_args.args[0]}
    assert created == EXPECTED_TOPICS
    admin.close.assert_awaited_once()


def test_idempotent_when_all_topics_already_exist():
    settings = make_settings()
    admin = _admin_mock(list_topics_return=list(EXPECTED_TOPICS))

    with patch("app.core.kafka_admin.AIOKafkaAdminClient", return_value=admin):
        asyncio.run(ensure_topics(settings))

    admin.create_topics.assert_not_awaited()
    admin.close.assert_awaited_once()


def test_swallows_topic_already_exists_on_concurrent_create():
    settings = make_settings()
    admin = _admin_mock(list_topics_return=[], create_side_effect=TopicAlreadyExistsError())

    with patch("app.core.kafka_admin.AIOKafkaAdminClient", return_value=admin):
        asyncio.run(ensure_topics(settings))  # must not raise

    admin.close.assert_awaited_once()


def test_degraded_mode_when_broker_unreachable():
    settings = make_settings(kafka_admin_retry_attempts=3, kafka_admin_retry_backoff_seconds=0.0)
    admin = _admin_mock(start_side_effect=KafkaConnectionError("boom"))

    with patch("app.core.kafka_admin.AIOKafkaAdminClient", return_value=admin), patch(
        "app.core.kafka_admin.asyncio.sleep", new=AsyncMock()
    ):
        asyncio.run(ensure_topics(settings))  # must not raise

    assert admin.start.await_count == 3
    admin.create_topics.assert_not_awaited()


def test_skips_creation_when_disabled():
    settings = make_settings(kafka_topic_init_enabled=False)

    with patch("app.core.kafka_admin.AIOKafkaAdminClient") as admin_cls:
        asyncio.run(ensure_topics(settings))

    admin_cls.assert_not_called()
