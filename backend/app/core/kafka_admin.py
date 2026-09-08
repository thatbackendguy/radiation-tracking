"""Kafka admin client — idempotent topic creation on backend startup.

The backend owns creation of all project Kafka topics (see guidelines.md §7.2)
so the Data Provider (M1) and Flink jobs (M2/M3) can assume their topics exist.
Topic creation is idempotent: re-running against a cluster that already has the
topics is a no-op, and an unreachable broker degrades gracefully rather than
crashing the service.
"""

from __future__ import annotations

import asyncio
import logging

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError, TopicAlreadyExistsError

from app.core.settings import Settings

logger = logging.getLogger(__name__)


def _project_topics(settings: Settings) -> list[NewTopic]:
    """Build NewTopic specs for every topic the backend is responsible for."""
    names = [
        settings.kafka_topic_raw,
        settings.kafka_topic_clean,
        settings.kafka_topic_aggregated,
        settings.kafka_topic_alerts,
        settings.kafka_topic_config,
    ]
    return [
        NewTopic(
            name=name,
            num_partitions=settings.kafka_num_partitions,
            replication_factor=settings.kafka_replication_factor,
        )
        for name in names
    ]


async def _connect_admin(settings: Settings) -> AIOKafkaAdminClient | None:
    """Start an admin client, retrying with backoff until the broker is reachable.

    Returns a started client, or None once all retries are exhausted (degraded mode).
    """
    for attempt in range(1, settings.kafka_admin_retry_attempts + 1):
        admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
        try:
            await admin.start()
            return admin
        except KafkaConnectionError as exc:
            await admin.close()
            logger.warning(
                "Kafka admin connection attempt %d/%d failed: %s",
                attempt,
                settings.kafka_admin_retry_attempts,
                exc,
            )
            if attempt < settings.kafka_admin_retry_attempts:
                await asyncio.sleep(settings.kafka_admin_retry_backoff_seconds)
    return None


async def ensure_topics(settings: Settings) -> None:
    """Idempotently create all project topics. Safe to call on every startup."""
    if not settings.kafka_topic_init_enabled:
        logger.info("Kafka topic init disabled; skipping topic creation.")
        return

    admin = await _connect_admin(settings)
    if admin is None:
        logger.warning(
            "Kafka unreachable after %d attempts; starting in degraded mode "
            "without ensuring topics exist.",
            settings.kafka_admin_retry_attempts,
        )
        return

    try:
        existing = set(await admin.list_topics())
        to_create = [t for t in _project_topics(settings) if t.name not in existing]
        if not to_create:
            logger.info("All Kafka topics already exist; nothing to create.")
            return
        try:
            await admin.create_topics(to_create)
            logger.info("Created Kafka topics: %s", ", ".join(t.name for t in to_create))
        except TopicAlreadyExistsError:
            # Raced with another service creating the same topics — idempotent, fine.
            logger.info("Some topics already existed (created concurrently); continuing.")
    finally:
        await admin.close()
