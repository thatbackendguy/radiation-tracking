"""Kafka → Postgres consume loop for the aggregated sink (ADR-018).

At-least-once end to end: Kafka offsets are committed only *after* the Postgres
batch commits, and the upsert is idempotent, so a crash between the two replays
the batch onto the same rows (effectively-once for the table).
"""

from __future__ import annotations

import json
import logging
import signal

from .config import Config
from .db import build_rows, connect_with_retry, ensure_schema, upsert_batch

logger = logging.getLogger(__name__)


def _decode(values) -> list[dict]:
    """JSON-decode a batch of raw Kafka message values, skipping bad ones."""
    records = []
    for raw in values:
        try:
            records.append(json.loads(raw))
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning("Skipping non-JSON aggregated message: %s", exc)
    return records


def process_batch(consumer, conn, config: Config) -> int:
    """Poll one batch, upsert it, then advance Kafka offsets. Returns rows written.

    Commit ordering is the at-least-once contract: the Kafka offset commit happens
    only after the Postgres batch has committed, so a crash in between simply
    replays the batch onto the same (idempotent) rows.
    """
    batch = consumer.poll(
        timeout_ms=int(config.batch_timeout_s * 1000),
        max_records=config.batch_size,
    )
    values = [msg.value for records in batch.values() for msg in records]
    if not values:
        return 0

    rows = build_rows(_decode(values))
    written = upsert_batch(conn, rows)
    consumer.commit()  # DB is durable → safe to advance offsets
    return written


def run(config: Config, *, _consumer=None, _conn=None) -> None:
    """Consume radiation.aggregated and upsert into Postgres until interrupted.

    ``_consumer`` / ``_conn`` are injectable for tests; in production they are
    built from ``config``.
    """
    conn = _conn or connect_with_retry(config.dsn, config.connect_retries, config.connect_backoff_s)
    ensure_schema(conn)

    consumer = _consumer
    if consumer is None:
        # Imported lazily so tests can inject a fake without kafka-python installed.
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            config.topic,
            bootstrap_servers=config.bootstrap_servers,
            group_id=config.group_id,
            enable_auto_commit=False,  # commit only after the DB batch is durable
            auto_offset_reset="earliest",
            value_deserializer=lambda b: b.decode("utf-8"),
            max_poll_records=config.batch_size,
        )

    stopping = {"flag": False}

    def _stop(*_args):
        logger.info("Shutdown signal received; draining and exiting.")
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    total = 0
    logger.info(
        "aggregated_sink started: topic=%s group=%s batch=%d",
        config.topic,
        config.group_id,
        config.batch_size,
    )
    try:
        while not stopping["flag"]:
            written = process_batch(consumer, conn, config)
            if written:
                total += written
                logger.info("Upserted %d rows (total %d).", written, total)
    finally:
        try:
            consumer.close()
        finally:
            conn.close()
        logger.info("aggregated_sink stopped after %d rows.", total)
