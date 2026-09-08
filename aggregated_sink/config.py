"""Environment-driven configuration for the aggregated Postgres sink."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    bootstrap_servers: str
    topic: str
    group_id: str
    dsn: str
    batch_size: int
    batch_timeout_s: float
    connect_retries: int
    connect_backoff_s: float
    log_level: str

    @classmethod
    def from_env(cls) -> "Config":
        # A full DATABASE_URL wins; otherwise assemble a libpq DSN from parts so
        # the compose service and a bare local run share one code path.
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            host = os.getenv("POSTGRES_HOST", "postgres")
            port = os.getenv("POSTGRES_PORT", "5432")
            db = os.getenv("POSTGRES_DB", "radiation")
            user = os.getenv("POSTGRES_USER", "radiation")
            password = os.getenv("POSTGRES_PASSWORD", "radiation")
            dsn = f"host={host} port={port} dbname={db} user={user} password={password}"

        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            topic=os.getenv("AGG_SINK_TOPIC", "radiation.aggregated"),
            group_id=os.getenv("AGG_SINK_GROUP_ID", "aggregated-postgres-sink"),
            dsn=dsn,
            # Upsert in batches to keep the DB round-trips down under the 100x replay.
            batch_size=int(os.getenv("AGG_SINK_BATCH_SIZE", "200")),
            batch_timeout_s=float(os.getenv("AGG_SINK_BATCH_TIMEOUT_S", "2.0")),
            connect_retries=int(os.getenv("AGG_SINK_CONNECT_RETRIES", "30")),
            connect_backoff_s=float(os.getenv("AGG_SINK_CONNECT_BACKOFF_S", "2.0")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
