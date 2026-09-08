from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_raw: str = "radiation.raw"
    kafka_topic_clean: str = "radiation.clean"
    kafka_topic_aggregated: str = "radiation.aggregated"
    kafka_topic_alerts: str = "radiation.alerts"
    kafka_topic_config: str = "config.updates"

    # Kafka topic creation (admin client, run on startup)
    kafka_num_partitions: int = 1
    kafka_replication_factor: int = 1
    kafka_topic_init_enabled: bool = True
    kafka_admin_retry_attempts: int = 10
    kafka_admin_retry_backoff_seconds: float = 3.0

    # Kafka consumer (radiation.clean -> ring buffer)
    kafka_consumer_group_id: str = "backend-consumer"
    kafka_consumer_enabled: bool = True

    # Kafka producer (config.updates — user filter settings -> Flink broadcast state)
    kafka_producer_enabled: bool = True
    kafka_producer_client_id: str = "backend-config-producer"

    # Ring buffer / /recent endpoint
    recent_buffer_size: int = 500
    recent_default_limit: int = 50
    recent_max_limit: int = 500

    # WebSocket /ws/stream — backpressure-aware fan-out.
    # Per-client FIFO cap for non-coalesced messages (alerts); coalesced clean /
    # aggregated state is bounded by key count via broadcaster_coalesced_maxsize.
    broadcaster_queue_maxsize: int = 100
    # Per-client cap on the coalescing lane, counted in distinct keys (sensors /
    # geo buckets). Bounds worst-case memory for a stalled client to one entry per
    # key up to this ceiling; past it the least-recently-updated key's pending
    # state is evicted (re-sent when that key next updates). Generous — only a
    # pathological cardinality explosion should reach it.
    broadcaster_coalesced_maxsize: int = 5000
    # Throttle window (ms): a slow client is flushed at most once per interval, so
    # coalescing accumulates within each window and send cadence stays bounded.
    broadcaster_flush_interval_ms: int = 100

    # Thresholds (CPM — counts per minute)
    cpm_warn_threshold: float = 100.0
    cpm_danger_threshold: float = 1000.0

    # Postgres history read (ADR-019) — the aggregated-blob archive written by the
    # aggregated_sink service (ADR-018). Read-only from the backend; powers the
    # /history/* insight endpoints. Disabled/degraded-safe: if the pool can't be
    # created the rest of the backend is unaffected and /history returns 503.
    history_enabled: bool = True
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "radiation"
    postgres_user: str = "radiation"
    postgres_password: str = "radiation"
    history_pool_min_size: int = 1
    history_pool_max_size: int = 4
    history_connect_timeout_seconds: float = 5.0
    # Retried at startup (Postgres may still be initializing when the backend's
    # `depends_on: postgres: condition: service_started` is satisfied — that
    # condition intentionally doesn't wait for a healthcheck, so the backend must
    # tolerate a cold-start race instead of staying degraded for the container's
    # whole lifetime).
    history_connect_retry_attempts: int = 5
    history_connect_retry_backoff_seconds: float = 2.0
    # Safety cap on rows any single /history query can return.
    history_max_rows: int = 5000

    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # Service
    backend_host: str = "0.0.0.0"
    backend_port: int = 8000
    # Deny cross-origin by default (secure-by-default): a bare backend allows no
    # browser origin until CORS_ORIGINS is set. docker-compose sets `*` for local
    # dev; the droplet .env restricts it to the public frontend URL.
    cors_origins: str = ""
    # Shared secret gating mutating config writes (POST /config). Empty = gate
    # disabled (local dev, unchanged behaviour). When set (droplet .env), every
    # POST /config must carry a matching X-Config-Token header. See ADR-013.
    config_write_token: str = ""

    def allowed_origins(self) -> list[str]:
        """Parsed CORS allowlist, reused for the WebSocket origin check."""
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


settings = Settings()
