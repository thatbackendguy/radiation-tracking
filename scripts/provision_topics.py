"""
provision_topics.py — idempotently create the project's Kafka topics on any broker,
and (optionally) verify the broker's retention caps hold for every project topic.

Local topic creation is handled by the backend admin on startup
(backend/app/core/kafka_admin.py). This standalone script provisions any **remote** broker
(the DO droplet stack, or a managed cloud cluster per ADR-010) without booting the whole
backend: point it at the bootstrap server (SASL_SSL credentials in the environment if the
broker is secured) and it creates the five topics (radiation.raw/clean/aggregated/alerts,
config.updates), skipping any that already exist. Re-running is a no-op.

`--verify-retention` additionally asserts the W6 prod-ready caps (docker-compose.yml
`KAFKA_LOG_RETENTION_BYTES`/`_MS`) are effective on every project topic: each topic's
`retention.bytes` and `retention.ms` must be bounded (not -1) and within the caps read
from the same env vars, so the radiation.* logs can never fill the droplet's 160 GB disk.
Exit code is non-zero on any violation — usable as a droplet smoke check:

    pip install -r scripts/requirements.txt
    # export the broker env (see .env.example), then:
    python scripts/provision_topics.py --verify-retention

Cross-platform (pure Python, aiokafka — the same client the backend uses; no rpk/bash).
Credentials are read from the environment only — never pass them on the command line or
commit them (guidelines.md / DoD "no secrets in images").
"""

from __future__ import annotations

import argparse
import asyncio
import os
import ssl
import sys

try:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
    from aiokafka.errors import TopicAlreadyExistsError
except ImportError:  # pure helpers (check_retention) stay importable in client-less CI
    AIOKafkaAdminClient = NewTopic = ConfigResource = ConfigResourceType = None
    TopicAlreadyExistsError = Exception

TOPICS = [
    "radiation.raw",
    "radiation.clean",
    "radiation.aggregated",
    "radiation.alerts",
    "config.updates",
]

# Caps mirror the kafka service defaults in docker-compose.yml (1 GiB/partition + 24 h) so
# local and droplet runs verify against the same bound unless the env overrides both.
DEFAULT_MAX_RETENTION_BYTES = 1073741824
DEFAULT_MAX_RETENTION_MS = 86400000


def check_retention(topic: str, configs: dict, max_bytes: int, max_ms: int) -> list[str]:
    """Return human-readable violations of the retention caps for one topic's configs.

    ``configs`` maps config name → effective value as returned by DescribeConfigs (values
    arrive as strings). A cap is violated when the effective retention is missing,
    non-numeric, unbounded (< 0, Kafka's "keep forever"), or above the cap.
    """
    problems: list[str] = []
    for key, cap in (("retention.bytes", max_bytes), ("retention.ms", max_ms)):
        raw = configs.get(key)
        try:
            value = int(raw)
        except (TypeError, ValueError):
            problems.append(f"{topic}: {key} missing or non-numeric ({raw!r})")
            continue
        if value < 0:
            problems.append(f"{topic}: {key}={value} is unbounded — the log can fill the disk")
        elif value > cap:
            problems.append(f"{topic}: {key}={value} exceeds the cap of {cap}")
    return problems


def _admin_kwargs() -> dict:
    """Build AIOKafkaAdminClient kwargs from env (plaintext locally, SASL_SSL on cloud)."""
    kwargs: dict = {
        "bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "client_id": "topic-provisioner",
    }

    protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "").strip()
    if protocol and protocol.upper() != "PLAINTEXT":
        kwargs["security_protocol"] = protocol
        if "SSL" in protocol.upper():
            kwargs["ssl_context"] = ssl.create_default_context()
        if "SASL" in protocol.upper():
            kwargs["sasl_mechanism"] = os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN").strip()
            kwargs["sasl_plain_username"] = os.environ.get("KAFKA_SASL_USERNAME", "")
            kwargs["sasl_plain_password"] = os.environ.get("KAFKA_SASL_PASSWORD", "")

    return kwargs


async def _fetch_topic_configs(admin: AIOKafkaAdminClient, topics: list[str]) -> dict:
    """DescribeConfigs for the topics → {topic: {config name: effective value}}."""
    resources = [ConfigResource(ConfigResourceType.TOPIC, name) for name in topics]
    configs: dict = {}
    for response in await admin.describe_configs(resources):
        for resource in response.resources:
            # (error_code, error_message, resource_type, resource_name, config_entries);
            # every DescribeConfigs version leads each entry with (name, value, ...).
            error_code, _, _, resource_name, config_entries = resource[:5]
            if error_code != 0:
                raise RuntimeError(f"DescribeConfigs failed for {resource_name}: {resource[1]}")
            configs[resource_name] = {entry[0]: entry[1] for entry in config_entries}
    return configs


async def verify_retention(admin: AIOKafkaAdminClient) -> list[str]:
    """Check every project topic's effective retention against the compose caps."""
    max_bytes = int(os.environ.get("KAFKA_LOG_RETENTION_BYTES", DEFAULT_MAX_RETENTION_BYTES))
    max_ms = int(os.environ.get("KAFKA_LOG_RETENTION_MS", DEFAULT_MAX_RETENTION_MS))

    configs = await _fetch_topic_configs(admin, TOPICS)
    problems: list[str] = []
    for topic in TOPICS:
        topic_configs = configs.get(topic, {})
        print(
            f"{topic}: retention.bytes={topic_configs.get('retention.bytes')} "
            f"retention.ms={topic_configs.get('retention.ms')}"
        )
        problems.extend(check_retention(topic, topic_configs, max_bytes, max_ms))
    return problems


async def provision(with_retention_check: bool = False) -> None:
    """Create any missing project topics; optionally verify retention caps afterwards."""
    partitions = int(os.environ.get("KAFKA_NUM_PARTITIONS", "1"))
    replication = int(os.environ.get("KAFKA_REPLICATION_FACTOR", "1"))

    admin = AIOKafkaAdminClient(**_admin_kwargs())
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        to_create = [
            NewTopic(name=name, num_partitions=partitions, replication_factor=replication)
            for name in TOPICS
            if name not in existing
        ]
        if not to_create:
            print("All topics already exist; nothing to create.")
        else:
            try:
                await admin.create_topics(to_create)
                print("Created topics:", ", ".join(t.name for t in to_create))
            except TopicAlreadyExistsError:
                print("Some topics were created concurrently; continuing (idempotent).")

        if with_retention_check:
            problems = await verify_retention(admin)
            if problems:
                raise RuntimeError("retention caps violated:\n" + "\n".join(problems))
            print("Retention caps verified on all project topics.")
    finally:
        await admin.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify-retention",
        action="store_true",
        help="after provisioning, assert every project topic's retention.bytes/ms is "
        "bounded and within KAFKA_LOG_RETENTION_BYTES/_MS (non-zero exit on violation)",
    )
    args = parser.parse_args(argv)

    try:
        asyncio.run(provision(with_retention_check=args.verify_retention))
    except Exception as exc:  # surface a clean message instead of a traceback
        print(f"Topic provisioning failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
