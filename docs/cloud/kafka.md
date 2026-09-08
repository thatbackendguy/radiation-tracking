# Cloud Kafka — provisioning & secure connection

This is the **Week-6 cloud kickoff for the Kafka tier (M3)**: stand up a managed broker,
create the project topics, and connect every service to it over `SASL_SSL` with no secrets
committed or baked into images. See [ADR-010](../decisions/ADR-010-cloud-kafka.md) for the
decision record.

## 1. Provider

Use a managed, serverless broker free tier — no VM to operate, and it idles cheaply between
demos (matches the "scale down off-hours to preserve credits" risk in `approach.md`):

- **Redpanda Serverless** (Kafka-API compatible) — recommended, or
- **Confluent Cloud Basic** free tier.

Create a cluster, then an API key/secret pair. Note the **bootstrap server**
(`<host>:9092`), the **API key** (username) and **secret** (password). The SASL mechanism is
`PLAIN` for both providers.

## 2. Configure the environment

Copy `.env.example` to `.env` (gitignored) and fill the cloud block — **never commit real
values**:

```
KAFKA_BOOTSTRAP_SERVERS=<cluster-host>:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=<api-key>
KAFKA_SASL_PASSWORD=<api-secret>
KAFKA_NUM_PARTITIONS=1          # raise for cloud throughput
KAFKA_REPLICATION_FACTOR=3      # managed clusters are multi-broker
```

These variables are read **today** by:
- the **Flink job** — `flink-jobs/operators/kafka_config.py::apply_security` applies them to
  every `KafkaSource`/`KafkaSink`. Blank/`PLAINTEXT` ⇒ no-op (local default unchanged).
- **`scripts/provision_topics.py`** — see below.

The **producer (M1)** and **backend (M4)** Kafka clients are **not yet SASL-wired** — a
follow-up will have them read the same vars so the whole stack authenticates end-to-end.
Until then, only the Flink job and the provisioner connect to a secured cloud broker.

## 3. Create the topics

Cross-platform, no `rpk`/bash required:

```
pip install -r scripts/requirements.txt
python scripts/provision_topics.py
```

This idempotently creates `radiation.raw`, `radiation.clean`, `radiation.aggregated`,
`radiation.alerts`, `config.updates` (skipping any that exist). Locally the backend admin
(`backend/app/core/kafka_admin.py`) already does this on startup; the script is the
no-backend path for provisioning the cloud cluster.

**On the DO droplet (W7):** run it with `--verify-retention` after the stack is up to
assert the prod-ready retention caps from `docker-compose.yml` are effective on every
project topic — each topic's `retention.bytes`/`retention.ms` must be bounded and within
`KAFKA_LOG_RETENTION_BYTES`/`KAFKA_LOG_RETENTION_MS`, so the radiation.* logs can never
fill the droplet's 160 GB disk. The script exits non-zero on any violation:

```
python scripts/provision_topics.py --verify-retention
```

## 4. Smoke test

With the cloud env exported, run the producer (a small slice) → Flink → and confirm a record
lands on `radiation.alerts` on the cloud cluster (provider console → topic → messages, or a
console consumer with the same SASL config). That satisfies the Week-6 deliverable "at least
one service reachable on a cloud endpoint".

## 5. Security posture

- **No secrets in git or images.** Credentials live only in `.env` (gitignored; `.env.*`
  excluded except `.env.example`). The Dockerfiles copy code, not env files.
- **TLS + auth in transit:** `SASL_SSL` (vs the local plaintext broker, which is for dev
  only and never exposed publicly).
- **Least privilege:** scope the API key to the project topics where the provider supports
  ACLs; rotate the key after the demo.
- **Scaling note:** all Flink operators are keyed (sensor_id / geohash), so raising
  `parallelism.default` (via `FLINK_PROPERTIES`) and topic partitions rescales keyed state
  cleanly via key groups — see ADR-010.
