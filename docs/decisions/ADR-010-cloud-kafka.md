# ADR-010: cloud Kafka — managed serverless broker, env-driven SASL_SSL, idempotent provisioning

Date: 2026-06-30
Status: Accepted
Authors: M3 (Yash)

---

## Context

The Week-6 plan (approach.md) assigns M3, after the trend detector:

> Then: provision cloud Kafka (Confluent/Redpanda free tier or VM), topics created.

The compressed close (cloud pulled into W6, finalised W7) needs the Kafka tier reachable on
a cloud endpoint so the other services can point at it. Locally the broker is an unauthenticated
plaintext Kafka in `docker-compose.yml` (`kafka:9092`) — fine for dev, unacceptable for a
publicly reachable cloud broker. The change must (a) not regress the local plaintext path,
(b) keep credentials out of git and images (DoD: "no secrets in images"), and (c) be
provisionable cross-platform (3 macOS / 2 Windows members; no `rpk`/bash-only steps).

The backend already creates the five topics idempotently on startup
(`backend/app/core/kafka_admin.py`, aiokafka), but that requires booting the backend and
does not yet pass SASL.

## Decision

**Provider — managed serverless broker free tier (Redpanda Serverless or Confluent Cloud
Basic), not a self-run VM.** No broker to operate or patch, idles cheaply between demos
(mitigates the "credits exhaust before soak" risk), and both speak the Kafka API with
`SASL_SSL` + `PLAIN` (API key/secret). A VM running Redpanda is the fallback if a free tier
is unavailable.

**Connection security — env-driven, applied uniformly, no-op by default.** A single helper
`flink-jobs/operators/kafka_config.py::apply_security` reads `KAFKA_SECURITY_PROTOCOL`,
`KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` and sets the matching
Kafka client properties on every `KafkaSource`/`KafkaSink` builder in the job. With nothing
set (or `PLAINTEXT`) it returns no properties, so `docker compose` is byte-for-byte
unchanged. The pure `kafka_security_properties` is unit-tested without a broker.

**Credentials — environment only.** Supplied via a gitignored `.env` (`.env.*` excluded
except `.env.example`, which carries placeholders). Never committed, never copied into a
Docker image (images contain code only). Documented in `.env.example` and
`docs/cloud/kafka.md`.

**Topic provisioning — `scripts/provision_topics.py`, standalone and idempotent.** A pure
Python script (aiokafka, the same client the backend uses, pinned in `scripts/requirements.txt`)
creates the five topics on any broker, honouring the same SASL_SSL env. It is the
no-backend path for the cloud cluster; locally the backend admin still does it on startup.
Cross-platform — no `rpk`, no shell.

**Scaling posture.** All Flink operators are keyed (sensor_id / geohash), so raising
`parallelism.default` (via `FLINK_PROPERTIES`) and the topic partition count rescales keyed
state cleanly via Flink key groups. Recorded so the W7 cloud finalize can scale without a
code change.

## Consequences

- The same job/producer/backend run locally (plaintext) and on cloud (SASL_SSL) by changing
  env only — no code branches, no second build.
- Credentials never enter version control or images; the security review item in W7 (M4)
  inherits a clean posture for the Kafka tier.
- The Flink-side helper is small and tested; the provisioning script is independent of the
  backend so the cloud cluster can be prepared before any service is deployed.
- `replication_factor`/partitions are env-tunable, so the same script fits a 1-broker dev
  cluster and a 3-broker managed cloud.

## Alternatives considered

- **Self-managed Kafka/Redpanda on a VM:** full control but ongoing ops (patching, disk,
  TLS certs) and idle cost — overkill for a demo. A managed free tier removes all of it.
- **Hardcode SASL in the builders / a cloud-only compose file:** would fork the local and
  cloud code paths and risk committing secrets. The env-driven no-op helper keeps one path.
- **Extend the backend admin to provision cloud topics:** viable, but couples cloud
  provisioning to booting the backend and edits M4's module; the standalone script is
  lighter and runs before any deploy. The backend admin still owns local startup creation.
- **`rpk`/CLI or Terraform for topics:** `rpk` is a bash-friendly binary (not cross-platform
  for the Windows members without extra setup); Terraform is heavier than five topics
  warrant. A pinned Python script matches the team's existing toolchain.
