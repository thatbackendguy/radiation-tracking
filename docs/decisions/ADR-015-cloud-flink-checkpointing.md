# ADR-015: cloud Flink checkpointing — RocksDB + durable volume on the DO droplet

Date: 2026-07-05
Status: Accepted
Authors: M2 (Sahil)

---

## Context

The Week-7 plan (approach.md) assigns M2:

> Finish the PyFlink cloud session cluster (checkpointing config); then checkpoint/restart test
> + savepoint docs.

The team deploys the whole stack as one `docker-compose` on a single DigitalOcean droplet
(guidelines §12; M3's ADR-010 + `chore(kafka): make broker container prod-ready for the DO
droplet`). The Flink pipeline holds meaningful keyed state — sensor dedup (md5 + TTL, ADR-003),
per-geohash rolling cpm series (ADR-009), alert cooldown (ADR-005) — that must survive a
TaskManager/JobManager restart during the 24-hour soak rather than replaying from the Kafka
earliest offset. Locally the job used a 60s in-memory checkpoint with the in-heap hashmap
backend; that neither persists nor scales.

## Decision

**State backend — RocksDB on the droplet, hashmap locally, env-gated.**
`FLINK_STATE_BACKEND=rocksdb` selects `EmbeddedRocksDBStateBackend` on the droplet (spillable,
bounds heap as the keyed state grows); unset locally keeps the in-heap hashmap, so
`docker compose up` on a laptop is unchanged. No extra jar — RocksDB ships in flink-dist 1.19.

**Checkpoint storage — a shared Docker volume, not object storage.** The cluster is a single
droplet, so `FLINK_CHECKPOINT_DIR=file:///opt/flink/state/checkpoints` on a `flink-state` named
volume mounted into both JM and TM is durable enough and needs no cloud bucket or credentials
(H4). `state.savepoints.dir` shares the same volume. Object storage (`gs://…`/`s3://…`) is the
path only if the cluster ever spans nodes; recorded as the multi-node fallback.

**Delivery guarantee — keep AT_LEAST_ONCE.** The Kafka sinks are already AT_LEAST_ONCE (the
backend deduplicates); checkpointing persists state without upgrading to EXACTLY_ONCE / Kafka
transactions, which would add latency and coordinator load for no gain here.

**Config as a pure, tested reader.** `operators/checkpointing.py::checkpoint_settings_from_env`
resolves all knobs from env with the previous defaults and is unit-tested without pyflink;
`apply_checkpointing` is the thin env wrapper. This mirrors the pure-helper / thin-wrapper split
used across the operators and keeps the "no-op when unset" guarantee under test.

## Consequences

- One job runs locally (in-memory, hashmap) and on the droplet (durable, RocksDB) by changing
  env only — no code branch, no second image (parity with ADR-010's Kafka posture).
- A TaskManager/JobManager restart recovers keyed state from the last checkpoint on the volume;
  the failure-mode demo (drop a TaskManager) becomes a `docker compose restart`.
- Checkpoint/restart is covered logically in CI (no MiniCluster) via the JSON round-trip of the
  rolling-stats state; the manual droplet drill is documented for the live demo.
- Single-droplet `file://` state is not shared across nodes — scaling out requires object
  storage (noted as the fallback).

## Alternatives considered

- **Object storage (`gs://…`/`s3://…`) for checkpoints:** the "proper" multi-node choice, but
  adds a bucket + credentials to manage for a single-droplet demo. A local volume is durable
  across container restarts, which is what the soak needs.
- **EXACTLY_ONCE with Kafka transactions:** stronger guarantee, but the backend already dedupes
  and transactions add latency/complexity — not warranted.
- **A separate cloud-only compose file:** would fork the local and cloud paths (ADR-010 rejected
  this too); the env-gated single root compose keeps one path.
