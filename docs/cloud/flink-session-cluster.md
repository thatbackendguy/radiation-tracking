# Flink session cluster on the cloud droplet (M2)

Status: Done (Week 7). The PyFlink pipeline runs on the single DigitalOcean droplet as part of
the root `docker-compose.yml` stack, with RocksDB-backed checkpoints persisted to a shared
Docker volume so the session cluster recovers keyed state across restarts.

## Goal

Run the same `radiation_tracking_job` as local compose, but on the DO droplet (8 GB RAM /
160 GB disk) with **durable checkpointing**, so a restarted JobManager or TaskManager recovers
state (per-geohash rolling series, sensor dedup, alert cooldown) from the last checkpoint
instead of replaying from the Kafka earliest offset.

## Topology (root docker-compose.yml)

The stack already runs a Flink 1.19 **session cluster** — `flink-jobmanager` +
`flink-taskmanager` (the project image with PyFlink + the Kafka connector + the operator
modules) + a one-shot `flink-job` submitter. Week-7 hardening for the droplet:

- **Resource caps**: `mem_limit` + `jobmanager/taskmanager.memory.process.size` on JM/TM,
  env-overridable (`FLINK_JM_*` / `FLINK_TM_*`), sized so Kafka(1.5g)+JM+TM+backend fit 8 GB.
  `restart: unless-stopped` so the cluster survives a droplet reboot during the soak.
- **Durable state**: a shared `flink-state` named volume mounted at `/opt/flink/state` in JM and
  TM; `state.savepoints.dir` defaults to `file:///opt/flink/state/savepoints`.

## Checkpointing config

`operators.checkpointing.checkpoint_settings_from_env` (applied by `apply_checkpointing`) reads
these. Local leaves them unset → 60s in-memory checkpoints, in-heap state (unchanged):

| Env var | Local default | Droplet |
|---------|---------------|---------|
| `FLINK_CHECKPOINT_INTERVAL_MS` | `60000` | keep or lower under load |
| `FLINK_CHECKPOINT_MIN_PAUSE_MS` | `5000` | keep |
| `FLINK_CHECKPOINT_TIMEOUT_MS` | `600000` | keep |
| `FLINK_CHECKPOINT_DIR` | *(unset → in-memory)* | `file:///opt/flink/state/checkpoints` |
| `FLINK_STATE_BACKEND` | *(unset → hashmap)* | `rocksdb` |
| `FLINK_SAVEPOINT_DIR` | `file:///opt/flink/state/savepoints` | same |

## Bring-up on the droplet

1. Provision the droplet, install Docker + Compose, clone the repo.
2. Create the droplet `.env` from `.env.example`; set
   `FLINK_CHECKPOINT_DIR=file:///opt/flink/state/checkpoints` and `FLINK_STATE_BACKEND=rocksdb`
   (+ any `KAFKA_*` if Kafka is external). **No secrets in git** (H4).
3. `docker compose up -d --build` — brings up Kafka, the Flink session cluster, backend,
   frontend, and submits the job. The shared `flink-state` volume persists checkpoints on the
   droplet disk.

## Savepoint / restart drill (checkpoint/restart deliverable)

Automated logical coverage (CI, no MiniCluster):
`flink-jobs/tests/integration/test_checkpoint_restart.py` proves the rolling-stats keyed state
(a JSON cpm_avg series in ValueState) round-trips a checkpoint→restore and resumes identically;
`tests/operators/test_checkpointing.py` covers the env gating.

On the droplet (manual drill for the failure-mode demo):

- Trigger a savepoint: `docker compose exec flink-jobmanager flink savepoint <jobId>`
  (writes to `state.savepoints.dir` on the shared volume).
- Cancel with savepoint: `... flink cancel -s <jobId>`.
- Restore: `... flink run -s <savepointPath> -d -pym radiation_tracking_job`.
- Restart recovery: `docker compose restart flink-taskmanager` — the job recovers keyed state
  from the last checkpoint on the `flink-state` volume (no reprocessing from earliest).

See `docs/decisions/ADR-015-cloud-flink-checkpointing.md` for the state-backend / storage
rationale.
