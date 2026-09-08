"""
checkpointing.py — checkpoint configuration for the radiation tracking job.

Interval / pause / timeout / durable storage / state backend are all env-driven so the same
job runs unchanged locally (60s in-memory checkpoints, in-heap state) and on the DO droplet
(RocksDB-backed checkpoints on the shared flink-state volume). ``checkpoint_settings_from_env``
is a pure reader (no pyflink import) so it is unit-tested directly; ``apply_checkpointing`` is
the thin wrapper that applies the settings to a StreamExecutionEnvironment (pyflink imported
lazily, same pure-helper / thin-wrapper split as the operators). See docs/decisions/ADR-015.
"""

from __future__ import annotations

import os
from typing import Optional, TypedDict

_DEFAULT_INTERVAL_MS = 60000
_DEFAULT_MIN_PAUSE_MS = 5000
_DEFAULT_TIMEOUT_MS = 600000


class CheckpointSettings(TypedDict):
    interval_ms: int
    min_pause_ms: int
    timeout_ms: int
    checkpoint_dir: Optional[str]
    state_backend: Optional[str]


def checkpoint_settings_from_env() -> CheckpointSettings:
    """Resolve checkpoint settings from the environment (pure — no pyflink import).

    Unset env reproduces the original ``enable_checkpointing(60_000)`` behaviour: a 60s
    interval, in-memory JobManager checkpoint storage, and the in-heap hashmap state backend.
    ``checkpoint_dir`` / ``state_backend`` are None when their vars are unset or blank, so the
    caller skips durable storage / RocksDB locally.
    """
    return {
        "interval_ms": int(
            os.environ.get("FLINK_CHECKPOINT_INTERVAL_MS", str(_DEFAULT_INTERVAL_MS))
        ),
        "min_pause_ms": int(
            os.environ.get("FLINK_CHECKPOINT_MIN_PAUSE_MS", str(_DEFAULT_MIN_PAUSE_MS))
        ),
        "timeout_ms": int(os.environ.get("FLINK_CHECKPOINT_TIMEOUT_MS", str(_DEFAULT_TIMEOUT_MS))),
        "checkpoint_dir": os.environ.get("FLINK_CHECKPOINT_DIR", "").strip() or None,
        "state_backend": os.environ.get("FLINK_STATE_BACKEND", "").strip().lower() or None,
    }


def apply_checkpointing(env, settings: Optional[CheckpointSettings] = None) -> None:
    """Apply checkpoint settings to a StreamExecutionEnvironment (pyflink imported lazily).

    Defaults to ``checkpoint_settings_from_env()``. Durable checkpoint storage and the RocksDB
    backend are only enabled when their env vars are set, so a bare local run keeps the 60s
    in-memory checkpoints and in-heap state.
    """
    if settings is None:
        settings = checkpoint_settings_from_env()

    env.enable_checkpointing(settings["interval_ms"])

    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_min_pause_between_checkpoints(settings["min_pause_ms"])
    checkpoint_config.set_checkpoint_timeout(settings["timeout_ms"])

    if settings["checkpoint_dir"]:
        checkpoint_config.set_checkpoint_storage_dir(settings["checkpoint_dir"])

    if settings["state_backend"] == "rocksdb":
        from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend

        env.set_state_backend(EmbeddedRocksDBStateBackend())
