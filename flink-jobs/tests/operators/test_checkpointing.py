"""Unit tests for the pure checkpoint settings reader (no Flink runtime)."""

from __future__ import annotations

from operators.checkpointing import checkpoint_settings_from_env

_CP_VARS = (
    "FLINK_CHECKPOINT_INTERVAL_MS",
    "FLINK_CHECKPOINT_MIN_PAUSE_MS",
    "FLINK_CHECKPOINT_TIMEOUT_MS",
    "FLINK_CHECKPOINT_DIR",
    "FLINK_STATE_BACKEND",
)


def _clear(monkeypatch) -> None:
    for var in _CP_VARS:
        monkeypatch.delenv(var, raising=False)


class TestCheckpointSettingsFromEnv:
    def test_defaults_when_unset(self, monkeypatch) -> None:
        _clear(monkeypatch)
        assert checkpoint_settings_from_env() == {
            "interval_ms": 60000,
            "min_pause_ms": 5000,
            "timeout_ms": 600000,
            "checkpoint_dir": None,
            "state_backend": None,
        }

    def test_numeric_overrides(self, monkeypatch) -> None:
        _clear(monkeypatch)
        monkeypatch.setenv("FLINK_CHECKPOINT_INTERVAL_MS", "30000")
        monkeypatch.setenv("FLINK_CHECKPOINT_MIN_PAUSE_MS", "1000")
        monkeypatch.setenv("FLINK_CHECKPOINT_TIMEOUT_MS", "120000")
        settings = checkpoint_settings_from_env()
        assert settings["interval_ms"] == 30000
        assert settings["min_pause_ms"] == 1000
        assert settings["timeout_ms"] == 120000

    def test_checkpoint_dir_passthrough(self, monkeypatch) -> None:
        _clear(monkeypatch)
        monkeypatch.setenv("FLINK_CHECKPOINT_DIR", "file:///opt/flink/state/checkpoints")
        settings = checkpoint_settings_from_env()
        assert settings["checkpoint_dir"] == "file:///opt/flink/state/checkpoints"

    def test_blank_checkpoint_dir_is_none(self, monkeypatch) -> None:
        _clear(monkeypatch)
        monkeypatch.setenv("FLINK_CHECKPOINT_DIR", "   ")
        assert checkpoint_settings_from_env()["checkpoint_dir"] is None

    def test_state_backend_normalised(self, monkeypatch) -> None:
        _clear(monkeypatch)
        monkeypatch.setenv("FLINK_STATE_BACKEND", "RocksDB")
        assert checkpoint_settings_from_env()["state_backend"] == "rocksdb"

    def test_blank_state_backend_is_none(self, monkeypatch) -> None:
        _clear(monkeypatch)
        monkeypatch.setenv("FLINK_STATE_BACKEND", "")
        assert checkpoint_settings_from_env()["state_backend"] is None
