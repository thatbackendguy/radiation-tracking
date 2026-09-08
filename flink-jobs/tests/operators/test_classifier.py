"""Unit tests for ThresholdClassifierFunction (broadcast operator).

The Flink runtime is not available in CI, so the broadcast MapState is replaced with
a dict-backed fake and the operator's process_element / process_broadcast_element are
driven directly. Covers the default-fallback read path and the poison-pill guard
(a malformed config.updates message must not crash the task or wipe last-good state).
"""

from __future__ import annotations

import json

from operators.classification import (
    DEFAULT_DANGER_THRESHOLD,
    DEFAULT_WARN_THRESHOLD,
)
from operators.classifier import (
    _DANGER_KEY,
    _WARN_KEY,
    ThresholdClassifierFunction,
)


class _FakeMapState:
    """Stand-in for pyflink MapState: the get/put/contains surface the operator uses."""

    def __init__(self) -> None:
        self._d: dict[str, str] = {}

    def contains(self, key: str) -> bool:
        return key in self._d

    def get(self, key: str) -> str:
        return self._d[key]

    def put(self, key: str, value: str) -> None:
        self._d[key] = value


class _FakeContext:
    """Stand-in for the broadcast (Read)Context: hands back one shared MapState."""

    def __init__(self, state: _FakeMapState) -> None:
        self._state = state

    def get_broadcast_state(self, descriptor) -> _FakeMapState:
        return self._state


def _classify(op: ThresholdClassifierFunction, ctx: _FakeContext, cpm) -> str | None:
    [out] = list(op.process_element({"cpm": cpm}, ctx))
    return out["classification"]


def _broadcast(op: ThresholdClassifierFunction, ctx: _FakeContext, raw: str) -> None:
    # process_broadcast_element returns an (empty) iterator; drain it to run the body.
    list(op.process_broadcast_element(raw, ctx))


class TestDefaultFallback:
    def test_empty_state_uses_classification_defaults(self) -> None:
        op = ThresholdClassifierFunction()
        ctx = _FakeContext(_FakeMapState())
        assert _classify(op, ctx, DEFAULT_WARN_THRESHOLD - 1) == "SAFE"
        assert _classify(op, ctx, DEFAULT_WARN_THRESHOLD) == "WARN"
        assert _classify(op, ctx, DEFAULT_DANGER_THRESHOLD) == "DANGER"

    def test_null_cpm_unclassified(self) -> None:
        op = ThresholdClassifierFunction()
        ctx = _FakeContext(_FakeMapState())
        assert _classify(op, ctx, None) is None


class TestBroadcastReconfig:
    def test_valid_config_overrides_defaults(self) -> None:
        op = ThresholdClassifierFunction()
        ctx = _FakeContext(_FakeMapState())
        # Under defaults 45 CPM is SAFE; after lowering warn to 40 it becomes WARN.
        assert _classify(op, ctx, 45) == "SAFE"
        _broadcast(op, ctx, json.dumps({"cpm_warn_threshold": 40, "cpm_danger_threshold": 500}))
        assert _classify(op, ctx, 45) == "WARN"
        assert _classify(op, ctx, 500) == "DANGER"


class TestPoisonPillGuard:
    def test_malformed_json_does_not_raise(self) -> None:
        op = ThresholdClassifierFunction()
        ctx = _FakeContext(_FakeMapState())
        _broadcast(op, ctx, "not-json{")  # must not raise
        # state untouched → still on defaults
        assert _classify(op, ctx, DEFAULT_WARN_THRESHOLD - 1) == "SAFE"

    def test_missing_threshold_key_does_not_raise(self) -> None:
        op = ThresholdClassifierFunction()
        ctx = _FakeContext(_FakeMapState())
        _broadcast(op, ctx, json.dumps({"cpm_warn_threshold": 40}))  # danger missing
        assert _classify(op, ctx, 45) == "SAFE"  # default warn=100 still applies

    def test_bad_message_keeps_last_good_thresholds(self) -> None:
        op = ThresholdClassifierFunction()
        ctx = _FakeContext(_FakeMapState())
        _broadcast(op, ctx, json.dumps({"cpm_warn_threshold": 40, "cpm_danger_threshold": 500}))
        assert _classify(op, ctx, 45) == "WARN"
        _broadcast(op, ctx, "{garbage")  # poison pill after a good config
        assert _classify(op, ctx, 45) == "WARN"  # last-good warn=40 retained, not reset
        assert ctx._state.get(_WARN_KEY) == "40.0"
        assert ctx._state.get(_DANGER_KEY) == "500.0"
