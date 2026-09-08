"""Unit tests for AlertCooldownOperator. The Flink runtime is not available in CI, so the
keyed ValueState is replaced with a simple fake and process_element is driven directly.
Because the live stream is keyed by sensor_id, each sensor owns a separate ValueState —
the tests mirror that by giving each simulated sensor its own state.
"""

from __future__ import annotations

from operators.alert_dedup import (
    AlertCooldownOperator,
    alert_window_end_millis,
    is_within_cooldown,
)


class _FakeValueState:
    """Stand-in for pyflink ValueState: just the value/update surface the operator uses."""

    def __init__(self) -> None:
        self._v = None

    def value(self):
        return self._v

    def update(self, value) -> None:
        self._v = value


def _run(op: AlertCooldownOperator, alert: dict) -> list[dict]:
    """Drive process_element and collect what it emits (empty list == suppressed)."""
    return list(op.process_element(alert, ctx=None))


def _alert(window_end: str, *, sensor_id: str = "sensor-9") -> dict:
    return {
        "sensor_id": sensor_id,
        "reason": "sustained-high",
        "window_end": window_end,
        "breach_count": 3,
    }


class TestAlertWindowEndMillis:
    def test_parses_iso_window_end(self) -> None:
        a = alert_window_end_millis(_alert("2026-06-19T01:05:00+00:00"))
        b = alert_window_end_millis(_alert("2026-06-19T01:06:00+00:00"))
        assert b - a == 60_000


class TestIsWithinCooldown:
    def test_inside_window_is_suppressed(self) -> None:
        assert is_within_cooldown(0, 60_000, cooldown_seconds=600) is True

    def test_at_boundary_is_not_suppressed(self) -> None:
        # Exactly cooldown_seconds later → outside the cooldown (strict <).
        assert is_within_cooldown(0, 600_000, cooldown_seconds=600) is False

    def test_earlier_candidate_is_suppressed(self) -> None:
        # A late candidate describing an already-reported (earlier) burst.
        assert is_within_cooldown(600_000, 120_000, cooldown_seconds=600) is True


class TestProcessElement:
    def _operator(self) -> AlertCooldownOperator:
        op = AlertCooldownOperator(cooldown_seconds=600)
        op._last_emitted_end = _FakeValueState()
        return op

    def test_first_alert_passes(self) -> None:
        op = self._operator()
        assert _run(op, _alert("2026-06-19T01:05:00+00:00")) != []

    def test_overlapping_alert_within_cooldown_is_suppressed(self) -> None:
        op = self._operator()
        first = _run(op, _alert("2026-06-19T01:05:00+00:00"))
        # Next sliding window ends one minute later — still within the 10 min cooldown.
        second = _run(op, _alert("2026-06-19T01:06:00+00:00"))
        assert first != []
        assert second == []

    def test_alert_after_cooldown_passes(self) -> None:
        op = self._operator()
        assert _run(op, _alert("2026-06-19T01:05:00+00:00")) != []
        # A fresh burst 11 minutes later (past the 10 min cooldown) is a new alert.
        assert _run(op, _alert("2026-06-19T01:16:00+00:00")) != []

    def test_distinct_sensors_are_independent(self) -> None:
        # In Flink each sensor_id keys its own ValueState, so one sensor's cooldown
        # never suppresses another's alert.
        op_a = self._operator()
        op_b = self._operator()
        assert _run(op_a, _alert("2026-06-19T01:05:00+00:00", sensor_id="A")) != []
        assert _run(op_b, _alert("2026-06-19T01:05:00+00:00", sensor_id="B")) != []
