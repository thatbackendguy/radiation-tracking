"""Unit tests for SensorDedupOperator. The Flink runtime is not available in CI, so the
keyed MapState is replaced with a dict-backed fake and the operator's process_element is
driven directly. Because the live stream is keyed by sensor_id, each sensor owns a
separate MapState — the tests mirror that by giving each simulated sensor its own state.
"""

from __future__ import annotations

from operators.sensor_dedup import SensorDedupOperator, dedup_key


class _FakeMapState:
    """Stand-in for pyflink MapState: just the contains/put surface the operator uses."""

    def __init__(self) -> None:
        self._d: dict[str, bool] = {}

    def contains(self, key: str) -> bool:
        return key in self._d

    def put(self, key: str, value: bool) -> None:
        self._d[key] = value


def _run(op: SensorDedupOperator, event: dict) -> list[dict]:
    """Drive process_element and collect what it emits (empty list == dropped)."""
    return list(op.process_element(event, ctx=None))


def _event(
    sensor_id: str, *, md5sum: str | None = None, captured_at: str = "2026-06-19T01:00:00"
) -> dict:
    event: dict = {"sensor_id": sensor_id, "captured_at": captured_at}
    if md5sum is not None:
        event["md5sum"] = md5sum
    return event


class TestDedupKey:
    def test_prefers_md5sum(self) -> None:
        assert dedup_key(_event("5", md5sum="abc123")) == "abc123"

    def test_falls_back_to_captured_at_when_md5sum_absent(self) -> None:
        assert dedup_key(_event("5", captured_at="2026-06-19T01:00:00")) == (
            "captured_at:2026-06-19T01:00:00"
        )

    def test_empty_md5sum_falls_back(self) -> None:
        assert dedup_key(_event("5", md5sum="")) == "captured_at:2026-06-19T01:00:00"


class TestProcessElement:
    def _operator(self) -> SensorDedupOperator:
        op = SensorDedupOperator()
        op._seen = _FakeMapState()
        return op

    def test_first_occurrence_passes(self) -> None:
        op = self._operator()
        assert _run(op, _event("5", md5sum="abc")) == [_event("5", md5sum="abc")]

    def test_identical_reading_is_dropped(self) -> None:
        op = self._operator()
        first = _run(op, _event("5", md5sum="abc"))
        second = _run(op, _event("5", md5sum="abc"))
        assert first != []
        assert second == []

    def test_distinct_md5sums_both_pass(self) -> None:
        op = self._operator()
        assert _run(op, _event("5", md5sum="abc")) != []
        assert _run(op, _event("5", md5sum="def")) != []

    def test_distinct_sensors_are_independent(self) -> None:
        # In Flink each sensor_id keys its own MapState, so the same fallback key
        # (shared captured_at, no md5sum) must pass for each sensor independently.
        op_a = self._operator()
        op_b = self._operator()
        assert _run(op_a, _event("A", captured_at="2026-06-19T01:00:00")) != []
        assert _run(op_b, _event("B", captured_at="2026-06-19T01:00:00")) != []

    def test_missing_md5sum_dedups_on_captured_at(self) -> None:
        op = self._operator()
        assert _run(op, _event("5", captured_at="2026-06-19T01:00:00")) != []
        # same sensor, same timestamp, still no md5sum → duplicate
        assert _run(op, _event("5", captured_at="2026-06-19T01:00:00")) == []
        # same sensor, different timestamp → distinct reading
        assert _run(op, _event("5", captured_at="2026-06-19T02:00:00")) != []
