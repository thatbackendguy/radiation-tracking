"""Unit tests for the area/timespan region filter.

The pure predicates (in_area / in_timespan / event_passes) are driven directly. The
broadcast operator is tested with a dict-backed fake MapState (the Flink runtime is not
available in CI), covering the pass-through default, area/timespan filtering, the clear
path, and the poison-pill guard — same approach as test_classifier.py.
"""

from __future__ import annotations

import json

from operators.region_filter import (
    _AREA_KEY,
    _TIMESPAN_KEY,
    RegionFilterFunction,
    event_passes,
    in_area,
    in_timespan,
)

# Tokyo-ish bbox and a one-day event-time window.
_AREA = {"min_lat": 35.0, "max_lat": 38.0, "min_lon": 139.0, "max_lon": 141.0}
_TIMESPAN = {"start": "2026-06-19 00:00:00", "end": "2026-06-20 00:00:00"}


def _event(lat=35.6895, lon=139.6917, captured_at="2026-06-19 01:00:00") -> dict:
    return {"sensor_id": "5", "latitude": lat, "longitude": lon, "captured_at": captured_at}


class TestInArea:
    def test_inside_inclusive_bounds(self) -> None:
        assert in_area(35.0, 139.0, _AREA) is True  # corners are inclusive
        assert in_area(36.5, 140.0, _AREA) is True

    def test_outside_each_edge(self) -> None:
        assert in_area(34.9, 140.0, _AREA) is False  # below min_lat
        assert in_area(38.1, 140.0, _AREA) is False  # above max_lat
        assert in_area(36.0, 138.9, _AREA) is False  # west of min_lon
        assert in_area(36.0, 141.1, _AREA) is False  # east of max_lon


class TestInTimespan:
    def test_start_inclusive_end_exclusive(self) -> None:
        assert in_timespan("2026-06-19 00:00:00", _TIMESPAN) is True  # start inclusive
        assert in_timespan("2026-06-19 12:00:00", _TIMESPAN) is True
        assert in_timespan("2026-06-20 00:00:00", _TIMESPAN) is False  # end exclusive
        assert in_timespan("2026-06-18 23:59:59", _TIMESPAN) is False


class TestEventPasses:
    def test_no_filter_passes_everything(self) -> None:
        assert event_passes(_event(lat=0.0, lon=0.0), None, None) is True

    def test_area_only(self) -> None:
        assert event_passes(_event(), _AREA, None) is True
        assert event_passes(_event(lat=10.0, lon=10.0), _AREA, None) is False

    def test_timespan_only(self) -> None:
        assert event_passes(_event(captured_at="2026-06-19 06:00:00"), None, _TIMESPAN) is True
        assert event_passes(_event(captured_at="2026-07-01 06:00:00"), None, _TIMESPAN) is False

    def test_both_must_hold(self) -> None:
        # In area but outside timespan → dropped.
        assert event_passes(_event(captured_at="2026-07-01 06:00:00"), _AREA, _TIMESPAN) is False
        # In timespan but outside area → dropped.
        assert event_passes(_event(lat=10.0, lon=10.0), _AREA, _TIMESPAN) is False
        # Both hold → passes.
        assert event_passes(_event(), _AREA, _TIMESPAN) is True


class _FakeMapState:
    """Stand-in for pyflink broadcast MapState (get/put/contains surface)."""

    def __init__(self) -> None:
        self._d: dict[str, str] = {}

    def contains(self, key: str) -> bool:
        return key in self._d

    def get(self, key: str) -> str:
        return self._d[key]

    def put(self, key: str, value: str) -> None:
        self._d[key] = value


class _FakeContext:
    def __init__(self, state: _FakeMapState) -> None:
        self._state = state

    def get_broadcast_state(self, descriptor) -> _FakeMapState:
        return self._state


def _emit(op: RegionFilterFunction, ctx: _FakeContext, event: dict) -> list[dict]:
    return list(op.process_element(event, ctx))


def _config(op: RegionFilterFunction, ctx: _FakeContext, raw: str) -> None:
    list(op.process_broadcast_element(raw, ctx))


class TestRegionFilterOperator:
    def test_empty_state_passes_through(self) -> None:
        op, ctx = RegionFilterFunction(), _FakeContext(_FakeMapState())
        assert _emit(op, ctx, _event(lat=0.0, lon=0.0)) == [_event(lat=0.0, lon=0.0)]

    def test_area_config_filters(self) -> None:
        op, ctx = RegionFilterFunction(), _FakeContext(_FakeMapState())
        _config(
            op,
            ctx,
            json.dumps({"cpm_warn_threshold": 100, "cpm_danger_threshold": 1000, "area": _AREA}),
        )
        assert _emit(op, ctx, _event()) == [_event()]  # inside
        assert _emit(op, ctx, _event(lat=10.0, lon=10.0)) == []  # outside

    def test_timespan_config_filters(self) -> None:
        op, ctx = RegionFilterFunction(), _FakeContext(_FakeMapState())
        _config(
            op,
            ctx,
            json.dumps(
                {"cpm_warn_threshold": 100, "cpm_danger_threshold": 1000, "timespan": _TIMESPAN}
            ),
        )
        assert _emit(op, ctx, _event(captured_at="2026-06-19 06:00:00")) != []
        assert _emit(op, ctx, _event(captured_at="2026-07-01 06:00:00")) == []

    def test_new_config_without_area_clears_filter(self) -> None:
        op, ctx = RegionFilterFunction(), _FakeContext(_FakeMapState())
        _config(
            op,
            ctx,
            json.dumps({"cpm_warn_threshold": 100, "cpm_danger_threshold": 1000, "area": _AREA}),
        )
        assert _emit(op, ctx, _event(lat=10.0, lon=10.0)) == []  # filtered out
        _config(op, ctx, json.dumps({"cpm_warn_threshold": 100, "cpm_danger_threshold": 1000}))
        assert _emit(op, ctx, _event(lat=10.0, lon=10.0)) != []  # filter cleared → passes

    def test_poison_pill_keeps_last_good_filter(self) -> None:
        op, ctx = RegionFilterFunction(), _FakeContext(_FakeMapState())
        _config(
            op,
            ctx,
            json.dumps({"cpm_warn_threshold": 100, "cpm_danger_threshold": 1000, "area": _AREA}),
        )
        _config(op, ctx, "{garbage")  # must not raise, must not wipe state
        assert _emit(op, ctx, _event(lat=10.0, lon=10.0)) == []  # area still enforced
        assert ctx._state.get(_AREA_KEY) == json.dumps(_AREA)
        assert json.loads(ctx._state.get(_TIMESPAN_KEY)) is None

    def test_empty_area_or_timespan_object_is_no_filter(self) -> None:
        # {"area": {}} passes deserialize_config (from_dict treats {} as None), but storing
        # the raw {} would KeyError in in_area/in_timespan and crash-loop the task. It must
        # be coerced to None (no constraint) so the branches pass everything through.
        op, ctx = RegionFilterFunction(), _FakeContext(_FakeMapState())
        _config(
            op,
            ctx,
            json.dumps(
                {
                    "cpm_warn_threshold": 100,
                    "cpm_danger_threshold": 1000,
                    "area": {},
                    "timespan": {},
                }
            ),
        )
        assert _emit(op, ctx, _event(lat=10.0, lon=10.0)) != []  # passes through, no KeyError
        assert json.loads(ctx._state.get(_AREA_KEY)) is None
        assert json.loads(ctx._state.get(_TIMESPAN_KEY)) is None
