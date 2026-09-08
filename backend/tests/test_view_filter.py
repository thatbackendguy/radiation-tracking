"""Unit tests for the per-client view filter (ADR-017)."""

from datetime import datetime, timezone

import pytest

from app.core.view_filter import ViewFilter


def _area(**overrides):
    bounds = {"min_lat": 30.0, "max_lat": 40.0, "min_lon": 130.0, "max_lon": 145.0}
    bounds.update(overrides)
    return bounds


class TestViewFilterValidation:
    def test_noop_filter(self):
        view = ViewFilter()
        assert view.is_noop is True

    def test_partial_area_rejected(self):
        with pytest.raises(ValueError, match="area filter requires all"):
            ViewFilter(min_lat=1.0)

    def test_inverted_latitude_rejected(self):
        with pytest.raises(ValueError, match="min_lat < max_lat"):
            ViewFilter(min_lat=40.0, max_lat=30.0, min_lon=130.0, max_lon=145.0)

    def test_out_of_range_longitude_rejected(self):
        with pytest.raises(ValueError, match="min_lon < max_lon"):
            ViewFilter(min_lat=30.0, max_lat=40.0, min_lon=130.0, max_lon=181.0)

    def test_partial_timespan_rejected(self):
        with pytest.raises(ValueError, match="both start and end"):
            ViewFilter(start=datetime(2026, 1, 1, tzinfo=timezone.utc))

    def test_inverted_timespan_rejected(self):
        with pytest.raises(ValueError, match="start must be before end"):
            ViewFilter(
                start=datetime(2026, 2, 1, tzinfo=timezone.utc),
                end=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )

    def test_naive_and_aware_bounds_mix_safely(self):
        # Naive datetimes are treated as UTC, so comparing them cannot raise.
        view = ViewFilter(start=datetime(2026, 1, 1), end=datetime(2026, 2, 1))
        assert view.start.tzinfo is not None and view.end.tzinfo is not None


class TestFromSubscribe:
    def test_full_payload(self):
        view = ViewFilter.from_subscribe(
            {
                "type": "subscribe",
                "area": _area(),
                "timespan": {"start": "2026-01-01T00:00:00Z", "end": "2026-02-01T00:00:00Z"},
            }
        )
        assert view.has_area and view.has_timespan

    def test_empty_payload_is_noop(self):
        assert ViewFilter.from_subscribe({"type": "subscribe"}).is_noop is True

    def test_null_area_and_timespan_clear_rules(self):
        view = ViewFilter.from_subscribe({"type": "subscribe", "area": None, "timespan": None})
        assert view.is_noop is True

    def test_non_numeric_bound_rejected(self):
        with pytest.raises(ValueError, match="min_lat must be a number"):
            ViewFilter.from_subscribe({"area": _area(min_lat="nope")})

    def test_bad_timestamp_rejected(self):
        with pytest.raises(ValueError, match="ISO-8601"):
            ViewFilter.from_subscribe({"timespan": {"start": "yesterday", "end": "tomorrow"}})

    def test_non_object_area_rejected(self):
        with pytest.raises(ValueError, match="must be objects"):
            ViewFilter.from_subscribe({"area": [1, 2, 3]})


class TestMatching:
    view = ViewFilter(
        min_lat=30.0,
        max_lat=40.0,
        min_lon=130.0,
        max_lon=145.0,
        start=datetime(2026, 6, 1, tzinfo=timezone.utc),
        end=datetime(2026, 7, 1, tzinfo=timezone.utc),
    )

    def test_event_inside_view_matches(self, make_event):
        event = make_event(latitude=35.0, longitude=139.0, captured_at="2026-06-11T10:00:00Z")
        assert self.view.matches_event(event) is True

    def test_event_outside_area_rejected(self, make_event):
        event = make_event(latitude=50.0, longitude=139.0, captured_at="2026-06-11T10:00:00Z")
        assert self.view.matches_event(event) is False

    def test_event_outside_timespan_rejected(self, make_event):
        event = make_event(latitude=35.0, longitude=139.0, captured_at="2026-08-01T00:00:00Z")
        assert self.view.matches_event(event) is False

    def test_clean_message_matching(self):
        inside = {
            "type": "clean",
            "data": {"latitude": 35.0, "longitude": 139.0, "captured_at": "2026-06-11T10:00:00Z"},
        }
        outside = {
            "type": "clean",
            "data": {"latitude": 5.0, "longitude": 139.0, "captured_at": "2026-06-11T10:00:00Z"},
        }
        assert self.view.matches_message(inside) is True
        assert self.view.matches_message(outside) is False

    def test_aggregated_message_uses_centroid_and_window(self):
        message = {
            "type": "aggregated",
            "data": {
                "centroid_latitude": 35.0,
                "centroid_longitude": 139.0,
                "window_start": "2026-06-11T10:00:00Z",
            },
        }
        assert self.view.matches_message(message) is True
        message["data"]["centroid_latitude"] = 80.0
        assert self.view.matches_message(message) is False

    def test_alert_falls_back_to_triggered_at(self):
        message = {
            "type": "alert",
            "data": {
                "latitude": 35.0,
                "longitude": 139.0,
                "triggered_at": "2026-08-11T10:00:00Z",
            },
        }
        assert self.view.matches_message(message) is False

    def test_missing_fields_fail_open(self):
        # An alert without coordinates or timestamps must never be hidden.
        assert self.view.matches_message({"type": "alert", "data": {"reason": "trend"}}) is True

    def test_unknown_type_and_bad_data_fail_open(self):
        assert self.view.matches_message({"type": "heartbeat"}) is True
        assert self.view.matches_message({"type": "clean", "data": None}) is True

    def test_unparseable_timestamp_fails_open(self):
        message = {
            "type": "clean",
            "data": {"latitude": 35.0, "longitude": 139.0, "captured_at": "not-a-date"},
        }
        assert self.view.matches_message(message) is True
