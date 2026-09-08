"""Unit tests for the pure sustained-high alert reducer (no PyFlink runtime)."""

from __future__ import annotations

from operators.alert_detection import REASON_SUSTAINED_HIGH, detect_sustained_high

_WIN_START = "2026-06-19T01:00:00+00:00"
_WIN_END = "2026-06-19T01:05:00+00:00"


def _event(
    *,
    classification: str = "DANGER",
    cpm: float | None = 1200.0,
    captured_at: str = "2026-06-19T01:00:30",
    lat: float = 37.42,
    lon: float = 141.03,
) -> dict:
    return {
        "sensor_id": "sensor-9",
        "captured_at": captured_at,
        "latitude": lat,
        "longitude": lon,
        "cpm": cpm,
        "classification": classification,
    }


def _detect(events: list[dict], min_breaches: int = 3) -> dict | None:
    return detect_sustained_high(
        events, "sensor-9", _WIN_START, _WIN_END, min_breaches=min_breaches
    )


class TestDetectSustainedHigh:
    def test_fires_when_breaches_meet_threshold(self) -> None:
        alert = _detect([_event(), _event(), _event()], min_breaches=3)
        assert alert is not None
        assert alert["sensor_id"] == "sensor-9"
        assert alert["reason"] == REASON_SUSTAINED_HIGH
        assert alert["classification"] == "DANGER"
        assert alert["breach_count"] == 3
        assert alert["window_start"] == _WIN_START
        assert alert["window_end"] == _WIN_END

    def test_no_alert_below_threshold(self) -> None:
        # Two DANGER readings, threshold 3 — a transient spike, not sustained.
        assert _detect([_event(), _event()], min_breaches=3) is None

    def test_no_alert_without_danger(self) -> None:
        safe = [_event(classification="SAFE", cpm=40.0) for _ in range(5)]
        warn = [_event(classification="WARN", cpm=150.0) for _ in range(5)]
        assert _detect(safe + warn) is None

    def test_only_danger_readings_count_as_breaches(self) -> None:
        events = [
            _event(classification="SAFE", cpm=40.0),
            _event(classification="WARN", cpm=150.0),
            _event(classification="DANGER", cpm=1100.0),
            _event(classification="DANGER", cpm=1200.0),
            _event(classification="DANGER", cpm=1300.0),
        ]
        alert = _detect(events, min_breaches=3)
        assert alert is not None
        assert alert["breach_count"] == 3

    def test_peak_cpm_is_reported(self) -> None:
        events = [
            _event(cpm=1100.0),
            _event(cpm=2500.0),
            _event(cpm=1300.0),
        ]
        alert = _detect(events, min_breaches=3)
        assert alert is not None
        assert alert["cpm"] == 2500.0

    def test_location_is_from_latest_breach(self) -> None:
        events = [
            _event(captured_at="2026-06-19T01:00:10", lat=37.0, lon=141.0),
            _event(captured_at="2026-06-19T01:04:50", lat=37.9, lon=141.9),
            _event(captured_at="2026-06-19T01:02:00", lat=37.5, lon=141.5),
        ]
        alert = _detect(events, min_breaches=3)
        assert alert is not None
        # Latest by captured_at is the 01:04:50 reading.
        assert alert["latitude"] == 37.9
        assert alert["longitude"] == 141.9
        assert alert["triggered_at"] == "2026-06-19T01:04:50"

    def test_exactly_at_threshold_fires(self) -> None:
        assert _detect([_event(), _event()], min_breaches=2) is not None
