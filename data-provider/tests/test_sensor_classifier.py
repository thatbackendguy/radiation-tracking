"""
Tests for data_provider/sensor_classifier.py

Run:
    pytest data_provider/tests/test_sensor_classifier.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from sensor_classifier import (  # noqa: E402
    FIXED,
    MOBILE,
    SensorClassifier,
    haversine_m,
)

# Tokyo-ish reference point used across the movement cases.
LAT0, LON0 = 35.6895, 139.6917


def test_haversine_known_distance():
    # One degree of latitude is ~111 km anywhere on Earth.
    d = haversine_m(0.0, 0.0, 1.0, 0.0)
    assert 110_500 < d < 111_500


def test_haversine_zero_for_same_point():
    assert haversine_m(LAT0, LON0, LAT0, LON0) == pytest.approx(0.0, abs=1e-6)


def test_first_reading_is_fixed():
    c = SensorClassifier()
    assert c.classify("s1", LAT0, LON0) == FIXED
    assert c.sensors_seen == 1


def test_stationary_sensor_stays_fixed():
    c = SensorClassifier()
    for _ in range(10):
        assert c.classify("s1", LAT0, LON0) == FIXED
    assert c.mobile_count() == 0


def test_gps_jitter_within_threshold_stays_fixed():
    # ~0.0005 deg latitude ≈ 55 m — jitter well under the 200 m default.
    c = SensorClassifier(movement_threshold_m=200.0)
    assert c.classify("s1", LAT0, LON0) == FIXED
    assert c.classify("s1", LAT0 + 0.0005, LON0) == FIXED
    assert c.classify("s1", LAT0, LON0 + 0.0005) == FIXED
    assert c.classify("s1", LAT0 - 0.0003, LON0 - 0.0003) == FIXED
    assert c.mobile_count() == 0


def test_moving_sensor_becomes_mobile():
    c = SensorClassifier(movement_threshold_m=200.0)
    assert c.classify("s1", LAT0, LON0) == FIXED
    # ~0.01 deg latitude ≈ 1.1 km — far beyond the threshold.
    assert c.classify("s1", LAT0 + 0.01, LON0) == MOBILE
    assert c.mobile_count() == 1


def test_mobile_label_is_sticky():
    c = SensorClassifier(movement_threshold_m=200.0)
    c.classify("s1", LAT0, LON0)
    assert c.classify("s1", LAT0 + 0.01, LON0) == MOBILE
    # Even if it returns to the origin, it stays mobile.
    assert c.classify("s1", LAT0, LON0) == MOBILE
    assert c.classify("s1", LAT0, LON0) == MOBILE


def test_sensors_are_independent():
    c = SensorClassifier(movement_threshold_m=200.0)
    c.classify("fixed-sensor", LAT0, LON0)
    c.classify("fixed-sensor", LAT0 + 0.0002, LON0)  # jitter → stays fixed

    c.classify("mobile-sensor", LAT0, LON0)
    assert c.classify("mobile-sensor", LAT0 + 0.02, LON0) == MOBILE

    # The mobile sensor moving must not flip the fixed one.
    assert c.classify("fixed-sensor", LAT0, LON0 + 0.0002) == FIXED
    assert c.sensors_seen == 2
    assert c.mobile_count() == 1


def test_threshold_is_configurable():
    # A step of ~0.001 deg (~111 m) is mobile under a tight 50 m threshold
    # but fixed under the 200 m default.
    tight = SensorClassifier(movement_threshold_m=50.0)
    tight.classify("s1", LAT0, LON0)
    assert tight.classify("s1", LAT0 + 0.001, LON0) == MOBILE

    loose = SensorClassifier(movement_threshold_m=200.0)
    loose.classify("s1", LAT0, LON0)
    assert loose.classify("s1", LAT0 + 0.001, LON0) == FIXED


def test_span_uses_full_bounding_box_not_consecutive_gap():
    # Small consecutive steps that never individually exceed the threshold
    # must still trip mobile once their *cumulative* spread does.
    c = SensorClassifier(movement_threshold_m=200.0)
    assert c.classify("s1", LAT0, LON0) == FIXED
    assert c.classify("s1", LAT0 + 0.0009, LON0) == FIXED  # ~100 m from origin
    # Another ~100 m step: consecutive gap ~100 m, but total span ~200 m+.
    assert c.classify("s1", LAT0 + 0.0019, LON0) == MOBILE


def test_non_positive_threshold_rejected():
    with pytest.raises(ValueError):
        SensorClassifier(movement_threshold_m=0.0)
    with pytest.raises(ValueError):
        SensorClassifier(movement_threshold_m=-1.0)
