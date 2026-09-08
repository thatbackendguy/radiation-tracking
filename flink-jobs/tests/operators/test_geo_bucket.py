"""Unit tests for the geohash bucketing helpers and enricher. These are pure (pygeohash
only, no PyFlink runtime), so the enricher's map() is driven directly like the other
operator tests.
"""

from __future__ import annotations

from operators.geo_bucket import GeoBucketEnricher, geo_bucket, with_geo_bucket

# Tokyo (Shibuya). pygeohash encodes this to the "xn774…" cell; the precision-5 prefix
# is stable, so it doubles as a regression check on the encoding contract.
_TOKYO_LAT = 35.6895
_TOKYO_LON = 139.6917


def _event(lat: float = _TOKYO_LAT, lon: float = _TOKYO_LON) -> dict:
    return {
        "sensor_id": "5",
        "captured_at": "2026-06-19T01:00:00",
        "latitude": lat,
        "longitude": lon,
    }


class TestGeoBucket:
    def test_known_point_encodes_to_expected_cell(self) -> None:
        assert geo_bucket(_TOKYO_LAT, _TOKYO_LON, precision=5) == "xn774"

    def test_hash_length_equals_precision(self) -> None:
        assert len(geo_bucket(_TOKYO_LAT, _TOKYO_LON, precision=6)) == 6
        assert len(geo_bucket(_TOKYO_LAT, _TOKYO_LON, precision=8)) == 8

    def test_default_precision_is_five(self) -> None:
        # No explicit precision → module default (5) unless FLINK_GEOHASH_PRECISION is set.
        assert len(geo_bucket(_TOKYO_LAT, _TOKYO_LON)) == 5

    def test_finer_precision_extends_coarser_prefix(self) -> None:
        coarse = geo_bucket(_TOKYO_LAT, _TOKYO_LON, precision=5)
        fine = geo_bucket(_TOKYO_LAT, _TOKYO_LON, precision=7)
        assert fine.startswith(coarse)

    def test_distant_points_get_different_cells(self) -> None:
        tokyo = geo_bucket(_TOKYO_LAT, _TOKYO_LON, precision=5)
        berlin = geo_bucket(52.52, 13.405, precision=5)
        assert tokyo != berlin


class TestWithGeoBucket:
    def test_adds_geohash_field(self) -> None:
        enriched = with_geo_bucket(_event(), precision=5)
        assert enriched["geohash"] == "xn774"

    def test_preserves_original_fields(self) -> None:
        enriched = with_geo_bucket(_event(), precision=5)
        assert enriched["sensor_id"] == "5"
        assert enriched["latitude"] == _TOKYO_LAT

    def test_does_not_mutate_caller_dict(self) -> None:
        event = _event()
        with_geo_bucket(event, precision=5)
        assert "geohash" not in event


class TestGeoBucketEnricher:
    def test_map_enriches_event(self) -> None:
        enriched = GeoBucketEnricher(precision=5).map(_event())
        assert enriched["geohash"] == "xn774"
        assert enriched["sensor_id"] == "5"
