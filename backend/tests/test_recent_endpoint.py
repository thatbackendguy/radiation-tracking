import asyncio

import app.core.ring_buffer as ring_buffer_module


def _fill_buffer(make_event, n: int) -> None:
    async def scenario():
        for i in range(n):
            await ring_buffer_module._buffer.append(make_event(sensor_id=f"sensor-{i}"))

    asyncio.run(scenario())


class TestRecentEndpoint:
    def test_empty_buffer_returns_empty_list(self, client):
        response = client.get("/recent")
        assert response.status_code == 200
        assert response.json() == {
            "count": 0,
            "events": [],
            "next_cursor": None,
            "has_more": False,
        }

    def test_returns_buffered_events(self, client, make_event):
        _fill_buffer(make_event, 3)
        response = client.get("/recent")
        body = response.json()
        assert response.status_code == 200
        assert body["count"] == 3
        assert [e["sensor_id"] for e in body["events"]] == [
            "sensor-0",
            "sensor-1",
            "sensor-2",
        ]

    def test_limit_returns_newest_events(self, client, make_event):
        _fill_buffer(make_event, 5)
        response = client.get("/recent", params={"limit": 2})
        body = response.json()
        assert body["count"] == 2
        assert [e["sensor_id"] for e in body["events"]] == ["sensor-3", "sensor-4"]

    def test_limit_zero_is_clamped_not_rejected(self, client, make_event):
        _fill_buffer(make_event, 3)
        response = client.get("/recent", params={"limit": 0})
        assert response.status_code == 200
        assert response.json()["count"] == 1

    def test_limit_above_max_is_clamped(self, client, make_event):
        _fill_buffer(make_event, 3)
        response = client.get("/recent", params={"limit": 999999})
        assert response.status_code == 200
        assert response.json()["count"] == 3

    def test_event_shape_matches_schema(self, client, make_event):
        _fill_buffer(make_event, 1)
        event = client.get("/recent").json()["events"][0]
        assert event["sensor_id"] == "sensor-0"
        assert event["classification"] == "SAFE"
        assert event["cpm"] == 42.0
        assert "captured_at" in event and "uploaded_at" in event
        assert "latitude" in event and "longitude" in event

    def test_no_cursor_page_reports_has_more(self, client, make_event):
        _fill_buffer(make_event, 5)
        body = client.get("/recent", params={"limit": 2}).json()
        assert [e["sensor_id"] for e in body["events"]] == ["sensor-3", "sensor-4"]
        assert body["has_more"] is True
        assert body["next_cursor"] is not None

    def test_cursor_walks_back_through_window(self, client, make_event):
        _fill_buffer(make_event, 5)

        first = client.get("/recent", params={"limit": 2}).json()
        second = client.get("/recent", params={"limit": 2, "cursor": first["next_cursor"]}).json()
        third = client.get("/recent", params={"limit": 2, "cursor": second["next_cursor"]}).json()

        assert [e["sensor_id"] for e in second["events"]] == ["sensor-1", "sensor-2"]
        assert [e["sensor_id"] for e in third["events"]] == ["sensor-0"]
        assert third["has_more"] is False
        assert third["next_cursor"] is None

    def test_no_cursor_omitted_is_backward_compatible(self, client, make_event):
        _fill_buffer(make_event, 3)
        body = client.get("/recent").json()
        # Existing callers that ignore the new fields still get the newest window.
        assert body["count"] == 3
        assert [e["sensor_id"] for e in body["events"]] == [
            "sensor-0",
            "sensor-1",
            "sensor-2",
        ]


class TestRecentViewFilters:
    """Per-client view filters on /recent (ADR-017) — narrow only this response."""

    def _fill_mixed(self, make_event) -> None:
        async def scenario():
            buffer = ring_buffer_module._buffer
            await buffer.append(make_event(sensor_id="tokyo", latitude=35.6, longitude=139.7))
            await buffer.append(make_event(sensor_id="berlin", latitude=52.5, longitude=13.4))
            await buffer.append(
                make_event(
                    sensor_id="tokyo-late",
                    latitude=35.7,
                    longitude=139.8,
                    captured_at="2026-08-01T00:00:00Z",
                )
            )

        asyncio.run(scenario())

    def test_area_filter_narrows_events(self, client, make_event):
        self._fill_mixed(make_event)
        body = client.get(
            "/recent",
            params={"min_lat": 30, "max_lat": 40, "min_lon": 130, "max_lon": 145},
        ).json()
        assert [e["sensor_id"] for e in body["events"]] == ["tokyo", "tokyo-late"]

    def test_time_filter_narrows_events(self, client, make_event):
        self._fill_mixed(make_event)
        body = client.get(
            "/recent",
            params={"start": "2026-06-01T00:00:00Z", "end": "2026-07-01T00:00:00Z"},
        ).json()
        assert [e["sensor_id"] for e in body["events"]] == ["tokyo", "berlin"]

    def test_combined_filters(self, client, make_event):
        self._fill_mixed(make_event)
        body = client.get(
            "/recent",
            params={
                "min_lat": 30,
                "max_lat": 40,
                "min_lon": 130,
                "max_lon": 145,
                "start": "2026-06-01T00:00:00Z",
                "end": "2026-07-01T00:00:00Z",
            },
        ).json()
        assert [e["sensor_id"] for e in body["events"]] == ["tokyo"]

    def test_partial_area_is_422(self, client, make_event):
        self._fill_mixed(make_event)
        response = client.get("/recent", params={"min_lat": 30})
        assert response.status_code == 422

    def test_inverted_time_range_is_422(self, client, make_event):
        self._fill_mixed(make_event)
        response = client.get(
            "/recent",
            params={"start": "2026-07-01T00:00:00Z", "end": "2026-06-01T00:00:00Z"},
        )
        assert response.status_code == 422

    def test_filtered_pagination_walks_matching_events(self, client, make_event):
        # 4 in-area events interleaved with out-of-area ones; page size 2.
        async def scenario():
            buffer = ring_buffer_module._buffer
            for i in range(4):
                await buffer.append(make_event(sensor_id=f"in-{i}", latitude=35.0, longitude=139.0))
                await buffer.append(make_event(sensor_id=f"out-{i}", latitude=52.5, longitude=13.4))

        asyncio.run(scenario())
        params = {"min_lat": 30, "max_lat": 40, "min_lon": 130, "max_lon": 145, "limit": 2}

        first = client.get("/recent", params=params).json()
        assert [e["sensor_id"] for e in first["events"]] == ["in-2", "in-3"]
        assert first["has_more"] is True

        second = client.get("/recent", params={**params, "cursor": first["next_cursor"]}).json()
        assert [e["sensor_id"] for e in second["events"]] == ["in-0", "in-1"]
        assert second["has_more"] is False
        assert second["next_cursor"] is None
