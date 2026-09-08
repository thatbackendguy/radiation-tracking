from fastapi.testclient import TestClient

from app.core.settings import settings


class TestHealth:
    def test_returns_200(self, client: TestClient) -> None:
        response = client.get("/health")
        assert response.status_code == 200

    def test_response_shape(self, client: TestClient) -> None:
        # Lifespan never runs in tests, so both Kafka components report their
        # pre-startup "stopped" state — still healthy (not degraded).
        body = client.get("/health").json()
        assert body == {
            "status": "ok",
            "service": "backend",
            "kafka_consumer": "stopped",
            "config_producer": "stopped",
            "ws_clients": 0,
        }


class TestGetConfig:
    def test_returns_200(self, client: TestClient) -> None:
        assert client.get("/config").status_code == 200

    def test_returns_defaults(self, client: TestClient) -> None:
        body = client.get("/config").json()
        assert body["cpm_warn_threshold"] == settings.cpm_warn_threshold
        assert body["cpm_danger_threshold"] == settings.cpm_danger_threshold
        assert body["area"] is None
        assert body["timespan"] is None


class TestPostConfig:
    def test_valid_threshold_update_returns_202(self, client: TestClient) -> None:
        response = client.post(
            "/config", json={"cpm_warn_threshold": 150, "cpm_danger_threshold": 2000}
        )
        assert response.status_code == 202

    def test_valid_threshold_update_reflected_in_get(self, client: TestClient) -> None:
        client.post("/config", json={"cpm_warn_threshold": 150, "cpm_danger_threshold": 2000})
        body = client.get("/config").json()
        assert body["cpm_warn_threshold"] == 150
        assert body["cpm_danger_threshold"] == 2000

    def test_partial_update_preserves_other_fields(self, client: TestClient) -> None:
        client.post("/config", json={"cpm_warn_threshold": 50})
        body = client.get("/config").json()
        assert body["cpm_warn_threshold"] == 50
        assert body["cpm_danger_threshold"] == settings.cpm_danger_threshold

    def test_publishes_full_merged_config_on_partial_update(
        self, client: TestClient, stub_config_producer
    ) -> None:
        # A one-field update must still publish BOTH thresholds — Flink requires them.
        client.post("/config", json={"cpm_warn_threshold": 50})
        stub_config_producer.publish.assert_awaited_once()
        sent = stub_config_producer.publish.await_args.args[0]
        assert sent["cpm_warn_threshold"] == 50
        assert sent["cpm_danger_threshold"] == settings.cpm_danger_threshold

    def test_area_filter_accepted(self, client: TestClient) -> None:
        payload = {"area": {"min_lat": 30.0, "max_lat": 42.0, "min_lon": 130.0, "max_lon": 145.0}}
        assert client.post("/config", json=payload).status_code == 202

    def test_timespan_serialized_with_offset_not_z(
        self, client: TestClient, stub_config_producer
    ) -> None:
        payload = {"timespan": {"start": "2011-03-11T00:00:00Z", "end": "2011-04-11T00:00:00Z"}}
        assert client.post("/config", json=payload).status_code == 202
        sent = stub_config_producer.publish.await_args.args[0]
        # Flink uses datetime.fromisoformat — the wire form must use +00:00, not a trailing 'Z'.
        assert sent["timespan"]["start"].endswith("+00:00")
        assert not sent["timespan"]["start"].endswith("Z")


class TestPublishFailure:
    def test_broker_unavailable_returns_503(self, client: TestClient, stub_config_producer) -> None:
        from app.core.kafka_producer import ConfigPublishError

        stub_config_producer.publish.side_effect = ConfigPublishError("broker down")
        response = client.post(
            "/config", json={"cpm_warn_threshold": 150, "cpm_danger_threshold": 2000}
        )
        assert response.status_code == 503

    def test_config_unchanged_when_publish_fails(
        self, client: TestClient, stub_config_producer
    ) -> None:
        from app.core.kafka_producer import ConfigPublishError

        stub_config_producer.publish.side_effect = ConfigPublishError("broker down")
        client.post("/config", json={"cpm_warn_threshold": 150, "cpm_danger_threshold": 2000})
        body = client.get("/config").json()
        assert body["cpm_warn_threshold"] == settings.cpm_warn_threshold
        assert body["cpm_danger_threshold"] == settings.cpm_danger_threshold


class TestConfigValidation:
    def test_warn_gte_danger_rejected(self, client: TestClient) -> None:
        response = client.post(
            "/config", json={"cpm_warn_threshold": 1000, "cpm_danger_threshold": 500}
        )
        assert response.status_code == 422

    def test_warn_equal_danger_rejected(self, client: TestClient) -> None:
        response = client.post(
            "/config", json={"cpm_warn_threshold": 500, "cpm_danger_threshold": 500}
        )
        assert response.status_code == 422

    def test_partial_update_breaking_invariant_rejected(self, client: TestClient) -> None:
        # Step 1: push warn threshold close to current danger (default 1000)
        client.post("/config", json={"cpm_warn_threshold": 900})
        # Step 2: lower danger below current warn — merged state would be warn=900, danger=500
        response = client.post("/config", json={"cpm_danger_threshold": 500})
        assert response.status_code == 422

    def test_min_lat_gte_max_lat_rejected(self, client: TestClient) -> None:
        payload = {"area": {"min_lat": 50.0, "max_lat": 30.0, "min_lon": 0.0, "max_lon": 10.0}}
        assert client.post("/config", json=payload).status_code == 422

    def test_min_lon_gte_max_lon_rejected(self, client: TestClient) -> None:
        payload = {"area": {"min_lat": 30.0, "max_lat": 50.0, "min_lon": 145.0, "max_lon": 130.0}}
        assert client.post("/config", json=payload).status_code == 422

    def test_timespan_start_gte_end_rejected(self, client: TestClient) -> None:
        payload = {"timespan": {"start": "2011-04-11T00:00:00Z", "end": "2011-03-11T00:00:00Z"}}
        assert client.post("/config", json=payload).status_code == 422

    def test_zero_cpm_threshold_rejected(self, client: TestClient) -> None:
        assert client.post("/config", json={"cpm_warn_threshold": 0}).status_code == 422

    def test_negative_cpm_threshold_rejected(self, client: TestClient) -> None:
        assert client.post("/config", json={"cpm_danger_threshold": -10}).status_code == 422


class TestConcurrentUpdates:
    def test_concurrent_partial_updates_merge_consistently(self, stub_config_producer) -> None:
        # Two partial updates fired at once — one sets warn, the other danger. The
        # _config_lock must serialize the read-modify-write cycle so the final state
        # reflects BOTH fields; without it one write is lost (last-write-wins) and the
        # backend/Flink could disagree.
        import asyncio

        import app.routers.config as config_module
        from app.routers.config import ConfigPayload, update_config

        async def drive() -> None:
            await asyncio.gather(
                update_config(ConfigPayload(cpm_warn_threshold=50)),
                update_config(ConfigPayload(cpm_danger_threshold=2000)),
            )

        asyncio.run(drive())

        assert config_module._active_config.cpm_warn_threshold == 50
        assert config_module._active_config.cpm_danger_threshold == 2000
        # Each update is published independently — no message is dropped under contention.
        assert stub_config_producer.publish.await_count == 2


class TestConfigWriteAuth:
    """POST /config is gated by X-Config-Token when config_write_token is set (finding (e))."""

    _VALID = {"cpm_warn_threshold": 150, "cpm_danger_threshold": 2000}

    def test_no_token_configured_allows_write(self, client: TestClient) -> None:
        # Default (empty token) keeps local dev open — no header required.
        assert settings.config_write_token == ""
        assert client.post("/config", json=self._VALID).status_code == 202

    def test_missing_header_when_token_set_is_401(self, client: TestClient, monkeypatch) -> None:
        monkeypatch.setattr("app.core.settings.settings.config_write_token", "s3cret")
        response = client.post("/config", json=self._VALID)
        assert response.status_code == 401

    def test_wrong_token_is_403(self, client: TestClient, monkeypatch) -> None:
        monkeypatch.setattr("app.core.settings.settings.config_write_token", "s3cret")
        response = client.post("/config", json=self._VALID, headers={"X-Config-Token": "nope"})
        assert response.status_code == 403

    def test_correct_token_is_accepted(self, client: TestClient, monkeypatch) -> None:
        monkeypatch.setattr("app.core.settings.settings.config_write_token", "s3cret")
        response = client.post("/config", json=self._VALID, headers={"X-Config-Token": "s3cret"})
        assert response.status_code == 202

    def test_gate_does_not_touch_get(self, client: TestClient, monkeypatch) -> None:
        # Reads stay open even when writes are gated.
        monkeypatch.setattr("app.core.settings.settings.config_write_token", "s3cret")
        assert client.get("/config").status_code == 200
