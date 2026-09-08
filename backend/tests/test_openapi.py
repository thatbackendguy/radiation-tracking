"""Contract tests for the finalised OpenAPI spec (Week 7).

The committed docs/api/openapi.json is regenerated from this same schema
(scripts/export_openapi.py), so locking the contract here also guards the
exported artifact against silent drift.
"""

import pytest
from fastapi.testclient import TestClient

from app.routers.stream import _origin_allowed


class TestOpenApiSpec:
    def test_info_finalised(self, client: TestClient) -> None:
        spec = client.get("/openapi.json").json()
        assert spec["info"]["version"] == "1.0.0"
        assert spec["info"]["title"] == "Radiation Tracking Backend"
        # /ws/stream sits outside OpenAPI paths — its contract lives in the description.
        assert "/ws/stream" in spec["info"]["description"]

    def test_all_tags_described(self, client: TestClient) -> None:
        spec = client.get("/openapi.json").json()
        tags = {tag["name"]: tag["description"] for tag in spec["tags"]}
        assert set(tags) == {"health", "metrics", "config", "recent", "history"}
        assert all(tags.values())

    def test_all_routes_present(self, client: TestClient) -> None:
        spec = client.get("/openapi.json").json()
        assert set(spec["paths"]) == {
            "/health",
            "/metrics",
            "/config",
            "/recent",
            "/history/summary",
            "/history/timeseries",
            "/history/hotspots",
        }

    def test_post_config_documents_error_contracts(self, client: TestClient) -> None:
        spec = client.get("/openapi.json").json()
        responses = spec["paths"]["/config"]["post"]["responses"]
        assert {"202", "422", "503"} <= set(responses)
        # The 503 doc must state the strict no-commit behaviour reviewers rely on.
        assert "NOT" in responses["503"]["description"]

    def test_config_payload_carries_examples(self, client: TestClient) -> None:
        spec = client.get("/openapi.json").json()
        payload_schema = spec["components"]["schemas"]["ConfigPayload"]["properties"]
        assert payload_schema["cpm_warn_threshold"]["examples"] == [100.0]
        assert payload_schema["cpm_danger_threshold"]["examples"] == [1000.0]

    def test_recent_documents_cursor_pagination(self, client: TestClient) -> None:
        spec = client.get("/openapi.json").json()
        recent_get = spec["paths"]["/recent"]["get"]
        params = {param["name"] for param in recent_get["parameters"]}
        assert {"limit", "cursor"} <= params


class TestDropletOriginPolicy:
    """The droplet .env restricts CORS_ORIGINS to the public frontend origin."""

    DROPLET_ORIGINS = "http://203.0.113.10"

    def test_frontend_origin_allowed(self, monkeypatch) -> None:
        monkeypatch.setattr("app.core.settings.settings.cors_origins", self.DROPLET_ORIGINS)
        assert _origin_allowed("http://203.0.113.10") is True

    def test_foreign_origin_rejected(self, monkeypatch) -> None:
        monkeypatch.setattr("app.core.settings.settings.cors_origins", self.DROPLET_ORIGINS)
        assert _origin_allowed("http://evil.example") is False

    def test_scheme_mismatch_rejected(self, monkeypatch) -> None:
        # https frontend vs http allowlist must NOT match — origins are exact.
        monkeypatch.setattr("app.core.settings.settings.cors_origins", self.DROPLET_ORIGINS)
        assert _origin_allowed("https://203.0.113.10") is False

    def test_ws_connect_rejected_from_foreign_origin(self, client: TestClient, monkeypatch):
        monkeypatch.setattr("app.core.settings.settings.cors_origins", self.DROPLET_ORIGINS)
        with pytest.raises(Exception):
            # Closed with 1008 before accept — the TestClient surfaces it as an error.
            with client.websocket_connect("/ws/stream", headers={"origin": "http://evil.example"}):
                pass
