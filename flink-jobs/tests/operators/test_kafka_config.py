"""Unit tests for the env-driven Kafka security properties. Pure function — no PyFlink, no
broker. The builder adapter is exercised with a fake builder that records set_property calls.
"""

from __future__ import annotations

from operators.kafka_config import apply_security, kafka_security_properties


class TestKafkaSecurityProperties:
    def test_no_protocol_is_empty(self) -> None:
        assert kafka_security_properties(env={}) == {}

    def test_plaintext_is_empty(self) -> None:
        # Local default must stay a no-op so docker compose is unchanged.
        assert kafka_security_properties(env={"KAFKA_SECURITY_PROTOCOL": "PLAINTEXT"}) == {}

    def test_sasl_ssl_plain_builds_jaas(self) -> None:
        props = kafka_security_properties(
            env={
                "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "KAFKA_SASL_MECHANISM": "PLAIN",
                "KAFKA_SASL_USERNAME": "key",
                "KAFKA_SASL_PASSWORD": "secret",
            }
        )
        assert props["security.protocol"] == "SASL_SSL"
        assert props["sasl.mechanism"] == "PLAIN"
        assert 'username="key"' in props["sasl.jaas.config"]
        assert 'password="secret"' in props["sasl.jaas.config"]
        assert "PlainLoginModule" in props["sasl.jaas.config"]

    def test_scram_uses_scram_login_module(self) -> None:
        props = kafka_security_properties(
            env={
                "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "KAFKA_SASL_MECHANISM": "SCRAM-SHA-256",
                "KAFKA_SASL_USERNAME": "u",
                "KAFKA_SASL_PASSWORD": "p",
            }
        )
        assert "ScramLoginModule" in props["sasl.jaas.config"]
        assert props["sasl.mechanism"] == "SCRAM-SHA-256"

    def test_sasl_defaults_mechanism_to_plain(self) -> None:
        props = kafka_security_properties(env={"KAFKA_SECURITY_PROTOCOL": "SASL_SSL"})
        assert props["sasl.mechanism"] == "PLAIN"

    def test_jaas_escapes_quote_and_backslash_in_credentials(self) -> None:
        # A secret containing " or \ must be escaped, or the double-quoted JAAS field breaks.
        props = kafka_security_properties(
            env={
                "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "KAFKA_SASL_MECHANISM": "PLAIN",
                "KAFKA_SASL_USERNAME": "ke\\y",
                "KAFKA_SASL_PASSWORD": 'se"cret',
            }
        )
        jaas = props["sasl.jaas.config"]
        # backslash → \\ and " → \" inside the double-quoted fields.
        assert 'username="ke\\\\y"' in jaas
        assert 'password="se\\"cret"' in jaas

    def test_ssl_without_sasl_sets_protocol_only(self) -> None:
        props = kafka_security_properties(env={"KAFKA_SECURITY_PROTOCOL": "SSL"})
        assert props == {"security.protocol": "SSL"}


class _FakeBuilder:
    """Records set_property calls and returns self, mirroring the fluent builder API."""

    def __init__(self) -> None:
        self.props: dict[str, str] = {}

    def set_property(self, key: str, value: str) -> "_FakeBuilder":
        self.props[key] = value
        return self


class TestApplySecurity:
    def test_returns_builder_for_chaining(self) -> None:
        builder = _FakeBuilder()
        assert apply_security(builder) is builder

    def test_no_security_leaves_builder_untouched(self, monkeypatch) -> None:
        for var in (
            "KAFKA_SECURITY_PROTOCOL",
            "KAFKA_SASL_MECHANISM",
            "KAFKA_SASL_USERNAME",
            "KAFKA_SASL_PASSWORD",
        ):
            monkeypatch.delenv(var, raising=False)
        builder = _FakeBuilder()
        apply_security(builder)
        assert builder.props == {}

    def test_applies_each_property(self, monkeypatch) -> None:
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "key")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "secret")
        builder = _FakeBuilder()
        apply_security(builder)
        assert builder.props["security.protocol"] == "SASL_SSL"
        assert "sasl.jaas.config" in builder.props
