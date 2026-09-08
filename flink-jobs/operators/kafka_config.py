"""
kafka_config.py — env-driven Kafka security properties for the Flink source/sink builders.

Locally the cluster is plaintext (``docker-compose.yml`` Kafka on ``kafka:9092``); on a
managed cloud broker (Confluent Cloud / Redpanda Serverless, ADR-010) the same job must
authenticate over ``SASL_SSL``. Rather than hardcode either, every ``KafkaSource`` /
``KafkaSink`` builder in radiation_tracking_job.py is passed through ``apply_security``,
which reads connection security from the environment and sets the matching Kafka client
properties. With nothing set (the local default) it is a **no-op**, so ``docker compose``
behaviour is unchanged.

Credentials come from the environment only (``KAFKA_SASL_USERNAME`` / ``KAFKA_SASL_PASSWORD``)
— never committed and never baked into the image (guidelines.md / DoD "no secrets in
images"). ``.env.example`` documents the variables with placeholders.

``kafka_security_properties`` is a pure function (reads env, returns a dict) and is
unit-tested without PyFlink; ``apply_security`` is the thin builder adapter.
"""

from __future__ import annotations

import os

# JAAS login modules per SASL family. Confluent Cloud / Redpanda use PLAIN (API key/secret);
# SCRAM is supported for self-managed brokers configured that way.
_PLAIN_LOGIN_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
_SCRAM_LOGIN_MODULE = "org.apache.kafka.common.security.scram.ScramLoginModule"


def _login_module(mechanism: str) -> str:
    """Pick the JAAS login module for a SASL mechanism (SCRAM-* vs PLAIN)."""
    return _SCRAM_LOGIN_MODULE if mechanism.upper().startswith("SCRAM") else _PLAIN_LOGIN_MODULE


def _escape_jaas(value: str) -> str:
    """Escape a value for a double-quoted JAAS field.

    JAAS values are wrapped in double quotes, so a literal backslash or double quote in a
    credential must be backslash-escaped or the ``sasl.jaas.config`` string is malformed
    (e.g. a secret containing ``"`` would prematurely close the quote). Backslash is escaped
    first so the quote's escaping backslash is not itself doubled.
    """
    return value.replace("\\", "\\\\").replace('"', '\\"')


def kafka_security_properties(env: dict[str, str] | None = None) -> dict[str, str]:
    """Build the Kafka client security properties from environment variables.

    Reads (all optional):
        KAFKA_SECURITY_PROTOCOL — e.g. SASL_SSL / SSL / PLAINTEXT (default/blank ⇒ none).
        KAFKA_SASL_MECHANISM    — e.g. PLAIN / SCRAM-SHA-256 (only used with a SASL protocol).
        KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD — credentials for the JAAS config.

    Returns an empty dict when no protocol (or PLAINTEXT) is configured, so the local
    plaintext setup is untouched. Otherwise returns the ``security.protocol`` and, for SASL
    protocols, ``sasl.mechanism`` + ``sasl.jaas.config`` ready for ``builder.set_property``.
    """
    env = os.environ if env is None else env

    protocol = env.get("KAFKA_SECURITY_PROTOCOL", "").strip()
    if not protocol or protocol.upper() == "PLAINTEXT":
        return {}

    props = {"security.protocol": protocol}

    if "SASL" in protocol.upper():
        mechanism = env.get("KAFKA_SASL_MECHANISM", "PLAIN").strip() or "PLAIN"
        username = _escape_jaas(env.get("KAFKA_SASL_USERNAME", ""))
        password = _escape_jaas(env.get("KAFKA_SASL_PASSWORD", ""))
        props["sasl.mechanism"] = mechanism
        props["sasl.jaas.config"] = (
            f"{_login_module(mechanism)} required " f'username="{username}" password="{password}";'
        )

    return props


def apply_security(builder):
    """Apply the env-derived Kafka security properties to a Source/Sink builder.

    Returns the builder so it can be used inline in the fluent build chain. A no-op when no
    security is configured (local plaintext).
    """
    for key, value in kafka_security_properties().items():
        builder = builder.set_property(key, value)
    return builder
