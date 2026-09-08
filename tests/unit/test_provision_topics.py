"""
Unit tests for scripts/provision_topics.py retention verification (M3, Week 7).

Only the pure cap-checking logic is covered here — the aiokafka plumbing needs a live
broker and is exercised by the droplet smoke check (`--verify-retention`). The module
import works without aiokafka installed (guarded import), which is what CI runs with.
"""

from __future__ import annotations

from scripts.provision_topics import (
    DEFAULT_MAX_RETENTION_BYTES,
    DEFAULT_MAX_RETENTION_MS,
    TOPICS,
    check_retention,
)

CAP_BYTES = 1073741824  # 1 GiB — docker-compose.yml default
CAP_MS = 86400000  # 24 h


def _configs(bytes_value: str | None, ms_value: str | None) -> dict:
    configs: dict = {}
    if bytes_value is not None:
        configs["retention.bytes"] = bytes_value
    if ms_value is not None:
        configs["retention.ms"] = ms_value
    return configs


class TestCheckRetention:
    def test_bounded_within_caps_passes(self) -> None:
        configs = _configs(str(CAP_BYTES), str(CAP_MS))
        assert check_retention("radiation.raw", configs, CAP_BYTES, CAP_MS) == []

    def test_unbounded_bytes_flagged(self) -> None:
        # -1 is Kafka's "keep forever" — exactly what must never reach the droplet.
        configs = _configs("-1", str(CAP_MS))
        problems = check_retention("radiation.raw", configs, CAP_BYTES, CAP_MS)
        assert len(problems) == 1
        assert "retention.bytes" in problems[0] and "unbounded" in problems[0]

    def test_unbounded_ms_flagged(self) -> None:
        configs = _configs(str(CAP_BYTES), "-1")
        problems = check_retention("radiation.alerts", configs, CAP_BYTES, CAP_MS)
        assert len(problems) == 1
        assert "retention.ms" in problems[0]

    def test_above_cap_flagged(self) -> None:
        configs = _configs(str(CAP_BYTES * 10), str(CAP_MS))
        problems = check_retention("radiation.clean", configs, CAP_BYTES, CAP_MS)
        assert len(problems) == 1
        assert "exceeds the cap" in problems[0]

    def test_missing_config_flagged(self) -> None:
        problems = check_retention("config.updates", {}, CAP_BYTES, CAP_MS)
        assert len(problems) == 2  # both retention.bytes and retention.ms missing

    def test_non_numeric_value_flagged(self) -> None:
        configs = _configs("not-a-number", str(CAP_MS))
        problems = check_retention("radiation.raw", configs, CAP_BYTES, CAP_MS)
        assert len(problems) == 1
        assert "non-numeric" in problems[0]

    def test_both_violations_reported_together(self) -> None:
        configs = _configs("-1", str(CAP_MS * 2))
        problems = check_retention("radiation.raw", configs, CAP_BYTES, CAP_MS)
        assert len(problems) == 2

    def test_topic_name_included_in_message(self) -> None:
        problems = check_retention("radiation.aggregated", {}, CAP_BYTES, CAP_MS)
        assert all(p.startswith("radiation.aggregated:") for p in problems)


class TestDefaults:
    def test_default_caps_mirror_compose(self) -> None:
        # The verify step must check against the same bound the kafka service ships with.
        assert DEFAULT_MAX_RETENTION_BYTES == CAP_BYTES
        assert DEFAULT_MAX_RETENTION_MS == CAP_MS

    def test_all_five_project_topics_listed(self) -> None:
        assert set(TOPICS) == {
            "radiation.raw",
            "radiation.clean",
            "radiation.aggregated",
            "radiation.alerts",
            "config.updates",
        }
