"""Tests for the consume loop's decoding + at-least-once commit ordering."""

import json

from aggregated_sink import consumer as consumer_module
from aggregated_sink.config import Config
from aggregated_sink.consumer import _decode, process_batch


def test_decode_skips_non_json():
    values = [json.dumps({"geohash": "a"}), "not-json", json.dumps({"geohash": "b"})]
    records = _decode(values)
    assert [r["geohash"] for r in records] == ["a", "b"]


class _FakeMsg:
    def __init__(self, value):
        self.value = value


class _FakeConsumer:
    def __init__(self, batch):
        self._batch = batch
        self.committed = 0

    def poll(self, timeout_ms=None, max_records=None):
        batch, self._batch = self._batch, {}
        return batch

    def commit(self):
        self.committed += 1


def _config():
    return Config(
        bootstrap_servers="x:9092",
        topic="radiation.aggregated",
        group_id="g",
        dsn="",
        batch_size=10,
        batch_timeout_s=0.01,
        connect_retries=1,
        connect_backoff_s=0,
        log_level="INFO",
    )


def test_process_batch_upserts_valid_then_commits_offsets(monkeypatch):
    """Malformed messages are dropped; the Kafka commit follows the DB upsert."""
    fake_consumer = _FakeConsumer(
        {
            "tp-0": [
                _FakeMsg(
                    json.dumps(
                        {"geohash": "a", "window_start": "2026-01-01T00:00:00+00:00", "count": 1}
                    )
                ),
                _FakeMsg("garbage"),  # dropped by _decode
                _FakeMsg(json.dumps({"count": 2})),  # dropped by build_rows (no key)
            ]
        }
    )

    seen = {}

    def fake_upsert(_conn, rows):
        seen["rows"] = rows
        # Offsets must NOT be advanced until after this returns.
        assert fake_consumer.committed == 0
        return len(rows)

    monkeypatch.setattr(consumer_module, "upsert_batch", fake_upsert)

    written = process_batch(fake_consumer, object(), _config())

    assert written == 1
    assert [r[0] for r in seen["rows"]] == ["a"]  # only the well-formed record
    assert fake_consumer.committed == 1  # offsets advanced exactly once, after upsert


def test_process_batch_empty_poll_is_noop(monkeypatch):
    fake_consumer = _FakeConsumer({})
    monkeypatch.setattr(consumer_module, "upsert_batch", lambda *a: 1 / 0)  # must not run
    assert process_batch(fake_consumer, object(), _config()) == 0
    assert fake_consumer.committed == 0
