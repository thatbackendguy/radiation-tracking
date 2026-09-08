"""
Integration tests for data_provider/producer.py against a real Kafka broker.

Spins up a throwaway Kafka via Testcontainers (guidelines §9.1), runs the
producer end-to-end against a small CSV, then consumes radiation.raw and
asserts the contract the rest of the pipeline depends on:

  * only valid rows are published (mapper discards are not sent),
  * events arrive in uploaded_at order (ordering guarantee, §7.3 / H8),
  * each message is keyed by sensor_id,
  * each value is well-formed RadiationEvent JSON.

These tests need a running Docker daemon. They are marked `integration` and
skip cleanly when Docker or the testcontainers package is unavailable, so the
unit suite still runs in environments without Docker.

Run:
    pip install -r requirements.txt -r requirements-dev.txt
    pytest data-provider/tests/test_producer_integration.py -v
    # skip them explicitly:
    pytest -m "not integration"
"""

from __future__ import annotations

import importlib.util
import json
import sys
import types
import uuid
from pathlib import Path

import pytest


def _load_producer_run():
    """
    Import producer.run despite the package living in a hyphenated directory.

    producer.py uses relative imports (`from .csv_reader import ...`), so it
    must be loaded as a submodule of a real package. The directory name
    ``data-provider`` is not a valid module name, so we register a synthetic
    package pointing at it and exec producer.py under that package — its
    relative imports then resolve against the same directory.
    """
    dp_dir = Path(__file__).parent.parent
    pkg_name = "data_provider_pkg"
    pkg = types.ModuleType(pkg_name)
    pkg.__path__ = [str(dp_dir)]
    sys.modules[pkg_name] = pkg

    spec = importlib.util.spec_from_file_location(f"{pkg_name}.producer", dp_dir / "producer.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.run


run = _load_producer_run()

# --- Skip the whole module unless Docker + testcontainers are available -----

testcontainers_kafka = pytest.importorskip(
    "testcontainers.kafka",
    reason="testcontainers not installed (pip install -r requirements-dev.txt)",
)
KafkaContainer = testcontainers_kafka.KafkaContainer

try:
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
except ImportError:  # pragma: no cover - kafka-python is a hard runtime dep
    pytest.skip("kafka-python not installed", allow_module_level=True)

pytestmark = pytest.mark.integration

TOPIC = "radiation.raw"

# Internal field names (the reader accepts these as-is, alongside the
# canonical "Captured Time"/"Device ID" display names).
HEADER = (
    "captured_at,uploaded_at,device_id,sensor_id,"
    "latitude,longitude,value,unit,height,location_name,measurement_import_id"
)


def _row(
    *,
    captured_at: str,
    uploaded_at: str,
    device_id: str = "100",
    sensor_id: str = "",
    lat: str = "35.0",
    lon: str = "139.0",
    value: str = "42.0",
    unit: str = "cpm",
) -> str:
    return f"{captured_at},{uploaded_at},{device_id},{sensor_id}," f"{lat},{lon},{value},{unit},,,"


@pytest.fixture(scope="module")
def kafka_bootstrap():
    """Start a single Kafka broker for the module and yield its bootstrap string."""
    with KafkaContainer() as kafka:
        yield kafka.get_bootstrap_server()


@pytest.fixture()
def single_partition_topic(kafka_bootstrap):
    """
    Create a fresh single-partition topic per test.

    One partition gives a total order on consumption, so a non-decreasing
    uploaded_at sequence directly proves the producer published in order
    (rather than relying on per-key partition ordering).
    """
    name = f"{TOPIC}.{uuid.uuid4().hex[:8]}"
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    try:
        admin.create_topics([NewTopic(name=name, num_partitions=1, replication_factor=1)])
    finally:
        admin.close()
    return name


def _consume(bootstrap: str, topic: str, expected: int, timeout_s: float = 20.0):
    """Read up to `expected` messages from the start of `topic`."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=int(timeout_s * 1000),
        group_id=f"test-{uuid.uuid4().hex[:8]}",
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    try:
        records = []
        for msg in consumer:
            records.append(msg)
            if len(records) >= expected:
                break
        return records
    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_producer_publishes_valid_rows_in_uploaded_at_order(
    tmp_path, kafka_bootstrap, single_partition_topic
):
    # uploaded_at deliberately shuffled relative to file order; the producer's
    # per-batch sort must emit them ascending by uploaded_at.
    rows = [
        _row(captured_at="2020-01-01T00:00:03Z", uploaded_at="2020-01-02T00:00:30Z", device_id="C"),
        _row(captured_at="2020-01-01T00:00:01Z", uploaded_at="2020-01-02T00:00:10Z", device_id="A"),
        _row(captured_at="2020-01-01T00:00:05Z", uploaded_at="2020-01-02T00:00:50Z", device_id="E"),
        _row(captured_at="2020-01-01T00:00:02Z", uploaded_at="2020-01-02T00:00:20Z", device_id="B"),
        _row(captured_at="2020-01-01T00:00:04Z", uploaded_at="2020-01-02T00:00:40Z", device_id="D"),
    ]
    csv_path = tmp_path / "ordered.csv"
    csv_path.write_text(HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8")

    # Large batch so the whole file sorts as one window; high rate = no real sleep.
    run(
        csv_path=csv_path,
        bootstrap_servers=kafka_bootstrap,
        topic=single_partition_topic,
        speed="100000",
        batch_size=1000,
        metrics_port=0,  # no HTTP endpoint in tests
    )

    records = _consume(kafka_bootstrap, single_partition_topic, expected=5)

    assert len(records) == 5, "all five valid rows should be published"

    uploaded = [r.value["uploaded_at"] for r in records]
    assert uploaded == sorted(uploaded), "events must arrive in uploaded_at order"
    assert [r.value["sensor_id"] for r in records] == ["A", "B", "C", "D", "E"]


def test_producer_keys_messages_by_sensor_id(tmp_path, kafka_bootstrap, single_partition_topic):
    rows = [
        _row(
            captured_at="2020-01-01T00:00:01Z",
            uploaded_at="2020-01-02T00:00:10Z",
            device_id="dev-1",
        ),
        _row(
            captured_at="2020-01-01T00:00:02Z",
            uploaded_at="2020-01-02T00:00:20Z",
            device_id="0",
            sensor_id="sens-7",
        ),  # sensor_id wins over device_id
    ]
    csv_path = tmp_path / "keyed.csv"
    csv_path.write_text(HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8")

    run(
        csv_path=csv_path,
        bootstrap_servers=kafka_bootstrap,
        topic=single_partition_topic,
        speed="100000",
        batch_size=1000,
        metrics_port=0,  # no HTTP endpoint in tests
    )

    records = _consume(kafka_bootstrap, single_partition_topic, expected=2)

    by_key = {r.key: r.value for r in records}
    assert set(by_key) == {"dev-1", "sens-7"}
    assert by_key["dev-1"]["sensor_id"] == "dev-1"
    assert by_key["sens-7"]["sensor_id"] == "sens-7"
    # Kafka message key must equal the event's sensor_id for keyed routing.
    for r in records:
        assert r.key == r.value["sensor_id"]


def test_producer_discards_invalid_rows_end_to_end(
    tmp_path, kafka_bootstrap, single_partition_topic
):
    rows = [
        # valid
        _row(
            captured_at="2020-01-01T00:00:01Z", uploaded_at="2020-01-02T00:00:10Z", device_id="ok"
        ),
        # invalid: missing coordinates -> mapper returns None
        _row(
            captured_at="2020-01-01T00:00:02Z",
            uploaded_at="2020-01-02T00:00:20Z",
            device_id="nocoords",
            lat="",
            lon="",
        ),
        # invalid: no usable sensor id (device_id "0", no sensor_id)
        _row(captured_at="2020-01-01T00:00:03Z", uploaded_at="2020-01-02T00:00:30Z", device_id="0"),
        # invalid: coordinates out of range
        _row(
            captured_at="2020-01-01T00:00:04Z",
            uploaded_at="2020-01-02T00:00:40Z",
            device_id="badlat",
            lat="999.0",
        ),
    ]
    csv_path = tmp_path / "mixed.csv"
    csv_path.write_text(HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8")

    metrics = run(
        csv_path=csv_path,
        bootstrap_servers=kafka_bootstrap,
        topic=single_partition_topic,
        speed="100000",
        batch_size=1000,
        metrics_port=0,  # no HTTP endpoint in tests
    )

    # Only the single valid row should ever appear; ask for 2 to confirm no more.
    records = _consume(kafka_bootstrap, single_partition_topic, expected=2, timeout_s=10.0)

    assert len(records) == 1
    assert records[0].value["sensor_id"] == "ok"

    # Metrics must reflect the same end-to-end outcome: 1 sent, 3 discarded.
    snap = metrics.snapshot()
    assert snap.sent == 1
    assert snap.discarded == 3
    assert snap.bytes_sent > 0
    assert snap.errors == 0


def test_published_events_are_wellformed_radiation_events(
    tmp_path, kafka_bootstrap, single_partition_topic
):
    csv_path = tmp_path / "shape.csv"
    csv_path.write_text(
        HEADER
        + "\n"
        + _row(
            captured_at="2020-01-01T00:00:01Z",
            uploaded_at="2020-01-02T00:00:10Z",
            device_id="shape-1",
            value="123.0",
            unit="cpm",
        )
        + "\n",
        encoding="utf-8",
    )

    run(
        csv_path=csv_path,
        bootstrap_servers=kafka_bootstrap,
        topic=single_partition_topic,
        speed="100000",
        batch_size=1000,
        metrics_port=0,  # no HTTP endpoint in tests
    )

    records = _consume(kafka_bootstrap, single_partition_topic, expected=1)
    assert len(records) == 1
    event = records[0].value

    for field in ("sensor_id", "captured_at", "uploaded_at", "latitude", "longitude", "md5sum"):
        assert field in event, f"missing required field {field!r}"
    assert event["cpm"] == 123.0
    assert event["unit"] == "cpm"
    assert event["classification"] is None  # producer leaves classification to Flink
    assert isinstance(event["latitude"], float)


# ---------------------------------------------------------------------------
# Regression: the real, committed sample CSV (canonical Safecast schema)
# ---------------------------------------------------------------------------

SAMPLE_CSV = Path(__file__).parent.parent.parent / "data" / "sample.csv"


def test_real_sample_csv_streams_to_kafka(kafka_bootstrap, single_partition_topic):
    """
    End-to-end guard against the header-schema mismatch that silently made the
    producer publish zero events: stream the committed sample and assert events
    actually land on Kafka with T-separated ISO timestamps.
    """
    if not SAMPLE_CSV.exists():
        pytest.skip("data/sample.csv not present")

    metrics = run(
        csv_path=SAMPLE_CSV,
        bootstrap_servers=kafka_bootstrap,
        topic=single_partition_topic,
        speed="100000",
        batch_size=1000,
        metrics_port=0,
    )

    snap = metrics.snapshot()
    assert snap.sent > 0, "sample produced zero events — schema regression"
    assert snap.bytes_sent > 0

    records = _consume(kafka_bootstrap, single_partition_topic, expected=snap.sent)
    assert len(records) == snap.sent
    sample_event = records[0].value
    assert "T" in sample_event["captured_at"]
    assert isinstance(sample_event["latitude"], float)
