"""
producer.py — Kafka producer for the radiation.raw topic.

Reads the Safecast CSV via csv_reader, converts rows via mapper, and
publishes RadiationEvent JSON to Kafka in uploaded_at order.

Speed modes (controlled by the --speed CLI flag):
  <N>    fixed rate  — emit N events per second regardless of timestamps.
                       e.g. --speed 100
  <N>x   multiplier  — replay N× faster than real time, sleeping proportionally
                       to the gap between consecutive uploaded_at timestamps.
                       e.g. --speed 10x  (1x = real-time, 100x = 100× faster)
  max    unthrottled — never sleep; push as fast as Kafka accepts. Used for the
                       producer load test (perf tuning) and full-dataset local runs.

Ordering guarantee (§7.3):
  csv_reader sorts per batch by uploaded_at, satisfying the no-pre-sort rule.
  Flink handles residual cross-batch disorder with its 30-second watermark.
"""

from __future__ import annotations

import json
import logging
import math
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .csv_reader import stream_rows
from .mapper import row_to_event
from .metrics import ProducerMetrics
from .sensor_classifier import DEFAULT_MOVEMENT_THRESHOLD_M, SensorClassifier

logger = logging.getLogger(__name__)

# Maximum simulated sleep in multiplier mode — caps long data gaps.
_MAX_SLEEP_SECONDS = 5.0

# Log a progress line every N events sent.
_LOG_EVERY = 500

# Default port for the Prometheus /metrics endpoint (0 disables it).
DEFAULT_METRICS_PORT = 8001

# Kafka producer throughput defaults (see docs/perf/producer-load-test.md, ADR-012).
# batch.size is the max bytes kafka-python coalesces per partition batch; linger.ms is how
# long it waits to fill a batch before sending. Larger values trade a little latency for far
# fewer, larger requests. These defaults are tuned for the high-throughput replay; override
# per-env with KAFKA_BATCH_SIZE / KAFKA_LINGER_MS / KAFKA_COMPRESSION_TYPE.
DEFAULT_BATCH_SIZE = 65_536
DEFAULT_LINGER_MS = 20
DEFAULT_COMPRESSION_TYPE = "lz4"


def _parse_speed(value: str) -> tuple[str, float]:
    """
    Parse the --speed flag value.

    Returns:
        ('rate', N)       — emit N events per second
        ('multiplier', N) — replay N× faster than real time
        ('max', 0.0)      — unthrottled, never sleep

    Raises:
        ValueError: on a non-numeric, non-positive, or non-finite N —
        ``--speed 0`` previously slipped through and crashed the rate mode
        with a ZeroDivisionError (main-review finding (g)).
    """
    v = value.strip()
    if v.lower() == "max":
        return ("max", 0.0)
    try:
        if v.lower().endswith("x"):
            mode, num = "multiplier", float(v[:-1])
        else:
            mode, num = "rate", float(v)
    except ValueError:
        raise ValueError(f"invalid --speed value {value!r}: expected <N>, <N>x or 'max'") from None
    if not math.isfinite(num) or num <= 0:
        raise ValueError(
            f"invalid --speed value {value!r}: N must be a positive finite number "
            "(use 'max' for unthrottled replay)"
        )
    return (mode, num)


def _parse_uploaded_at(ts: str | None) -> float | None:
    """Return a Unix timestamp float from an ISO8601 string, or None."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts).timestamp()
    except ValueError:
        # Pythons before 3.11 reject fractional seconds that aren't 3 or 6
        # digits (Safecast upload times have 5). Normalise to microseconds.
        fixed = _normalise_fractional_seconds(ts)
        if fixed == ts:
            return None
        try:
            return datetime.fromisoformat(fixed).timestamp()
        except ValueError:
            return None


_FRACTIONAL_RE = re.compile(r"\.(\d+)")


def _normalise_fractional_seconds(ts: str) -> str:
    """Pad/truncate a timestamp's fractional seconds to 6 digits (microseconds)."""
    m = _FRACTIONAL_RE.search(ts)
    if not m:
        return ts
    micros = (m.group(1) + "000000")[:6]
    return ts[: m.start()] + "." + micros + ts[m.end() :]


def _make_producer(
    bootstrap_servers: str,
    metrics: ProducerMetrics,
    batch_size: int = DEFAULT_BATCH_SIZE,
    linger_ms: int = DEFAULT_LINGER_MS,
    compression_type: str | None = DEFAULT_COMPRESSION_TYPE,
) -> KafkaProducer:
    def _serialize_value(value: Any) -> bytes:
        # Serialize once here (kafka-python calls this in the producer thread)
        # and account the byte size for the bytes_sent_total metric for free.
        data = json.dumps(value).encode("utf-8")
        metrics.record_bytes(len(data))
        return data

    # "none"/"" disables compression; kafka-python wants None, not the string.
    compression = compression_type or None
    if compression == "none":
        compression = None

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=_serialize_value,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        batch_size=batch_size,
        linger_ms=linger_ms,
        compression_type=compression,
    )


def run(
    csv_path: Path,
    bootstrap_servers: str,
    topic: str = "radiation.raw",
    speed: str = "1x",
    batch_size: int = 10_000,
    skip_rows: int = 0,
    metrics_port: int = DEFAULT_METRICS_PORT,
    start_ts: str | None = None,
    end_ts: str | None = None,
    movement_threshold_m: float = DEFAULT_MOVEMENT_THRESHOLD_M,
    producer_batch_size: int = DEFAULT_BATCH_SIZE,
    producer_linger_ms: int = DEFAULT_LINGER_MS,
    compression_type: str | None = DEFAULT_COMPRESSION_TYPE,
) -> ProducerMetrics:
    """
    Stream the Safecast CSV to Kafka.

    Args:
        csv_path:          Path to the Safecast measurements CSV.
        bootstrap_servers: Kafka bootstrap string, e.g. "localhost:29092".
        topic:             Destination Kafka topic (default: radiation.raw).
        speed:             Speed flag value — "100" for 100 ev/s, "10x" for 10× real-time.
        batch_size:        Rows per csv_reader batch (passed through).
        skip_rows:         Skip this many rows from the start (resume support).
        metrics_port:      Port for the Prometheus /metrics endpoint (0 disables).
        start_ts:          ISO8601 lower bound on captured_at (inclusive). None = no lower bound.
        end_ts:            ISO8601 upper bound on captured_at (inclusive). None = no upper bound.
        movement_threshold_m: Radius (metres) a sensor's readings may span and
                           still be tagged "fixed"; beyond it the sensor becomes
                           "mobile". See sensor_classifier / ADR-011.
        producer_batch_size:  Kafka producer batch.size in bytes (coalesced per
                           partition). NOTE: distinct from `batch_size` above,
                           which is the CSV reader's sort batch. See ADR-012.
        producer_linger_ms:   Kafka producer linger.ms — how long to wait to fill
                           a batch before sending.
        compression_type:     Kafka producer compression ("lz4"/"gzip"/"snappy"/
                           "zstd"/"none"). See docs/perf/producer-load-test.md.

    Returns:
        The ProducerMetrics collected during the run (handy for tests/reports).
    """
    speed_mode, speed_val = _parse_speed(speed)
    logger.info(
        "Starting producer: csv=%s topic=%s speed=%s (%s=%.2f)",
        csv_path,
        topic,
        speed,
        speed_mode,
        speed_val,
    )

    metrics = ProducerMetrics()
    metrics.start_http_server(metrics_port)

    producer = _make_producer(
        bootstrap_servers,
        metrics,
        batch_size=producer_batch_size,
        linger_ms=producer_linger_ms,
        compression_type=compression_type,
    )
    classifier = SensorClassifier(movement_threshold_m=movement_threshold_m)

    prev_uploaded_ts: float | None = None

    try:
        for row in stream_rows(
            csv_path,
            batch_size=batch_size,
            skip_rows=skip_rows,
            start_ts=start_ts,
            end_ts=end_ts,
        ):
            event = row_to_event(row)
            if event is None:
                metrics.record_discarded()
                continue

            # Tag the sensor as mobile vs fixed from the spread of its coordinates
            # seen so far (mapper guarantees latitude/longitude are present here).
            event["sensor_type"] = classifier.classify(
                event["sensor_id"], event["latitude"], event["longitude"]
            )

            _apply_speed(speed_mode, speed_val, event.get("uploaded_at"), prev_uploaded_ts)
            uploaded_ts = _parse_uploaded_at(event.get("uploaded_at"))
            prev_uploaded_ts = uploaded_ts

            try:
                future = producer.send(
                    topic,
                    key=event["sensor_id"],
                    value=event,
                )
                future.add_errback(metrics.record_error)
                metrics.record_sent(uploaded_ts)
            except KafkaError as exc:
                logger.warning("Failed to send event (sensor=%s): %s", event.get("sensor_id"), exc)
                metrics.record_error(exc)

            if metrics.sent % _LOG_EVERY == 0:
                metrics.refresh_rate_gauge()
                snap = metrics.snapshot()
                logger.info(
                    "Progress: sent=%d discarded=%d errors=%d rate=%.1f ev/s",
                    snap.sent,
                    snap.discarded,
                    snap.errors,
                    snap.events_per_second,
                )

    finally:
        producer.flush()
        producer.close()
        metrics.refresh_rate_gauge()
        snap = metrics.snapshot()
        logger.info(
            "Done: sent=%d discarded=%d errors=%d elapsed=%.1fs rate=%.1f ev/s " "bytes=%d",
            snap.sent,
            snap.discarded,
            snap.errors,
            snap.elapsed_seconds,
            snap.events_per_second,
            snap.bytes_sent,
        )

    return metrics


def _apply_speed(
    mode: str,
    value: float,
    current_uploaded_at: str | None,
    prev_uploaded_ts: float | None,
) -> None:
    """Sleep the appropriate amount based on the speed mode."""
    if mode == "max":
        # Unthrottled: never sleep — throughput is bounded only by Kafka.
        return
    if mode == "rate":
        # Fixed events-per-second: sleep 1/rate between each event.
        time.sleep(1.0 / value)
    else:
        # Multiplier: sleep proportional to the real-time gap, scaled down.
        current_ts = _parse_uploaded_at(current_uploaded_at)
        if current_ts is not None and prev_uploaded_ts is not None:
            gap = current_ts - prev_uploaded_ts
            if gap > 0:
                delay = min(gap / value, _MAX_SLEEP_SECONDS)
                if delay > 0:
                    time.sleep(delay)
