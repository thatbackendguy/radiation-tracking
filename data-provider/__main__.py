"""
Entry point: python -m data_provider

Usage examples:
  python -m data_provider --csv data/sample.csv --speed 10x
  python -m data_provider --csv data/measurements.csv --speed 500
  python -m data_provider --preset fukushima --speed 200x
  python -m data_provider --help
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from .csv_reader import _parse_ts
from .presets import PRESETS
from .producer import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_COMPRESSION_TYPE,
    DEFAULT_LINGER_MS,
    _parse_speed,
    run,
)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="data_provider",
        description="Stream the Safecast CSV to Kafka (radiation.raw topic).",
    )
    p.add_argument(
        "--csv",
        type=Path,
        default=Path(os.getenv("CSV_PATH", "data/measurements.csv")),
        help="Path to the Safecast measurements CSV (default: $CSV_PATH or data/measurements.csv)",
    )
    p.add_argument(
        "--speed",
        default=os.getenv("PRODUCER_SPEED", "1x"),
        help=(
            "Replay speed. Three modes:\n"
            "  <N>   fixed rate — N events per second (e.g. --speed 500)\n"
            "  <N>x  multiplier — N× faster than real time (e.g. --speed 10x)\n"
            "  max   unthrottled — as fast as Kafka accepts (load test / full replay)\n"
            "Default: $PRODUCER_SPEED or 1x (real-time)"
        ),
    )
    p.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "radiation.raw"),
        help="Kafka topic to publish to (default: $KAFKA_TOPIC or radiation.raw)",
    )
    p.add_argument(
        "--kafka-bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        help="Kafka bootstrap servers (default: $KAFKA_BOOTSTRAP_SERVERS or localhost:29092)",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("CSV_BATCH_SIZE", "10000")),
        help="Rows per sort batch in the CSV reader (default: 10000)",
    )
    p.add_argument(
        "--producer-batch-size",
        type=int,
        default=int(os.getenv("KAFKA_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))),
        help=(
            "Kafka producer batch.size in bytes — distinct from --batch-size (the CSV "
            f"sort batch). Larger = fewer, bigger requests. Default: $KAFKA_BATCH_SIZE "
            f"or {DEFAULT_BATCH_SIZE}. See ADR-012."
        ),
    )
    p.add_argument(
        "--producer-linger-ms",
        type=int,
        default=int(os.getenv("KAFKA_LINGER_MS", str(DEFAULT_LINGER_MS))),
        help=(
            "Kafka producer linger.ms — how long to wait to fill a batch before sending. "
            f"Default: $KAFKA_LINGER_MS or {DEFAULT_LINGER_MS}."
        ),
    )
    p.add_argument(
        "--compression-type",
        default=os.getenv("KAFKA_COMPRESSION_TYPE", DEFAULT_COMPRESSION_TYPE),
        choices=["lz4", "gzip", "snappy", "zstd", "none"],
        help=(
            "Kafka producer compression codec. Default: $KAFKA_COMPRESSION_TYPE or "
            f"{DEFAULT_COMPRESSION_TYPE}."
        ),
    )
    p.add_argument(
        "--skip-rows",
        type=int,
        default=0,
        help="Skip N rows from the start of the CSV (resume support)",
    )
    p.add_argument(
        "--metrics-port",
        type=int,
        default=int(os.getenv("METRICS_PORT", "8001")),
        help="Port for the Prometheus /metrics endpoint, 0 to disable (default: 8001)",
    )
    p.add_argument(
        "--movement-threshold-m",
        type=float,
        default=float(os.getenv("MOVEMENT_THRESHOLD_M", "200")),
        help=(
            "Radius in metres a sensor's readings may span and still be tagged "
            "'fixed'; beyond it the sensor is tagged 'mobile'. "
            "Default: $MOVEMENT_THRESHOLD_M or 200."
        ),
    )
    p.add_argument(
        "--preset",
        default=os.getenv("BACKFILL_PRESET"),
        choices=list(PRESETS),
        metavar="NAME",
        help=(
            f"Named time-window preset. Shorthand for --start/--end on well-known events. "
            f"Available: {', '.join(PRESETS)}. "
            f"Example: --preset fukushima  "
            f"(sets start={PRESETS['fukushima'][0]}, end={PRESETS['fukushima'][1]}). "
            f"Explicit --start/--end override the preset bounds. "
            f"Default: $BACKFILL_PRESET or None."
        ),
    )
    p.add_argument(
        "--start",
        default=os.getenv("BACKFILL_START"),
        metavar="DATETIME",
        help=(
            "ISO8601 lower bound on captured_at (inclusive). "
            "Only events at or after this timestamp are streamed. "
            "Example: --start 2011-03-11T00:00:00 (Fukushima event). "
            "Default: $BACKFILL_START or None (no lower bound)."
        ),
    )
    p.add_argument(
        "--end",
        default=os.getenv("BACKFILL_END"),
        metavar="DATETIME",
        help=(
            "ISO8601 upper bound on captured_at (inclusive). "
            "Only events at or before this timestamp are streamed. "
            "Example: --end 2011-04-11T23:59:59. "
            "Default: $BACKFILL_END or None (no upper bound)."
        ),
    )
    p.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    if not args.csv.exists():
        print(f"ERROR: CSV file not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    # Fail fast on a bad --speed (finding (g): `--speed 0` used to reach the
    # replay loop and die there with a ZeroDivisionError).
    try:
        _parse_speed(args.speed)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    start_ts = _parse_ts(args.start) if args.start else None
    end_ts = _parse_ts(args.end) if args.end else None
    if args.preset:
        preset_start, preset_end = PRESETS[args.preset]
        # Explicit --start / --end take priority over the preset.
        start_ts = start_ts or preset_start
        end_ts = end_ts or preset_end
        logging.getLogger(__name__).info(
            "Preset %r: start=%s end=%s", args.preset, start_ts, end_ts
        )

    run(
        csv_path=args.csv,
        bootstrap_servers=args.kafka_bootstrap_servers,
        topic=args.topic,
        speed=args.speed,
        batch_size=args.batch_size,
        skip_rows=args.skip_rows,
        metrics_port=args.metrics_port,
        start_ts=start_ts,
        end_ts=end_ts,
        movement_threshold_m=args.movement_threshold_m,
        producer_batch_size=args.producer_batch_size,
        producer_linger_ms=args.producer_linger_ms,
        compression_type=args.compression_type,
    )


if __name__ == "__main__":
    main()
