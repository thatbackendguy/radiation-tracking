"""
producer_load_test.py — measure Data Provider throughput across batch.size / linger.ms.

Drives data-provider/producer.py unthrottled (``--speed max``) against a real broker and
reports end-to-end producer throughput (events/s and MB/s) for a small matrix of Kafka
producer settings, so the batch.size / linger.ms / compression defaults (ADR-012) rest on
measured numbers rather than folklore. Emits a Markdown table (stdout, and to --out for the
committed report at docs/perf/producer-load-test.md).

Cross-platform (pure Python stdlib + the same kafka-python the producer uses; no bash/jq).
It synthesises the load CSV by cycling the committed data/sample.csv up to --rows, so it
needs neither the 29 GB dataset nor any host-specific path:

    docker compose up -d kafka          # just the broker
    pip install -r data-provider/requirements.txt
    python scripts/producer_load_test.py --bootstrap-servers localhost:29092 --rows 200000

Every config publishes to a throwaway topic (default radiation.raw.loadtest) that is
recreated per run so measurements don't accumulate; it never touches radiation.raw.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import tempfile
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_CSV = REPO_ROOT / "data" / "sample.csv"

# (label, batch.size bytes, linger.ms, compression). The first row is the kafka-python
# out-of-the-box baseline so the report shows the delta the tuning buys.
DEFAULT_CONFIGS = [
    ("baseline (kafka-python defaults)", 16_384, 0, "none"),
    ("linger.ms=20", 16_384, 20, "none"),
    ("batch=64K + linger=20", 65_536, 20, "none"),
    ("batch=64K + linger=20 + lz4", 65_536, 20, "lz4"),
    ("batch=128K + linger=50 + lz4", 131_072, 50, "lz4"),
]


def _load_producer():
    """Import producer.py from the hyphenated data-provider/ dir as a package submodule.

    Same shim as tests/test_producer_integration.py: producer.py uses relative imports, so
    it must load under a real package. We register a synthetic package pointing at the dir.
    """
    dp_dir = REPO_ROOT / "data-provider"
    pkg_name = "data_provider_pkg"
    if pkg_name not in sys.modules:
        pkg = types.ModuleType(pkg_name)
        pkg.__path__ = [str(dp_dir)]
        sys.modules[pkg_name] = pkg
    spec = importlib.util.spec_from_file_location(f"{pkg_name}.producer", dp_dir / "producer.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def synthesise_csv(rows: int, dest: Path) -> int:
    """Write ``rows`` data lines to ``dest`` by cycling data/sample.csv; return lines written.

    Speed is ``max`` (timestamps ignored) and the CSV reader sorts per batch, so cycling the
    sample is a faithful throughput workload without shipping the full dataset.
    """
    lines = SAMPLE_CSV.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise RuntimeError(f"{SAMPLE_CSV} is empty")
    header, data = lines[0], lines[1:]
    if not data:
        raise RuntimeError(f"{SAMPLE_CSV} has no data rows")
    with dest.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(header + "\n")
        for i in range(rows):
            fh.write(data[i % len(data)] + "\n")
    return rows


def _recreate_topic(bootstrap_servers: str, topic: str, partitions: int) -> None:
    """Delete (if present) and create ``topic`` so each config measures from empty."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        try:
            admin.delete_topics([topic])
        except UnknownTopicOrPartitionError:
            pass
        # Deletion is async; retry create until the old topic is gone.
        import time

        for _ in range(30):
            try:
                admin.create_topics(
                    [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
                )
                return
            except TopicAlreadyExistsError:
                time.sleep(1)
        raise RuntimeError(f"could not recreate topic {topic} (deletion did not settle)")
    finally:
        admin.close()


def format_report(rows: int, partitions: int, results: list[dict]) -> str:
    """Render the results as the Markdown table committed to the perf report."""
    lines = [
        "| Config | batch.size | linger.ms | compression | events/s | MB/s | elapsed (s) |",
        "|--------|-----------:|----------:|-------------|---------:|-----:|------------:|",
    ]
    for r in results:
        lines.append(
            f"| {r['label']} | {r['batch_size']} | {r['linger_ms']} | {r['compression']} "
            f"| {r['events_per_second']:,.0f} | {r['mb_per_second']:.2f} | {r['elapsed']:.1f} |"
        )
    baseline = results[0]["events_per_second"] if results else 0
    best = max(results, key=lambda r: r["events_per_second"]) if results else None
    footer = ""
    if best and baseline:
        footer = (
            f"\nBest: **{best['label']}** at {best['events_per_second']:,.0f} ev/s — "
            f"{best['events_per_second'] / baseline:.2f}× the kafka-python baseline "
            f"({rows:,} events, {partitions} partition(s))."
        )
    return "\n".join(lines) + "\n" + footer


def run_load_test(
    bootstrap_servers: str,
    rows: int,
    topic: str,
    partitions: int,
    configs: list[tuple[str, int, int, str]],
) -> list[dict]:
    producer = _load_producer()
    results: list[dict] = []
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = Path(tmp) / "loadtest.csv"
        synthesise_csv(rows, csv_path)
        for label, batch_size, linger_ms, compression in configs:
            _recreate_topic(bootstrap_servers, topic, partitions)
            print(f"\n=== {label} (batch={batch_size} linger={linger_ms} {compression}) ===")
            metrics = producer.run(
                csv_path=csv_path,
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                speed="max",
                metrics_port=0,  # no /metrics endpoint during the benchmark
                producer_batch_size=batch_size,
                producer_linger_ms=linger_ms,
                compression_type=compression,
            )
            snap = metrics.snapshot()
            results.append(
                {
                    "label": label,
                    "batch_size": batch_size,
                    "linger_ms": linger_ms,
                    "compression": compression,
                    "events_per_second": snap.events_per_second,
                    "mb_per_second": (
                        (snap.bytes_sent / 1_000_000) / snap.elapsed_seconds
                        if snap.elapsed_seconds > 0
                        else 0.0
                    ),
                    "elapsed": snap.elapsed_seconds,
                    "sent": snap.sent,
                }
            )
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        help="Kafka bootstrap (default: $KAFKA_BOOTSTRAP_SERVERS or localhost:29092)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=200_000,
        help="Synthetic event count per config (default: 200000)",
    )
    parser.add_argument(
        "--topic", default="radiation.raw.loadtest", help="Throwaway benchmark topic"
    )
    parser.add_argument("--partitions", type=int, default=1, help="Benchmark topic partitions")
    parser.add_argument(
        "--out", type=Path, default=None, help="Optional path to append the Markdown table to"
    )
    args = parser.parse_args(argv)

    try:
        results = run_load_test(
            args.bootstrap_servers, args.rows, args.topic, args.partitions, DEFAULT_CONFIGS
        )
    except Exception as exc:  # surface a clean message — the broker is often just not up
        print(f"load test aborted: {exc}", file=sys.stderr)
        return 1

    report = format_report(args.rows, args.partitions, results)
    print("\n" + report)
    if args.out is not None:
        args.out.write_text(report + "\n", encoding="utf-8")
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
