# Producer Load Test — batch.size / linger.ms / compression tuning

**Owner:** M1 (Data Provider) · **Week 7** · Related decision: [ADR-012](../decisions/ADR-012-producer-perf-tuning.md)

This is the committed load-test report for the Week-7 producer performance tuning. It
measures the end-to-end throughput of `data-provider/producer.py` (CSV → map → classify →
Kafka `send` → flush) across a small matrix of Kafka producer settings, so the tuned
defaults rest on measured numbers rather than guesswork.

## How to reproduce

```bash
docker compose up -d kafka                       # just the broker
pip install -r data-provider/requirements.txt    # includes lz4 for the default codec
python scripts/producer_load_test.py \
    --bootstrap-servers localhost:29092 --rows 200000
```

The harness ([`scripts/producer_load_test.py`](../../scripts/producer_load_test.py)) is pure
stdlib + the same `kafka-python` the producer uses (cross-platform, no bash/jq). It
synthesises the workload by cycling the committed `data/sample.csv` up to `--rows`, so it
needs neither the 29 GB dataset nor any host-specific path, and recreates a throwaway topic
(`radiation.raw.loadtest`) per config so measurements never accumulate. The producer is
driven unthrottled via the new `--speed max` mode.

## Results

Workload: **200,000 events**, single partition, `acks=all`, producer + broker on the same
host. Environment: macOS 26.5, Docker 29.4 (`apache/kafka:3.8.1`), Python 3.10, kafka-python
2.0.2. Two runs; the numbers below are run 1, with run 2 in parentheses where it moves the
takeaway — the relative ordering was stable across both.

| Config | batch.size | linger.ms | compression | events/s | MB/s | elapsed (s) |
|--------|-----------:|----------:|-------------|---------:|-----:|------------:|
| baseline (kafka-python defaults) | 16384 | 0  | none | 8,735 (9,724) | 3.05 | 21.4 |
| linger.ms=20                     | 16384 | 20 | none | 9,712 (9,672) | 3.39 | 19.3 |
| batch=64K + linger=20            | 65536 | 20 | none | 10,178 (10,252) | 3.55 | 18.4 |
| **batch=64K + linger=20 + lz4** *(default)* | 65536 | 20 | lz4 | **18,666 (18,657)** | 6.51 | 10.0 |
| batch=128K + linger=50 + lz4     | 131072 | 50 | lz4 | 19,953 (19,638) | 6.96 | 9.4 |

## Interpretation

- **Compression is the dominant lever (~2×).** Under `acks=all` on a single partition, each
  producer batch waits for the broker to acknowledge before the window frees up, so the run
  is bound by *bytes on the wire per round-trip*, not CPU. `lz4` roughly halves the payload
  → roughly doubles throughput (8.7k → 18.7k ev/s). This is the whole story of the speed-up.
- **`batch.size` / `linger.ms` alone are marginal (~10–15%).** Raising the batch to 64 KiB
  and letting the producer linger 20 ms coalesces more records per request, but without
  compression the wire cost still dominates (8.7k → 10.2k ev/s). They matter mostly as the
  *enablers* that let lz4 compress fuller batches.
- **128 KiB / 50 ms is only ~7% over the default** and buys it with extra latency. Not worth
  it for this project: the live map must reflect a threshold change within 3 s, so we keep
  `linger.ms` low (20 ms).

## Chosen defaults (ADR-012)

`batch.size=65536`, `linger.ms=20`, `compression.type=lz4` — captures essentially all of the
tuning gain (**~2.1× baseline**) while keeping added latency at 20 ms. All three are
env-overridable (`KAFKA_BATCH_SIZE` / `KAFKA_LINGER_MS` / `KAFKA_COMPRESSION_TYPE`) and
exposed as `--producer-batch-size` / `--producer-linger-ms` / `--compression-type`.

> Note: absolute numbers are for a single-partition, same-host dev setup and understate a
> multi-partition cloud broker (raise `KAFKA_NUM_PARTITIONS` to parallelise). The **relative**
> comparison — compression as the primary lever — is what drives the default.
