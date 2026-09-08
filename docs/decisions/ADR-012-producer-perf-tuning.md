# ADR-012: producer throughput tuning (batch.size / linger.ms / compression)

Date: 2026-07-05
Status: Accepted
Authors: M1 (Aditya)

---

## Context

The Week-7 plan (approach.md) assigns M1:

> Producer perf tuning (`batch.size`/`linger.ms`) + load test report committed.

Until now `data-provider/producer.py` created its `KafkaProducer` with hard-coded
`linger_ms=5`, the kafka-python default `batch_size` (16 KiB), and no compression. For the
DO-droplet soak (and the full local replay) the producer should push meaningfully faster
without weakening the delivery contract (`acks=all`, no reordering — H8).

A load test measured end-to-end producer throughput across a small settings matrix; results
and reproduction steps are in [docs/perf/producer-load-test.md](../perf/producer-load-test.md).

---

## Decision

**Defaults** (all env-overridable, all exposed as CLI flags):

| Setting | Old | New | Env | CLI |
|---------|-----|-----|-----|-----|
| `batch.size` | 16384 (default) | **65536** | `KAFKA_BATCH_SIZE` | `--producer-batch-size` |
| `linger.ms` | 5 | **20** | `KAFKA_LINGER_MS` | `--producer-linger-ms` |
| `compression.type` | none | **lz4** | `KAFKA_COMPRESSION_TYPE` | `--compression-type` |

The delivery contract is unchanged: `acks="all"`, `retries=3`, one message per event keyed by
`sensor_id`, published in `uploaded_at` order. Tuning only affects **how** records are batched
and framed on the wire, never their order or durability.

**Naming:** the new `--producer-batch-size` is deliberately distinct from the existing
`--batch-size`, which is the CSV reader's *sort batch* (rows per in-memory ordering window) —
an unrelated knob. The producer-level params are prefixed `producer_*` in `run()` to keep the
two from being confused.

**New `--speed max` mode:** an unthrottled replay (never sleeps) used by the load test and by
full-dataset local runs. The existing `rate` / `multiplier` modes are unchanged.

**Dependency:** lz4 compression needs the `lz4` wheel, now pinned in
`data-provider/requirements.txt` / `pyproject.toml` (`lz4==4.3.3`). It ships pure wheels for
macOS / Windows / linux-amd64, so it does not regress the cross-platform build (CLAUDE.md).

---

## Consequences

- **~2.1× throughput** on the benchmark (8.7k → 18.7k ev/s, 200k events, single partition),
  driven almost entirely by compression: under `acks=all` on one partition the run is bound
  by bytes-per-round-trip, and lz4 roughly halves the payload. `batch.size` / `linger.ms`
  alone give ~10–15% and mainly let lz4 compress fuller batches.
- **+20 ms tail latency** from `linger.ms=20` — negligible against the ≤ 3 s
  threshold-to-map requirement, and the reason we did **not** take the marginally-faster
  128 KiB / 50 ms config.
- **CPU:** lz4 is deliberately the cheapest codec (vs gzip/zstd); compression cost did not
  offset the wire savings in the benchmark. gzip (stdlib, no extra dep) was rejected because
  it is CPU-heavy and tends to *reduce* throughput at this event size.
- Cloud scaling lever is orthogonal: raise `KAFKA_NUM_PARTITIONS` to parallelise beyond the
  single-partition ceiling the benchmark hit.

---

## Alternatives considered

- **gzip compression** — no new dependency (stdlib), but higher CPU and typically lower
  throughput for small JSON records; rejected in favour of lz4.
- **Leave compression off, only raise batch/linger** — only ~15% faster; leaves the biggest
  lever (wire bytes) on the table.
- **128 KiB / 50 ms / lz4** — ~7% faster than the chosen default but adds latency for no
  meaningful gain given the 3 s UI budget.
