# ADR-001: JSON Schema vs Apache Avro for Kafka Events

Date: 2026-06-01  
Status: Accepted  
Author: Yash Prakashbhai Prajapati  
Reviewers: Aditya Gupta, Sahil Sajwan, Jay Shiroya   

---

## Context

The team must choose a serialisation format for events on the Kafka topics
`radiation.raw`, `radiation.clean`, `radiation.aggregated`, and `radiation.alerts`.
The two primary candidates are **JSON (with JSON Schema validation)** and
**Apache Avro (with a Schema Registry)**.

The project is a 5-person academic system built over 8 weeks with 
Python stack.

---

## Decision

**Use JSON with JSON Schema validation** for Weeks 1–6, with a designated
checkpoint in Week 6 to reassess if throughput becomes a bottleneck.

---

## Rationale

### Arguments for JSON Schema

| Factor | JSON Schema | Avro |
|--------|-------------|------|
| Toolchain setup | Zero extra services | Requires Schema Registry (Confluent or Apicurio) |
| Debugging | `kafka-console-consumer` readable | Binary wire format — needs `avro-tools` |
| Python producer code | `jsonschema` pip package | `fastavro` + registry auth |
| Flink deserialisation | `ObjectMapper` / `JsonNode` | Generated POJOs or `GenericRecord` |
| Schema evolution | Field-level, documented in JSON | Built-in compatibility modes |
| Throughput | ~15–20 % larger payload | Compact binary |
| Learning curve | All 5 members familiar | New to most members |

The Safecast dataset is replayed at configurable speed, not at raw ingestion
rate, so wire payload size is not a bottleneck for this project. The extra
Schema Registry service would complicate `docker compose up` and add a
dependency that slows down local development iteration.

### Arguments for Avro (considered but deferred)

- Schema evolution guarantees are stronger and enforced by the registry.
- Binary format is significantly smaller for high-throughput production systems.
- Flink's `AvroDeserializationSchema` is a first-class API.

These advantages matter at production scale; for an 8-week academic prototype
they introduce unnecessary operational overhead.

---

## Consequences

- `schemas/radiation_event.json` (JSON Schema draft-07) is the authoritative
  contract for all Kafka events.
- Any change to the schema requires an MR with M1, M2, and M4 as reviewers
  (Architecture Contract §7.1 of guidelines).
- The Flink pipeline validates incoming events against the schema using a
  custom `SchemaValidator` utility before processing.
- If Week 6 profiling shows payload size is a bottleneck, the team will
  evaluate migrating to Avro and open a new ADR.

---

## Alternatives Rejected

| Alternative | Reason rejected |
|-------------|----------------|
| Protocol Buffers | No native Kafka / Flink tooling as smooth as Avro; adds `protoc` compile step |
| MessagePack | Even less familiar to the team; offers no schema enforcement |
| Raw CSV pass-through | No type safety, no schema validation, harder to consume in Flink |
