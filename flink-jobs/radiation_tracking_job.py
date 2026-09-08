from __future__ import annotations

import logging
import os
from typing import Optional

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from model.radiation_event import RadiationEvent
from operators.alert_dedup import AlertCooldownOperator
from operators.alert_region_join import build_alert_region_join
from operators.alert_window import build_alert_detection
from operators.checkpointing import apply_checkpointing
from operators.classifier import apply_classification
from operators.cpm_range_filter import CPMRangeFilter
from operators.geo_window import build_geo_aggregation
from operators.kafka_config import apply_security
from operators.null_cpm_filter import NullCPMFilter
from operators.region_filter import apply_region_filter
from operators.rolling_stats import build_rolling_stats
from operators.sensor_dedup import SensorDedupOperator
from operators.trend_detector import build_trend_detection
from operators.watermark import build_watermark_strategy
from serde.radiation_aggregated_serializer import serialize_aggregated
from serde.radiation_alert_serializer import serialize_alert
from serde.radiation_event_deserializer import deserialize
from serde.radiation_event_serializer import serialize

logger = logging.getLogger(__name__)

RAW_TOPIC = "radiation.raw"
CLEAN_TOPIC = "radiation.clean"
AGGREGATED_TOPIC = "radiation.aggregated"
ALERTS_TOPIC = "radiation.alerts"
CONFIG_TOPIC = "config.updates"


def _console_sink_enabled() -> bool:
    """Whether to also print cleaned events to stdout (Week-2 sanity view, opt-in)."""
    return os.environ.get("FLINK_CONSOLE_SINK", "").strip().lower() in ("1", "true", "yes")


def _parse_record(raw: str) -> Optional[dict]:
    """Deserialise a radiation.raw JSON string into a dict, dropping poison-pill records.

    Returns the parsed dict, or None when the record is malformed. Parsing through
    RadiationEvent validates required fields and timestamps, but the plain dict keeps
    flowing — the filter operators and the sink work on dicts.

    A single malformed record must not crash the operator: an unhandled exception fails
    the task and Flink restarts it on the same offset, looping forever on the poison-pill
    record. So parse failures are logged and the record is dropped (callers filter None).
    """
    try:
        event = deserialize(raw)
        RadiationEvent.from_dict(event)
    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("skipping malformed radiation.raw record: %s", exc)
        return None
    return event


def _console_summary(event: dict) -> str:
    """One-line summary of a cleaned event for the optional debug console sink."""
    return (
        f"{event['sensor_id']} @ {event['captured_at']} "
        f"({event['latitude']:.4f}, {event['longitude']:.4f}) "
        f"cpm={event.get('cpm')} unit={event.get('unit')}"
    )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    apply_checkpointing(env)

    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    # apply_security() adds SASL_SSL properties from the environment for a managed cloud
    # broker (ADR-010); it is a no-op locally (plaintext), so docker compose is unchanged.
    source = apply_security(
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(RAW_TOPIC)
        .set_group_id("flink-radiation-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
    ).build()

    # Event-time watermarks on captured_at (bounded out-of-orderness 30s + idle-source
    # handling): the producer streams in uploaded_at order, so Flink re-establishes
    # captured_at order here (guidelines H8). Assigned on the raw JSON string at source.
    raw_stream = env.from_source(
        source,
        build_watermark_strategy(),
        "Radiation Kafka Source",
    )

    # Broadcast source for user-configured thresholds (config.updates). Carried as JSON
    # strings; the classifier parses them into broadcast state. no_watermarks(): config
    # is a control stream, not event-time data.
    config_source = apply_security(
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(CONFIG_TOPIC)
        .set_group_id("flink-config-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
    ).build()
    config_stream = env.from_source(
        config_source,
        WatermarkStrategy.no_watermarks(),
        "Config Updates Kafka Source",
    )

    # Cleaning pipeline: parse → drop malformed → filter null/empty CPM → CPM range
    # sanity check → key by sensor_id → drop duplicate readings (keyed md5sum state +
    # TTL). The result is the validated, deduped stream.
    deduped_stream = (
        raw_stream.map(_parse_record)
        .filter(lambda event: event is not None)
        .filter(NullCPMFilter())
        .filter(CPMRangeFilter())
        .key_by(lambda event: event["sensor_id"], key_type=Types.STRING())
        .process(SensorDedupOperator())
    )

    # Tag each event SAFE / WARN / DANGER from the config.updates broadcast thresholds
    # (defaults until the first config arrives). This classified stream is radiation.clean.
    clean_stream = apply_classification(deduped_stream, config_stream)

    # Serialise the validated dicts back to JSON and publish to radiation.clean for the
    # Backend consumer (M4) and downstream Flink operators (M3). AT_LEAST_ONCE pairs with
    # the 60s checkpointing above; the Backend deduplicates on its side if needed.
    clean_sink = apply_security(
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(CLEAN_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    ).build()
    # output_type=Types.STRING() is required: without it map() emits a pickled byte[] and
    # the sink's SimpleStringSchema cannot cast it to String, crash-looping the sink task.
    clean_stream.map(serialize, output_type=Types.STRING()).sink_to(clean_sink)

    # Area/timespan filter: drop classified events outside the user-selected bounding box
    # and event-time window (config.updates broadcast). The map-facing branches below
    # (aggregated blobs + alerts) consume this filtered view so they only cover the
    # selection; the radiation.clean sink above stays on the full clean_stream so the
    # Backend keeps the complete feed. With no area/timespan set it passes everything.
    filtered_stream = apply_region_filter(clean_stream, config_stream)

    # Geo-bucket aggregation: enrich each filtered event with its geohash cell, then
    # tumble per-cell event-time windows into rich-stats blobs (count, cpm avg/max/min,
    # per-class counts, centroid) for radiation.aggregated — the map heat/density overlay
    # and region filters consume these. A parallel branch off the filtered stream;
    # the radiation.clean sink above is unaffected.
    aggregated_stream = build_geo_aggregation(filtered_stream)

    # Cross-window enrichment: key the per-window blobs by geohash and attach a rolling
    # CPM average plus a z-score of this window against the cell's recent baseline, flagging
    # statistical anomalies (spike/dip in either direction — distinct from the monotonic
    # rising-trend alert). Additive only: it adds rolling_cpm_avg / cpm_zscore / anomaly to
    # each blob and preserves every existing field, so the aggregated sink and the
    # hot-region join below consume the enriched stream unchanged. See docs/decisions/ADR-009.
    aggregated_stream = build_rolling_stats(aggregated_stream)
    aggregated_sink = apply_security(
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(AGGREGATED_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    ).build()
    # output_type=Types.STRING() for the same reason as the clean sink above.
    aggregated_stream.map(serialize_aggregated, output_type=Types.STRING()).sink_to(aggregated_sink)

    # Sustained-high alerts: key the classified stream by sensor_id, slide event-time
    # windows over it and emit an alert when a sensor produces enough DANGER readings
    # (operators/alert_window), then collapse the overlapping sliding-window re-fires
    # into one alert per sensor per cooldown (operators/alert_dedup) before publishing
    # to radiation.alerts. Another parallel branch off the filtered stream; the clean
    # sink is unaffected. The Backend (M4) consumes these and fans them out to the map
    # client.
    alert_stream = (
        build_alert_detection(filtered_stream)
        .key_by(lambda alert: alert["sensor_id"], key_type=Types.STRING())
        .process(AlertCooldownOperator())
    )

    # Enrich each deduped alert with its geohash cell's recent stats (cpm avg/max, count,
    # worst classification, centroid) by joining against the aggregated blobs — an additive
    # "hot_region" block so the map can label the region around an alert, not just the
    # triggering sensor. Re-keys sensor_id → geohash; the aggregated_stream above is the
    # second join input. Backward-compatible: alerts without coordinates pass through with
    # hot_region None. See docs/decisions/ADR-007.
    enriched_alert_stream = build_alert_region_join(alert_stream, aggregated_stream)

    # Secondary alert stream: detect geohash cells whose cpm_avg is rising across N
    # aggregation windows (an early-warning trend, even below DANGER) and emit a
    # reason="rising-trend" alert per cell. Consumes the same in-job aggregated_stream (a
    # third fan-out of it, alongside the aggregated sink and the join) so there is no extra
    # Kafka round-trip. Unioned with the sustained-high alerts below so both land on
    # radiation.alerts through one sink. See docs/decisions/ADR-008.
    trend_alert_stream = build_trend_detection(aggregated_stream)
    combined_alert_stream = enriched_alert_stream.union(trend_alert_stream)

    alerts_sink = apply_security(
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(ALERTS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    ).build()
    # output_type=Types.STRING() for the same reason as the clean sink above.
    combined_alert_stream.map(serialize_alert, output_type=Types.STRING()).sink_to(alerts_sink)

    # Opt-in stdout sink (FLINK_CONSOLE_SINK=1) keeps the Week-2 sanity view available.
    if _console_sink_enabled():
        clean_stream.map(_console_summary).print()

    env.execute("Radiation Tracking Flink Job")


if __name__ == "__main__":
    main()
