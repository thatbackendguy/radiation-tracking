"""aggregated_sink — persists radiation.aggregated blobs to Postgres (ADR-018).

A standalone Kafka consumer service (same idiom as data-provider): it reads the
Flink-produced `radiation.aggregated` topic and idempotently upserts each
geohash-cell × window blob into a Postgres table, giving the project a queryable
historical archive of the aggregated data. It sits off to the side of the
real-time path — the backend WebSocket fan-out is unaffected.
"""
