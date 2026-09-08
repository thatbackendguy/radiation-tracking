# Pin amd64: PyFlink's pemja dependency ships no arm64 wheel and the base image
# lacks the JNI headers to compile it from source, so the build fails on Apple
# Silicon. Forcing linux/amd64 builds (under emulation there); amd64/CI unaffected.
FROM --platform=linux/amd64 flink:1.19-java17

# Flink 1.19 externalises the Kafka connector, so the apache-flink wheel does not
# ship it. Bundle the matching fat jar into FLINK_HOME/lib so the radiation.raw
# KafkaSource resolves at runtime. Version suffix must track the Flink minor (-1.19).
ARG FLINK_KAFKA_CONNECTOR_VERSION=3.2.0-1.19

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev curl && \
    # PyFlink starts its Python worker process via `python`; the base image only
    # ships `python3`, so alias it or map operators fail with "Cannot run program python".
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/* && \
    curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar"

COPY requirements.txt /opt/flink/jobs/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/jobs/requirements.txt

# Shared checkpoint/savepoint root for the flink-state volume (ADR-015). The official
# entrypoint drops privileges to the `flink` user, but a named volume mounted here is
# created root-owned by Docker unless the image path already exists with the right
# owner — Docker copies this ownership onto a NEW (empty) volume on first mount.
# Without it, any FLINK_CHECKPOINT_DIR under /opt/flink/state fails at job init with
# "Failed to create directory for shared state" (JobMaster cannot start).
# NOTE: a volume created before this fix keeps its old root ownership — repair it once:
#   docker exec -u root flink-jm chown -R flink:flink /opt/flink/state
RUN mkdir -p /opt/flink/state/checkpoints /opt/flink/state/savepoints && \
    chown -R flink:flink /opt/flink/state

WORKDIR /opt/flink/jobs
COPY . .
