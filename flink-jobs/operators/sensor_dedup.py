from __future__ import annotations

import os
from collections.abc import Iterator

try:
    from pyflink.datastream.functions import KeyedProcessFunction
except ImportError:  # running tests outside the Flink Docker image

    class KeyedProcessFunction:  # type: ignore[no-redef]
        def open(self, runtime_context) -> None: ...

        def process_element(self, value, ctx):
            raise NotImplementedError


# Default lifetime of a remembered reading. A fixed sensor that re-reports the same
# measurement within this window is deduped; after it, the entry expires so per-sensor
# state cannot grow unbounded over a long-running job. Override via env for tuning.
_DEFAULT_TTL_SECONDS = int(os.environ.get("FLINK_DEDUP_TTL_SECONDS", "3600"))


def dedup_key(event: dict) -> str:
    """Identity used to detect a duplicate reading of the same sensor measurement.

    Prefer the producer-supplied ``md5sum`` — a hash of the original CSV row that M1
    emits precisely for deduplication. Fall back to ``captured_at`` when md5sum is
    absent so two distinct readings of the same sensor stay distinguishable. The stream
    is keyed by ``sensor_id`` before this operator, so the fallback never conflates
    readings from different sensors that happen to share a timestamp.
    """
    md5sum = event.get("md5sum")
    if md5sum:
        return str(md5sum)
    return f"captured_at:{event.get('captured_at')}"


class SensorDedupOperator(KeyedProcessFunction):
    """Drops duplicate readings emitted by a fixed sensor.

    Fixed sensors re-report the same measurement; this keeps a per-sensor MapState of
    the dedup keys (md5sum) already seen and suppresses repeats. The stream must be
    keyed by ``sensor_id`` upstream so the state is scoped per sensor.
    """

    def __init__(self, ttl_seconds: int = _DEFAULT_TTL_SECONDS) -> None:
        self._ttl_seconds = ttl_seconds
        self._seen = None

    def open(self, runtime_context) -> None:
        from pyflink.common.time import Time
        from pyflink.common.typeinfo import Types
        from pyflink.datastream.state import MapStateDescriptor, StateTtlConfig

        descriptor = MapStateDescriptor("seen-readings", Types.STRING(), Types.BOOLEAN())
        ttl_config = (
            StateTtlConfig.new_builder(Time.seconds(self._ttl_seconds))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanup_full_snapshot()
            .build()
        )
        descriptor.enable_time_to_live(ttl_config)
        self._seen = runtime_context.get_map_state(descriptor)

    def process_element(self, event: dict, ctx) -> Iterator[dict]:
        key = dedup_key(event)
        if self._seen.contains(key):
            return  # already seen this reading for this sensor — drop it
        self._seen.put(key, True)
        yield event
