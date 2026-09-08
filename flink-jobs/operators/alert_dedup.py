"""
alert_dedup.py — cooldown dedup for sustained-high alerts.

The sliding alert window (operators/alert_window) re-fires on every overlapping
window a DANGER burst spans, so a single sustained hotspot yields a candidate alert
per slide. This keyed operator collapses that burst into one alert per sensor per
cooldown period: it keeps the ``window_end`` (event time) of the last *emitted*
alert in per-sensor ValueState and suppresses any later candidate whose window ends
within ``cooldown_seconds`` of it. The stream must be keyed by ``sensor_id`` upstream
so the state is scoped per sensor — mirroring SensorDedupOperator.

Cooldown is measured in *event time* (the alerts' window bounds), not wall-clock, so
it behaves identically on replay/backfill regardless of how fast the producer streams.
The pure helpers (``alert_window_end_millis`` / ``is_within_cooldown``) are unit-tested
without PyFlink; ``AlertCooldownOperator`` is the thin Flink wrapper. See
docs/decisions/ADR-005.
"""

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


from model.radiation_event import _parse_timestamp
from operators.timestamps import to_epoch_millis

# How long (event-time seconds) to suppress repeat alerts for the same sensor after
# one fires. With the default 5 min window / 1 min slide, a steady hotspot would emit
# ~5 overlapping candidates per burst; a 10 min cooldown collapses those to one alert
# every 10 minutes of event time. Read once at import; override via env for tuning.
_DEFAULT_COOLDOWN_SECONDS = int(os.environ.get("FLINK_ALERT_COOLDOWN_SECONDS", "600"))


def alert_window_end_millis(alert: dict) -> int:
    """Epoch millis of an alert's ``window_end`` — the event-time anchor for cooldown."""
    return to_epoch_millis(_parse_timestamp(alert["window_end"]))


def is_within_cooldown(
    last_emitted_end_millis: int, candidate_end_millis: int, cooldown_seconds: int
) -> bool:
    """Whether a candidate alert falls inside the cooldown of the last emitted alert.

    True (suppress) when the candidate's window ends less than ``cooldown_seconds``
    after the last emitted alert's window end. Late candidates whose window ends at or
    before the last emitted one are also suppressed (they describe an already-reported
    burst), so the comparison is on the absolute event-time gap.
    """
    return candidate_end_millis - last_emitted_end_millis < cooldown_seconds * 1000


class AlertCooldownOperator(KeyedProcessFunction):
    """Suppresses repeat sustained-high alerts for the same sensor within a cooldown.

    Keeps the last emitted alert's window-end (epoch millis) per sensor in ValueState;
    the stream must be keyed by ``sensor_id`` upstream so the state is scoped per sensor.
    """

    def __init__(self, cooldown_seconds: int = _DEFAULT_COOLDOWN_SECONDS) -> None:
        self._cooldown_seconds = cooldown_seconds
        self._last_emitted_end = None

    def open(self, runtime_context) -> None:
        from pyflink.common.time import Time
        from pyflink.common.typeinfo import Types
        from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

        descriptor = ValueStateDescriptor("last-alert-window-end", Types.LONG())
        # Expire idle sensors' cooldown state so it cannot grow unbounded over a long
        # job; TTL is generously longer than the cooldown so it never clears an
        # in-effect cooldown early.
        ttl_config = (
            StateTtlConfig.new_builder(Time.seconds(max(self._cooldown_seconds * 10, 3600)))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanup_full_snapshot()
            .build()
        )
        descriptor.enable_time_to_live(ttl_config)
        self._last_emitted_end = runtime_context.get_state(descriptor)

    def process_element(self, alert: dict, ctx) -> Iterator[dict]:
        candidate_end = alert_window_end_millis(alert)
        last_end = self._last_emitted_end.value()
        if last_end is not None and is_within_cooldown(
            last_end, candidate_end, self._cooldown_seconds
        ):
            return  # still cooling down from the last alert for this sensor — drop it
        self._last_emitted_end.update(candidate_end)
        yield alert
