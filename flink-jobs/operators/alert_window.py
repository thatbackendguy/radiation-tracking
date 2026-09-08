"""
alert_window.py — sustained-high alert detection as a keyed event-time window operator.

Wires the alert detection into the job graph: key each classified event by
``sensor_id`` and slide fixed event-time windows (default 5 min length, 1 min
slide — FLINK_ALERT_WINDOW_SECONDS / FLINK_ALERT_SLIDE_SECONDS) over the watermarked
stream. Each fired window is reduced to at most one candidate alert by the pure
``detect_sustained_high`` (operators/alert_detection), which this ProcessWindowFunction
wraps with the window bounds; windows without enough DANGER readings yield nothing.

A *sliding* window (vs the geo aggregation's tumbling window) is used so a burst of
DANGER readings is caught promptly and on every overlapping window it spans — the
overlap-driven re-firing is then collapsed downstream by the keyed cooldown dedup
(operators/alert_dedup). Imports PyFlink lazily so the module imports under plain
pytest; the pure reducer is unit-tested in operators/alert_detection, and the
event-time windowing contract in tests/integration/test_alert_window.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Iterable, Iterator

try:
    from pyflink.datastream.functions import ProcessWindowFunction
except ImportError:  # running tests outside the Flink Docker image

    class ProcessWindowFunction:  # type: ignore[no-redef]
        def process(self, key, context, elements):
            raise NotImplementedError


from operators.alert_detection import detect_sustained_high

# Sliding event-time window geometry (seconds). Length is how far back "sustained"
# looks; slide is how often the condition is re-evaluated. Read once at import;
# override via env for tuning. See docs/decisions/ADR-005.
_DEFAULT_WINDOW_SECONDS = int(os.environ.get("FLINK_ALERT_WINDOW_SECONDS", "300"))
_DEFAULT_SLIDE_SECONDS = int(os.environ.get("FLINK_ALERT_SLIDE_SECONDS", "60"))


def _millis_to_iso(epoch_millis: int) -> str:
    """Render a Flink epoch-millis window bound as a UTC ISO8601 string."""
    return datetime.fromtimestamp(epoch_millis / 1000, tz=timezone.utc).isoformat()


class SustainedHighWindowFunction(ProcessWindowFunction):
    """Reduce one sensor's events in one sliding window into a candidate alert, if any."""

    def process(
        self,
        key: str,
        context: "ProcessWindowFunction.Context",
        elements: Iterable[dict],
    ) -> Iterator[dict]:
        window = context.window()
        alert = detect_sustained_high(
            list(elements),
            sensor_id=key,
            window_start=_millis_to_iso(window.start),
            window_end=_millis_to_iso(window.end),
        )
        # Windows below the breach threshold produce no alert — emit nothing rather
        # than a None the sink would choke on.
        if alert is not None:
            yield alert


def build_alert_detection(
    clean_stream,
    window_seconds: int = _DEFAULT_WINDOW_SECONDS,
    slide_seconds: int = _DEFAULT_SLIDE_SECONDS,
):
    """Wire the sustained-high alert detection onto the classified, watermarked clean stream.

    Args:
        clean_stream: ``DataStream[dict]`` of classified events (carries event-time
            watermarks from the Kafka source).
        window_seconds: sliding event-time window length.
        slide_seconds: how often the window slides.

    Returns:
        ``DataStream[dict]`` of candidate alerts, ready for the cooldown dedup and
        then the ``radiation.alerts`` KafkaSink.
    """
    from pyflink.common import Time
    from pyflink.common.typeinfo import Types
    from pyflink.datastream.window import SlidingEventTimeWindows

    from operators.watermark import build_dict_watermark_strategy

    # Re-assign event-time watermarks before windowing — same reason as the geo
    # aggregation (operators/geo_window.build_geo_aggregation): clean_stream is the
    # output of the classifier's broadcast connect, whose config side uses
    # no_watermarks(), so the propagated watermark is pinned at Long.MIN_VALUE and
    # these sliding windows would never fire. Re-deriving the watermark from
    # captured_at on the dict stream detaches the alert windows from that stalled
    # input. Do NOT remove without restoring watermark progress. See docs/decisions/ADR-005.
    watermarked = clean_stream.assign_timestamps_and_watermarks(build_dict_watermark_strategy())

    return (
        watermarked.key_by(lambda event: event["sensor_id"], key_type=Types.STRING())
        .window(
            SlidingEventTimeWindows.of(Time.seconds(window_seconds), Time.seconds(slide_seconds))
        )
        .process(SustainedHighWindowFunction())
    )
