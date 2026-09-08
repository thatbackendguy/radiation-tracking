"""
geo_window.py — geo-bucket aggregation as a keyed event-time window operator.

Wires the geo aggregation into the job graph: enrich each classified event with its
geohash cell (operators/geo_bucket), key by that cell, and tumble fixed event-time
windows (default 60s, FLINK_AGG_WINDOW_SECONDS) over the watermarked stream. Each
fired window is reduced to one rich-stats blob by the pure ``aggregate_bucket``
(operators/geo_aggregation), which this ProcessWindowFunction wraps with the window
bounds. Imports PyFlink lazily so the module imports under plain pytest; the pure
reducer is unit-tested in operators/geo_aggregation, and the event-time windowing
contract in tests/integration/test_event_time_windows.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Iterator

try:
    from pyflink.datastream.functions import ProcessWindowFunction
except ImportError:  # running tests outside the Flink Docker image

    class ProcessWindowFunction:  # type: ignore[no-redef]
        def process(self, key, context, elements):
            raise NotImplementedError


from operators.geo_aggregation import _DEFAULT_WINDOW_SECONDS, aggregate_bucket
from operators.geo_bucket import GeoBucketEnricher


def _millis_to_iso(epoch_millis: int) -> str:
    """Render a Flink epoch-millis window bound as a UTC ISO8601 string."""
    return datetime.fromtimestamp(epoch_millis / 1000, tz=timezone.utc).isoformat()


class GeoBucketWindowFunction(ProcessWindowFunction):
    """Reduce one geohash cell's events in one tumbling window into an aggregate dict."""

    def process(
        self,
        key: str,
        context: "ProcessWindowFunction.Context",
        elements: Iterable[dict],
    ) -> Iterator[dict]:
        window = context.window()
        # geohash length == precision by construction (pygeohash returns exactly
        # ``precision`` chars), so the key itself carries the precision used.
        yield aggregate_bucket(
            list(elements),
            geohash=key,
            precision=len(key),
            window_start=_millis_to_iso(window.start),
            window_end=_millis_to_iso(window.end),
        )


def build_geo_aggregation(clean_stream, window_seconds: int = _DEFAULT_WINDOW_SECONDS):
    """Wire the geo-bucket aggregation onto the classified, watermarked clean stream.

    Args:
        clean_stream: ``DataStream[dict]`` of classified events (carries event-time
            watermarks from the Kafka source).
        window_seconds: tumbling event-time window length.

    Returns:
        ``DataStream[dict]`` of aggregated blobs, ready to serialise to the
        ``radiation.aggregated`` KafkaSink.
    """
    from pyflink.common import Time
    from pyflink.common.typeinfo import Types
    from pyflink.datastream.window import TumblingEventTimeWindows

    from operators.watermark import build_dict_watermark_strategy

    # Re-assign event-time watermarks before windowing. clean_stream is the output of
    # the classifier's broadcast connect (events.connect(config.broadcast)); the
    # config side uses no_watermarks(), and a two-input operator emits min(input
    # watermarks), so the propagated watermark is pinned at Long.MIN_VALUE and these
    # tumbling windows would never fire. Re-deriving the watermark from captured_at on
    # the dict stream here detaches the aggregation from that stalled input. Do NOT
    # remove without restoring watermark progress some other way. See docs/decisions/ADR-004.
    watermarked = clean_stream.assign_timestamps_and_watermarks(build_dict_watermark_strategy())

    return (
        watermarked.map(GeoBucketEnricher())
        .key_by(lambda event: event["geohash"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(window_seconds)))
        .process(GeoBucketWindowFunction())
    )
