"""
region_filter.py — area + timespan filter as a Flink broadcast operator.

Implements the ``area`` (bounding box) / ``timespan`` (event-time window) contract from
schemas/config_update.json: "Flink operators discard events outside this box/window".
The user-drawn region and timespan arrive on the ``config.updates`` broadcast stream
(same topic the classifier reads for thresholds); this operator keeps the current
filter in broadcast state and drops events that fall outside it, so the map's blobs
(radiation.aggregated) and alerts (radiation.alerts) only cover the selection.

Applied on the classified stream *before* the geo aggregation and alert branches; the
``radiation.clean`` sink stays unfiltered so the Backend keeps the full feed. With no
``area``/``timespan`` configured the operator passes everything through (a region/timespan
is opt-in). See docs/decisions/ADR-006.

The pure predicates (``in_area`` / ``in_timespan`` / ``event_passes``) carry no PyFlink
import and are unit-tested directly; ``RegionFilterFunction`` is the thin broadcast
wrapper (PyFlink imported with a stub fallback, same pattern as ``classifier.py``).
"""

from __future__ import annotations

import json
import logging
from typing import Iterator, Optional

try:
    from pyflink.common.typeinfo import Types
    from pyflink.datastream import BroadcastProcessFunction
    from pyflink.datastream.state import MapStateDescriptor
except ImportError:  # running tests outside the Flink Docker image

    class BroadcastProcessFunction:  # type: ignore[no-redef]
        def process_element(self, value, ctx):
            raise NotImplementedError

        def process_broadcast_element(self, value, ctx):
            raise NotImplementedError

    class MapStateDescriptor:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs) -> None: ...

    class Types:  # type: ignore[no-redef]
        @staticmethod
        def STRING():
            return None


from model.radiation_event import _parse_timestamp
from operators.timestamps import to_epoch_millis
from serde.config_update_deserializer import deserialize_config

logger = logging.getLogger(__name__)

# Broadcast state holding the current filter. ``area`` / ``timespan`` are stored as JSON
# strings (the raw config sub-objects, or the literal ``"null"`` when not set).
FILTER_BROADCAST_DESCRIPTOR = MapStateDescriptor("radiation-filter", Types.STRING(), Types.STRING())

_AREA_KEY = "area"
_TIMESPAN_KEY = "timespan"


def in_area(latitude: float, longitude: float, area: dict) -> bool:
    """Whether a point falls inside the inclusive lat/lon bounding box ``area``."""
    return (
        area["min_lat"] <= float(latitude) <= area["max_lat"]
        and area["min_lon"] <= float(longitude) <= area["max_lon"]
    )


def in_timespan(captured_at: str, timespan: dict) -> bool:
    """Whether ``captured_at`` falls in ``[start, end)`` (event-time, start-inclusive).

    Compared in epoch millis via the same ``captured_at`` parsing the watermarks use,
    so naive timestamps are treated as UTC and the bounds line up with event time.
    """
    captured = to_epoch_millis(_parse_timestamp(captured_at))
    start = to_epoch_millis(_parse_timestamp(timespan["start"]))
    end = to_epoch_millis(_parse_timestamp(timespan["end"]))
    return start <= captured < end


def event_passes(event: dict, area: Optional[dict], timespan: Optional[dict]) -> bool:
    """Whether ``event`` survives the current filter (no area/timespan ⇒ passes)."""
    if area is not None and not in_area(event["latitude"], event["longitude"], area):
        return False
    if timespan is not None and not in_timespan(event["captured_at"], timespan):
        return False
    return True


class RegionFilterFunction(BroadcastProcessFunction):
    """Drops events outside the broadcast area/timespan; passes all when none is set."""

    def _read(self, state, key: str) -> Optional[dict]:
        # Stored as JSON; the literal "null" (or an absent key) means "no constraint".
        if not state.contains(key):
            return None
        return json.loads(state.get(key))

    def process_element(
        self, value: dict, ctx: "BroadcastProcessFunction.ReadOnlyContext"
    ) -> Iterator[dict]:
        state = ctx.get_broadcast_state(FILTER_BROADCAST_DESCRIPTOR)
        area = self._read(state, _AREA_KEY)
        timespan = self._read(state, _TIMESPAN_KEY)
        if event_passes(value, area, timespan):
            yield value

    def process_broadcast_element(
        self, value: str, ctx: "BroadcastProcessFunction.Context"
    ) -> Iterator[dict]:
        # config.updates is a control stream; a malformed message must not crash the
        # task (it would restart on the same offset and crash-loop, freezing the
        # filtered branches). Validate first; only mutate state on success, keeping the
        # last-good filter when a bad message is dropped — mirrors classifier.py.
        try:
            deserialize_config(value)
            raw = json.loads(value)
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("skipping malformed config.updates message in region filter: %s", exc)
            return iter(())

        state = ctx.get_broadcast_state(FILTER_BROADCAST_DESCRIPTOR)
        # Store the raw sub-objects (or "null"); a config without area/timespan clears
        # the filter so the branches go back to passing everything. `or None` coerces a
        # falsy {} to None to match ConfigUpdate.from_dict's own truthiness: it validates
        # "area": {} as no-area, so storing the raw {} would slip past the guard and later
        # KeyError in in_area (`{}["min_lat"]`), crash-looping the task on a poison-pill.
        state.put(_AREA_KEY, json.dumps(raw.get("area") or None))
        state.put(_TIMESPAN_KEY, json.dumps(raw.get("timespan") or None))
        return iter(())


def apply_region_filter(events, config_stream):
    """Wire the area/timespan filter into the job graph.

    Args:
        events: ``DataStream[dict]`` of classified events (the clean stream).
        config_stream: ``DataStream[str]`` of ``config.updates`` JSON.

    Returns:
        ``DataStream[dict]`` of events inside the current area/timespan, ready to feed
        the geo aggregation and alert branches.
    """
    broadcast = config_stream.broadcast(FILTER_BROADCAST_DESCRIPTOR)
    return events.connect(broadcast).process(RegionFilterFunction())
