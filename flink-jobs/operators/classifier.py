"""
classifier.py — threshold classifier as a Flink broadcast operator.

Connects the cleaned event stream with the ``config.updates`` broadcast stream so
that user-configured CPM thresholds are applied with low latency (broadcast state,
no shuffle). Each event is tagged SAFE / WARN / DANGER on its ``classification``
field and re-emitted for the ``radiation.clean`` sink.

Events flow through the pipeline as plain dicts (matching the filter/dedup
operators upstream), so this operator reads and writes dicts; the job serialises
them to JSON only at the sink. Imports PyFlink — only loaded when the job runs
inside the Flink container. The pure classification maths lives in
``classification.py`` and is unit-tested there.
"""

from __future__ import annotations

import logging
from typing import Iterator

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


from operators.classification import (
    DEFAULT_DANGER_THRESHOLD,
    DEFAULT_WARN_THRESHOLD,
    classify,
)
from serde.config_update_deserializer import deserialize_config

logger = logging.getLogger(__name__)

# Broadcast state holding the current thresholds. Stored as strings so the
# descriptor stays simple; parsed back to float on read.
CONFIG_BROADCAST_DESCRIPTOR = MapStateDescriptor(
    "radiation-thresholds", Types.STRING(), Types.STRING()
)

_WARN_KEY = "warn"
_DANGER_KEY = "danger"


class ThresholdClassifierFunction(BroadcastProcessFunction):
    """Tags each event with a classification using broadcast threshold config."""

    def process_element(
        self, value: dict, ctx: "BroadcastProcessFunction.ReadOnlyContext"
    ) -> Iterator[dict]:
        state = ctx.get_broadcast_state(CONFIG_BROADCAST_DESCRIPTOR)
        warn = float(state.get(_WARN_KEY)) if state.contains(_WARN_KEY) else DEFAULT_WARN_THRESHOLD
        danger = (
            float(state.get(_DANGER_KEY))
            if state.contains(_DANGER_KEY)
            else DEFAULT_DANGER_THRESHOLD
        )

        value["classification"] = classify(value.get("cpm"), warn, danger)
        yield value

    def process_broadcast_element(
        self, value: str, ctx: "BroadcastProcessFunction.Context"
    ) -> Iterator[dict]:
        # config.updates is a control stream; a single malformed message must not
        # crash the broadcast task. An unhandled exception fails the task and Flink
        # restarts it on the same offset, crash-looping forever on the poison-pill
        # message and freezing the whole pipeline (radiation.clean stops flowing).
        # So parse first; only mutate state on success, leaving the last-good
        # thresholds (or defaults) in place when a bad message is dropped — mirrors
        # the raw stream's _parse_record tolerance.
        try:
            config = deserialize_config(value)
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("skipping malformed config.updates message: %s", exc)
            return iter(())

        state = ctx.get_broadcast_state(CONFIG_BROADCAST_DESCRIPTOR)
        state.put(_WARN_KEY, str(config.cpm_warn_threshold))
        state.put(_DANGER_KEY, str(config.cpm_danger_threshold))
        return iter(())


def apply_classification(events, config_stream):
    """Wire the classifier into a job graph.

    Args:
        events: ``DataStream[dict]`` of cleaned events (downstream of the
            filter/dedup operators).
        config_stream: ``DataStream[str]`` of ``config.updates`` JSON.

    Returns:
        ``DataStream[dict]`` of classified events, ready to be serialised to the
        ``radiation.clean`` KafkaSink.
    """
    broadcast = config_stream.broadcast(CONFIG_BROADCAST_DESCRIPTOR)
    return events.connect(broadcast).process(ThresholdClassifierFunction())
