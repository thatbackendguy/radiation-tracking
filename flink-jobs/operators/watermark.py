"""
watermark.py — event-time watermark strategy on ``captured_at``.

Enforces the ordering contract from approach.md / guidelines.md H8: the producer
streams in ``uploaded_at`` order with no pre-sorting, and Flink re-establishes
event-time order from ``captured_at`` using bounded-out-of-orderness watermarks.

``Duration`` / ``WatermarkStrategy`` are imported lazily inside the strategy
builders so they are only needed when the job runs inside the Flink container;
the ``TimestampAssigner`` base falls back to a stub when PyFlink is absent (same
pattern as ``geo_window`` / ``geo_bucket``) so the assigners — whose per-event
``extract_timestamp`` is pure ``captured_at`` maths from ``timestamps.py`` — can
be unit-tested under plain pytest without a Flink runtime.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # annotation-only; never imported at runtime
    from pyflink.common import WatermarkStrategy

try:
    from pyflink.common.watermark_strategy import TimestampAssigner
except ImportError:  # running tests outside the Flink Docker image

    class TimestampAssigner:  # type: ignore[no-redef]
        """Stub base so this module imports without PyFlink; the real base is in PyFlink."""

        def extract_timestamp(self, value, record_timestamp):
            raise NotImplementedError


from operators.timestamps import captured_at_millis, extract_captured_at_millis

logger = logging.getLogger(__name__)

# Default bounded out-of-orderness — events may arrive up to this late relative
# to the current watermark before they are treated as late data.
DEFAULT_MAX_OUT_OF_ORDERNESS_SECONDS = 30
# Mark a source partition idle after this long with no data so it stops holding
# back the watermark (a sparse sensor must not stall windowed operators).
DEFAULT_IDLE_TIMEOUT_SECONDS = 60


class CapturedAtTimestampAssigner(TimestampAssigner):
    """Assigns event time from the ``captured_at`` field of each JSON event string.

    The assigner runs at the Kafka source, before the job drops malformed
    records, so it must tolerate poison-pill payloads: a parse failure returns
    ``0`` (epoch) instead of raising. A timestamp of 0 cannot advance the
    bounded-out-of-orderness watermark, and the bad record is discarded
    downstream by the job's ``_parse_record`` guard anyway.
    """

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            return extract_captured_at_millis(value)
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("could not extract captured_at for watermark: %s", exc)
            return 0


class DictCapturedAtTimestampAssigner(TimestampAssigner):
    """Assigns event time from the ``captured_at`` field of an already-parsed event dict.

    The source assigner (``CapturedAtTimestampAssigner``) works on raw JSON strings;
    this one works on the dicts flowing through the cleaned pipeline, so watermarks
    can be re-assigned downstream of the classifier's broadcast connect (which
    otherwise pins the propagated watermark at Long.MIN_VALUE — see
    ``geo_window.build_geo_aggregation``). By this point records are already cleaned,
    but it stays tolerant for symmetry: a parse failure returns ``0`` rather than
    raising and failing the task.
    """

    def extract_timestamp(self, value: dict, record_timestamp: int) -> int:
        try:
            return captured_at_millis(value)
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("could not extract captured_at for watermark: %s", exc)
            return 0


def build_watermark_strategy(
    max_out_of_orderness_seconds: int = DEFAULT_MAX_OUT_OF_ORDERNESS_SECONDS,
    idle_timeout_seconds: int = DEFAULT_IDLE_TIMEOUT_SECONDS,
) -> WatermarkStrategy:
    """Build a bounded-out-of-orderness watermark strategy keyed on ``captured_at``.

    For the raw JSON-string stream at the Kafka source.

    Args:
        max_out_of_orderness_seconds: allowed lateness before events are dropped.
        idle_timeout_seconds: idle-source detection timeout; pass 0 to disable.
    """
    return _with_assigner(
        CapturedAtTimestampAssigner(), max_out_of_orderness_seconds, idle_timeout_seconds
    )


def build_dict_watermark_strategy(
    max_out_of_orderness_seconds: int = DEFAULT_MAX_OUT_OF_ORDERNESS_SECONDS,
    idle_timeout_seconds: int = DEFAULT_IDLE_TIMEOUT_SECONDS,
) -> WatermarkStrategy:
    """Build the same strategy as ``build_watermark_strategy`` for dict event streams.

    Used to re-assign watermarks on the cleaned (dict) stream before the geo
    aggregation windows it; same bound/idleness so event-time behaviour is identical
    to the source.
    """
    return _with_assigner(
        DictCapturedAtTimestampAssigner(), max_out_of_orderness_seconds, idle_timeout_seconds
    )


def _with_assigner(
    assigner: TimestampAssigner,
    max_out_of_orderness_seconds: int,
    idle_timeout_seconds: int,
) -> WatermarkStrategy:
    from pyflink.common import Duration, WatermarkStrategy

    strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(max_out_of_orderness_seconds)
    )
    if idle_timeout_seconds > 0:
        strategy = strategy.with_idleness(Duration.of_seconds(idle_timeout_seconds))

    # with_timestamp_assigner MUST come last: in PyFlink, chaining .with_idleness() AFTER
    # .with_timestamp_assigner() drops the assigner, so records reach the windows with no
    # event-time timestamp and Flink falls back to the Kafka ingestion time — silently
    # bucketing windows by arrival instead of captured_at (H8 violation). Applying idleness
    # first and the assigner last preserves captured_at event time. Verified on a session
    # cluster: window bounds align to captured_at only with this order.
    return strategy.with_timestamp_assigner(assigner)
