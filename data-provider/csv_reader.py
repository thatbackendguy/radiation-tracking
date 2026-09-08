"""
csv_reader.py — Pure-Python streaming reader for the Safecast CSV.

Reads the file in fixed-size row batches using csv.DictReader (no pandas).
Each batch is sorted by the ordering column before rows are yielded, keeping
memory proportional to batch_size rather than the full 29 GB file.

Schema normalisation:
  The canonical Safecast export uses display column names ("Captured Time",
  "Device ID", "Uploaded Time", …).  This reader maps them to the internal
  snake_case field names the mapper expects, so downstream code works on one
  stable schema regardless of the source header.  Already-internal headers
  (e.g. "captured_at") are accepted unchanged, so older lowercase fixtures
  keep working.

Ordering contract (from guidelines §7.3):
  Producer must push in uploaded_at order.  When the export lacks an upload
  timestamp we fall back to captured_at.  Per-batch sort gives Flink bounded
  out-of-orderness at chunk boundaries — well within the 30-second watermark
  slack configured on the Flink side.
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterator
from pathlib import Path

logger = logging.getLogger(__name__)

# Map a normalised header (lower-case, spaces → underscores) to the internal
# snake_case field name. Covers canonical Safecast display names *and* the
# internal names themselves, so both schemas read correctly.
_HEADER_ALIASES: dict[str, str] = {
    "captured_time": "captured_at",
    "captured_at": "captured_at",
    "uploaded_time": "uploaded_at",
    "uploaded_at": "uploaded_at",
    "latitude": "latitude",
    "longitude": "longitude",
    "value": "value",
    "unit": "unit",
    "location_name": "location_name",
    "device_id": "device_id",
    "sensor_id": "sensor_id",
    "md5sum": "md5sum",
    "height": "height",
    "surface": "surface",
    "loader_id": "measurement_import_id",
    "measurement_import_id": "measurement_import_id",
}

_ORDER_COL_PREFERRED = "uploaded_at"
_ORDER_COL_FALLBACK = "captured_at"

DEFAULT_BATCH_SIZE = 10_000


def _parse_ts(raw: str) -> str:
    """Normalise a raw captured_at string to a comparable ISO8601 form."""
    ts = raw.strip().replace("Z", "+00:00")
    if len(ts) > 10 and ts[10] == " ":
        ts = ts[:10] + "T" + ts[11:]
    return ts


def _in_span(row: dict[str, str], start_ts: str | None, end_ts: str | None) -> bool:
    """Return True if the row's captured_at falls within [start_ts, end_ts].

    Both the row timestamp and the caller-supplied bounds are normalised via
    _parse_ts before comparison so that space-separated dates, Z suffixes, and
    +00:00 offsets all compare correctly under lexicographic ordering.
    Callers should also pass pre-normalised bounds (done in __main__.main) so
    that the normalisation cost is paid once, not once per row.
    """
    if start_ts is None and end_ts is None:
        return True
    raw = row.get("captured_at", "")
    if not raw:
        return True  # missing timestamp — let downstream discard it
    ts = _parse_ts(raw)
    if start_ts is not None and ts < _parse_ts(start_ts):
        return False
    if end_ts is not None and ts > _parse_ts(end_ts):
        return False
    return True


def _norm_header(col: str) -> str:
    return col.strip().lower().replace(" ", "_")


def _build_rename_map(fieldnames: list[str]) -> dict[str, str]:
    """Map each recognised source column to its internal field name."""
    rename: dict[str, str] = {}
    for col in fieldnames:
        internal = _HEADER_ALIASES.get(_norm_header(col))
        if internal is not None:
            rename[col] = internal
    return rename


def _pick_order_col(internal_names: list[str]) -> str:
    if _ORDER_COL_PREFERRED in internal_names:
        return _ORDER_COL_PREFERRED
    logger.debug(
        "%r absent from CSV header; ordering by %r", _ORDER_COL_PREFERRED, _ORDER_COL_FALLBACK
    )
    return _ORDER_COL_FALLBACK


def _filter_row(row: dict[str, str], rename: dict[str, str]) -> dict[str, str]:
    """Keep only recognised columns, renamed to internal field names."""
    return {internal: row.get(src, "") for src, internal in rename.items()}


def stream_rows(
    csv_path: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    skip_rows: int = 0,
    start_ts: str | None = None,
    end_ts: str | None = None,
) -> Iterator[dict[str, str]]:
    """
    Yield one dict per CSV row, ordered by uploaded_at (fallback: captured_at).

    Rows are yielded as plain dicts with string values keyed by internal
    field names — type conversion is handled by mapper.py.  Only recognised
    columns are included; extras are silently dropped.

    Args:
        csv_path:  Path to the Safecast measurements CSV.
        batch_size: Rows buffered per sort pass (default 10 000).
        skip_rows:  Skip this many data rows from the start (resume support).
        start_ts:  ISO8601 lower bound on captured_at (inclusive). None = no lower bound.
        end_ts:    ISO8601 upper bound on captured_at (inclusive). None = no upper bound.

    Yields:
        dict[str, str] — one row, extra columns removed, values as raw strings.

    Raises:
        FileNotFoundError: if csv_path does not exist.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    logger.info(
        "Streaming %s (batch_size=%d, skip_rows=%d, start=%s, end=%s)",
        path,
        batch_size,
        skip_rows,
        start_ts,
        end_ts,
    )

    total_yielded = 0
    total_skipped = 0
    batch_num = 0
    order_col: str | None = None

    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)

        if reader.fieldnames is None:
            logger.warning("CSV has no header row — nothing to stream")
            return

        rename = _build_rename_map(list(reader.fieldnames))
        if not rename:
            logger.warning(
                "No recognised columns in CSV header %s — nothing will be yielded",
                list(reader.fieldnames),
            )
        order_col = _pick_order_col(list(rename.values()))
        logger.info("Recognised %d columns; order column: %r", len(rename), order_col)

        batch: list[dict[str, str]] = []

        for raw_row in reader:
            if total_skipped < skip_rows:
                total_skipped += 1
                continue

            filtered = _filter_row(raw_row, rename)
            if not _in_span(filtered, start_ts, end_ts):
                continue
            batch.append(filtered)

            if len(batch) >= batch_size:
                batch_num += 1
                batch.sort(key=lambda r: (not bool(r.get(order_col)), r.get(order_col) or ""))
                for row in batch:
                    yield row
                total_yielded += len(batch)
                logger.debug(
                    "batch %d: yielded %d rows (total %d)", batch_num, len(batch), total_yielded
                )
                batch = []

        # Flush the final partial batch.
        if batch:
            batch_num += 1
            batch.sort(key=lambda r: (not bool(r.get(order_col)), r.get(order_col) or ""))
            for row in batch:
                yield row
            total_yielded += len(batch)
            logger.debug(
                "final batch %d: yielded %d rows (total %d)", batch_num, len(batch), total_yielded
            )

    logger.info("Stream complete: %d rows yielded, %d rows skipped", total_yielded, total_skipped)
