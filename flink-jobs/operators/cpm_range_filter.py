from __future__ import annotations

try:
    from pyflink.datastream import FilterFunction
except ImportError:  # running tests outside the Flink Docker image

    class FilterFunction:  # type: ignore[no-redef]
        def filter(self, event: dict) -> bool:
            raise NotImplementedError


_CPM_MIN: float = 0.0
_CPM_MAX: float = 1_000_000.0


class CPMRangeFilter(FilterFunction):
    """Discards events with physically impossible CPM values (negative or > 1,000,000).

    Applied after NullCPMFilter, so cpm is guaranteed non-null at this point.
    The None guard is kept for safety when the operator is used in isolation.
    """

    def filter(self, event: dict) -> bool:
        cpm = event.get("cpm")
        if cpm is None:
            return True
        try:
            return _CPM_MIN <= float(cpm) <= _CPM_MAX
        except (TypeError, ValueError):
            return False
