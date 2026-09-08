from __future__ import annotations

try:
    from pyflink.datastream import FilterFunction
except ImportError:  # running tests outside the Flink Docker image

    class FilterFunction:  # type: ignore[no-redef]
        def filter(self, event: dict) -> bool:
            raise NotImplementedError


class NullCPMFilter(FilterFunction):
    """Discards radiation events where cpm is null or missing."""

    def filter(self, event: dict) -> bool:
        return event.get("cpm") is not None
