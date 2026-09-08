"""
Unit tests for scripts/producer_load_test.py pure helpers (M1, Week 7).

The benchmark itself needs a live broker; the CSV synthesis and Markdown rendering it is
built on are pure and covered here. The module imports without kafka-python (the admin
import is function-local), which is what CI runs with.
"""

from __future__ import annotations

from pathlib import Path

from scripts.producer_load_test import format_report, synthesise_csv


class TestSynthesiseCsv:
    def test_writes_header_plus_requested_rows(self, tmp_path: Path) -> None:
        dest = tmp_path / "load.csv"
        written = synthesise_csv(50, dest)
        lines = dest.read_text(encoding="utf-8").splitlines()
        assert written == 50
        assert len(lines) == 51  # header + 50 data rows

    def test_header_matches_sample(self, tmp_path: Path) -> None:
        dest = tmp_path / "load.csv"
        synthesise_csv(3, dest)
        header = dest.read_text(encoding="utf-8").splitlines()[0]
        sample_header = (
            (Path(__file__).resolve().parents[2] / "data" / "sample.csv")
            .read_text(encoding="utf-8")
            .splitlines()[0]
        )
        assert header == sample_header

    def test_cycles_when_rows_exceed_sample(self, tmp_path: Path) -> None:
        # More requested rows than the sample has → rows repeat, never truncate to the sample.
        dest = tmp_path / "load.csv"
        synthesise_csv(5000, dest)
        assert len(dest.read_text(encoding="utf-8").splitlines()) == 5001


class TestFormatReport:
    def _results(self) -> list[dict]:
        return [
            {
                "label": "baseline",
                "batch_size": 16384,
                "linger_ms": 0,
                "compression": "none",
                "events_per_second": 9000.0,
                "mb_per_second": 3.1,
                "elapsed": 22.0,
            },
            {
                "label": "tuned",
                "batch_size": 65536,
                "linger_ms": 20,
                "compression": "lz4",
                "events_per_second": 18000.0,
                "mb_per_second": 6.3,
                "elapsed": 11.0,
            },
        ]

    def test_table_has_a_row_per_config(self) -> None:
        report = format_report(200_000, 1, self._results())
        assert "| baseline |" in report
        assert "| tuned |" in report

    def test_best_is_highest_throughput_with_ratio(self) -> None:
        report = format_report(200_000, 1, self._results())
        assert "Best: **tuned**" in report
        assert "2.00×" in report  # 18000 / 9000
