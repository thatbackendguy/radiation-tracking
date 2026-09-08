"""
Unit tests for scripts/failure_demo.py REST-payload helpers (M3, Week 7).

The demo itself needs a live compose stack; the payload parsing it bases decisions on
(which job is RUNNING, how many TaskManagers are registered) is pure and covered here
with the shapes the JobManager REST API returns.
"""

from __future__ import annotations

from scripts.failure_demo import count_taskmanagers, pick_running_job


class TestPickRunningJob:
    def test_returns_running_job(self) -> None:
        payload = {
            "jobs": [
                {"jid": "a1", "name": "Radiation Tracking", "state": "RUNNING"},
            ]
        }
        job = pick_running_job(payload)
        assert job is not None and job["jid"] == "a1"

    def test_skips_non_running_states(self) -> None:
        payload = {
            "jobs": [
                {"jid": "a1", "name": "old run", "state": "CANCELED"},
                {"jid": "b2", "name": "Radiation Tracking", "state": "RUNNING"},
            ]
        }
        job = pick_running_job(payload)
        assert job is not None and job["jid"] == "b2"

    def test_restarting_job_is_not_running(self) -> None:
        # Mid-failure the job reports RESTARTING — the demo must see that as degraded.
        payload = {"jobs": [{"jid": "a1", "state": "RESTARTING"}]}
        assert pick_running_job(payload) is None

    def test_no_jobs_returns_none(self) -> None:
        assert pick_running_job({"jobs": []}) is None
        assert pick_running_job({}) is None


class TestCountTaskmanagers:
    def test_counts_registered_taskmanagers(self) -> None:
        payload = {"taskmanagers": [{"id": "tm-1"}, {"id": "tm-2"}]}
        assert count_taskmanagers(payload) == 2

    def test_empty_and_missing_lists_count_zero(self) -> None:
        assert count_taskmanagers({"taskmanagers": []}) == 0
        assert count_taskmanagers({}) == 0
