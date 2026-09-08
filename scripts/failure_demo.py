"""
failure_demo.py — drop the Flink TaskManager and watch the job recover (failure-mode demo).

Demonstrates the pipeline's fault tolerance for the final presentation: it kills the
`flink-taskmanager` compose service, observes the radiation-tracking job lose its slots
(state leaves RUNNING / registered TaskManagers drop), brings the TaskManager back, and
times how long the job takes to return to RUNNING. Everything is observed through the
JobManager REST API — the same numbers the http://localhost:8081 UI shows.

Cross-platform (pure Python stdlib — no bash/curl/jq), driven through `docker compose`
so it behaves identically on macOS/Windows/the DO droplet:

    docker compose up -d          # full stack, job submitted by the flink-job service
    python scripts/failure_demo.py

Options: `--rest-url` (default http://localhost:8081 or $FLINK_REST_URL), `--service`
(default flink-taskmanager), `--timeout` seconds per phase (default 300).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

RUNNING = "RUNNING"


def pick_running_job(jobs_payload: dict) -> dict | None:
    """Return the first RUNNING job from a /jobs/overview payload, else None."""
    for job in jobs_payload.get("jobs", []):
        if job.get("state") == RUNNING:
            return job
    return None


def count_taskmanagers(taskmanagers_payload: dict) -> int:
    """Number of registered TaskManagers in a /taskmanagers payload."""
    return len(taskmanagers_payload.get("taskmanagers", []))


def _get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True)


def _log(started: float, message: str) -> None:
    print(f"[t+{time.monotonic() - started:6.1f}s] {message}", flush=True)


def _wait_for(started: float, timeout: float, describe: str, predicate) -> None:
    """Poll ``predicate`` (returns a truthy status message when met) until timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status = predicate()
        except (urllib.error.URLError, OSError, json.JSONDecodeError):
            status = None  # REST briefly unreachable is expected mid-failure
        if status:
            _log(started, status)
            return
        time.sleep(2)
    raise TimeoutError(f"timed out after {timeout:.0f}s waiting for {describe}")


def run_demo(rest_url: str, service: str, timeout: float) -> None:
    started = time.monotonic()

    job = pick_running_job(_get_json(f"{rest_url}/jobs/overview"))
    if job is None:
        raise RuntimeError(
            f"no RUNNING job on {rest_url} — start the stack first (docker compose up -d) "
            "and wait for the flink-job submitter to finish"
        )
    baseline_tms = count_taskmanagers(_get_json(f"{rest_url}/taskmanagers"))
    if baseline_tms < 1:
        raise RuntimeError("no registered TaskManagers — nothing to drop")
    _log(started, f"baseline: job '{job.get('name')}' RUNNING, {baseline_tms} TaskManager(s)")

    _log(started, f"killing {service} ...")
    _compose("kill", service)

    def degraded() -> str | None:
        tms = count_taskmanagers(_get_json(f"{rest_url}/taskmanagers"))
        state = (pick_running_job(_get_json(f"{rest_url}/jobs/overview")) or {}).get(
            "state", "not-RUNNING"
        )
        if tms < baseline_tms or state != RUNNING:
            return f"failure observed: {tms} TaskManager(s) registered, job {state}"
        return None

    _wait_for(started, timeout, "the JobManager to notice the lost TaskManager", degraded)

    _log(started, f"restarting {service} ...")
    _compose("up", "-d", service)
    recovery_started = time.monotonic()

    def recovered() -> str | None:
        tms = count_taskmanagers(_get_json(f"{rest_url}/taskmanagers"))
        job_now = pick_running_job(_get_json(f"{rest_url}/jobs/overview"))
        if tms >= baseline_tms and job_now is not None:
            return (
                f"recovered: {tms} TaskManager(s) back, job '{job_now.get('name')}' RUNNING "
                f"({time.monotonic() - recovery_started:.1f}s after restart)"
            )
        return None

    _wait_for(started, timeout, "the job to return to RUNNING", recovered)
    _log(started, "demo complete — Flink restarted the job from the last successful state")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rest-url",
        default=os.environ.get("FLINK_REST_URL", "http://localhost:8081"),
        help="JobManager REST endpoint (default: $FLINK_REST_URL or http://localhost:8081)",
    )
    parser.add_argument(
        "--service",
        default="flink-taskmanager",
        help="compose service to drop (default: flink-taskmanager)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300,
        help="seconds to wait for each phase (failure seen / recovery) before giving up",
    )
    args = parser.parse_args(argv)

    try:
        run_demo(args.rest_url.rstrip("/"), args.service, args.timeout)
    except (RuntimeError, TimeoutError, subprocess.CalledProcessError, OSError) as exc:
        # OSError covers urllib.error.URLError — an unreachable JobManager REST endpoint
        # should read as "stack not up", not a traceback.
        print(f"failure demo aborted: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
