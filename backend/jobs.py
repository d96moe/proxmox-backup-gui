"""Background job manager for long-running backup/restore operations."""
from __future__ import annotations

import threading
import time
import uuid
from typing import Callable


_jobs: dict[str, dict] = {}
_lock = threading.Lock()


def create_job(label: str) -> str:
    job_id = str(uuid.uuid4())[:8]
    with _lock:
        _jobs[job_id] = {
            "id":      job_id,
            "label":   label,
            "status":  "pending",
            "logs":    [],
            "started": time.time(),
            "ended":   None,
        }
    return job_id


def get_job(job_id: str) -> dict | None:
    with _lock:
        job = _jobs.get(job_id)
        return dict(job) if job else None


def get_active_jobs() -> list[dict]:
    """Return all jobs with status running or pending."""
    with _lock:
        return [dict(j) for j in _jobs.values() if j["status"] in ("running", "pending")]


def run_job(job_id: str, fn: Callable[[Callable[[str], None]], None]) -> None:
    """Run fn in a background thread. fn receives a log(msg) callback."""

    def _run():
        def log(msg: str):
            with _lock:
                _jobs[job_id]["logs"].append(msg)

        with _lock:
            _jobs[job_id]["status"] = "running"
        try:
            fn(log)
            with _lock:
                _jobs[job_id]["status"] = "done"
        except Exception as exc:
            with _lock:
                _jobs[job_id]["logs"].append(f"ERROR: {exc}")
                _jobs[job_id]["status"] = "error"
        finally:
            with _lock:
                _jobs[job_id]["ended"] = time.time()

    threading.Thread(target=_run, daemon=True).start()
