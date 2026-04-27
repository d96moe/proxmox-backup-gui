"""HTTP client for the PVE agent (pve_agent.py).

Used by the GUI backend (app.py) when a host has agent_url configured.
Replaces direct SSH + PVE/PBS/Restic API calls with a single HTTP endpoint
running locally on the PVE host (10.10.0.1:8099).
"""
from __future__ import annotations

import time
from typing import Callable, Iterator

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AgentClient:
    def __init__(self, base_url: str, token: str = "", timeout: int = 30) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        if token:
            self._session.headers["Authorization"] = f"Bearer {token}"

    def _url(self, path: str) -> str:
        return f"{self._base}/{path.lstrip('/')}"

    def _get(self, path: str) -> dict | list:
        resp = self._session.get(self._url(path), timeout=self._timeout)
        if not resp.ok:
            raise RuntimeError(f"Agent GET {path} → {resp.status_code}: {resp.text}")
        return resp.json()

    def _post(self, path: str, body: dict) -> dict:
        resp = self._session.post(self._url(path), json=body, timeout=self._timeout)
        if not resp.ok:
            raise RuntimeError(f"Agent POST {path} → {resp.status_code}: {resp.text}")
        return resp.json()

    def _delete(self, path: str) -> dict:
        resp = self._session.delete(self._url(path), timeout=self._timeout)
        if not resp.ok:
            raise RuntimeError(f"Agent DELETE {path} → {resp.status_code}: {resp.text}")
        return resp.json()

    # ── Health ────────────────────────────────────────────────────────────────

    def health(self) -> dict:
        return self._get("/health")

    def rescan(self) -> dict:
        return self._post("/rescan", {})

    # ── VMs ──────────────────────────────────────────────────────────────────

    def get_vms(self) -> list[dict]:
        return self._get("/vms")

    def get_items(self) -> dict:
        """Fetch all items (VMs + LXCs with annotated snapshots) in one call.

        Returns {vms, lxcs, storage, pbs_stale} — same shape as /api/host/<id>/items.
        Uses the agent's /items endpoint which does a single PBS + single restic call.
        """
        return self._get("/items")

    # ── Snapshots ────────────────────────────────────────────────────────────

    def get_snapshots(self, vm_type: str, vmid: int) -> dict:
        return self._get(f"/snapshots/{vm_type}/{vmid}")

    def delete_snapshot(self, vm_type: str, vmid: int, ts: int) -> None:
        self._delete(f"/snapshots/{vm_type}/{vmid}/{ts}")

    # ── Operations ───────────────────────────────────────────────────────────

    def backup(self, vmid: int, vm_type: str, node: str, storage: str) -> str:
        """Trigger a backup; returns op_id."""
        data = self._post("/operations/backup", {
            "vmid": vmid, "vm_type": vm_type,
            "node": node, "storage": storage,
        })
        return data["op_id"]

    def restore(self, vmid: int, vm_type: str, node: str,
                storage_id: str, backup_time_iso: str, pbs_datastore: str) -> str:
        """Trigger a restore; returns op_id."""
        data = self._post("/operations/restore", {
            "vmid": vmid, "vm_type": vm_type, "node": node,
            "storage_id": storage_id, "backup_time_iso": backup_time_iso,
            "pbs_datastore": pbs_datastore,
        })
        return data["op_id"]

    def get_operation(self, op_id: str) -> dict:
        return self._get(f"/operations/{op_id}")

    def get_operations(self) -> list[dict]:
        return self._get("/operations")

    def wait_for_op(self, op_id: str, log: Callable[[str], None],
                    poll_interval: float = 2.0) -> bool:
        """Poll until op is done. Calls log() with each new log line. Returns True on ok."""
        sent = 0
        while True:
            op = self.get_operation(op_id)
            lines = op.get("log", [])
            while sent < len(lines):
                log(lines[sent])
                sent += 1
            status = op.get("status")
            if status == "ok":
                return True
            if status == "failed":
                return False
            if poll_interval > 0:
                time.sleep(poll_interval)

    def stream_op(self, op_id: str) -> Iterator[str]:
        """SSE-stream log lines from a running/finished operation."""
        resp = self._session.get(
            self._url(f"/operations/{op_id}/stream"),
            stream=True, timeout=None,
        )
        if not resp.ok:
            raise RuntimeError(f"Agent stream {op_id} → {resp.status_code}")
        for raw in resp.iter_lines(decode_unicode=True):
            if raw.startswith("data:"):
                line = raw[5:].strip()
                if line == "__done__":
                    return
                yield line

    # ── Schedules ────────────────────────────────────────────────────────────

    def get_schedules(self) -> dict:
        return self._get("/schedules")

    # ── PBS tasks ────────────────────────────────────────────────────────────

    def get_pbs_tasks(self, running_only: bool = False) -> list[dict]:
        params = "?running=1" if running_only else ""
        return self._get(f"/pbs/tasks{params}")

    def get_pbs_task_log(self, upid: str) -> list[str]:
        return self._get(f"/pbs/tasks/{upid}/log").get("lines", [])

    # ── Settings ─────────────────────────────────────────────────────────────

    def get_settings(self) -> dict:
        return self._get("/settings")

    def set_settings(self, settings: dict) -> dict:
        return self._post("/settings", settings)

    # ── Restic log ────────────────────────────────────────────────────────────

    def get_restic_log(self) -> dict:
        return self._get("/restic/log")

    def get_operations(self) -> list[dict]:
        """Return all operations (any status) from the agent."""
        result = self._get("/operations")
        return result if isinstance(result, list) else []

    def start_restic_prune(self) -> dict:
        """Start a restic prune operation on the agent. Returns {op_id}."""
        return self._post("/operations/restic-prune", {})

    # ── Connection settings ───────────────────────────────────────────────────

    def get_connection(self) -> dict:
        return self._get("/connection")

    def set_connection(self, settings: dict) -> dict:
        return self._post("/connection", settings)
