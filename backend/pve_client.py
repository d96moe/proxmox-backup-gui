"""Proxmox VE API client — fetches VM/LXC names and status."""
from __future__ import annotations

import json
import subprocess

import requests
import urllib3

from config import HostConfig

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _pvesh_create(path: str, data: dict):
    """Run an API POST via the local `pvesh` CLI — fallback for PVE versions that
    intermittently drop HTTP POSTs proxied to pvedaemon (PVE 9.0.x on ARM under
    memory pressure → RemoteDisconnected). pvesh talks to pvedaemon locally and is
    reliable. pvesh paths map 1:1 to API paths, so this covers backup/restore/
    stop/start. Returns the endpoint's data (a UPID string for task endpoints)."""
    import re
    cmd = ["pvesh", "create", path, "--output-format", "json"]
    for k, v in data.items():
        if v is None:
            continue
        cmd += [f"--{k}", str(v)]
    # Task endpoints (vzdump/qmrestore) run SYNCHRONOUSLY under pvesh and stream
    # the whole task log to stdout, ending with the UPID — so a 12s incremental or
    # a multi-minute full backup both run here. Allow a long timeout.
    out = subprocess.run(cmd, check=True, timeout=7200,
                         capture_output=True, text=True).stdout.strip()
    if not out:
        return {}
    # Extract just the UPID so callers can poll the (already-finished) task as
    # usual — without this the streamed log was mistaken for the UPID.
    m = re.search(r'UPID:[^\s"]+', out)
    if m:
        return m.group(0)
    try:
        return json.loads(out)
    except Exception:
        return out.strip().strip('"')


def _pvesh_get(path: str):
    """Run an API GET via the local `pvesh` CLI — fallback for ARM PVE that
    intermittently drops HTTP GETs forwarded to pvedaemon. Returns parsed data.

    The HTTP layer URL-encodes path segments (e.g. the task UPID in
    wait_for_task); pvesh wants the raw API path, so unquote it first."""
    from urllib.parse import unquote
    path = unquote(path)
    out = subprocess.run(["pvesh", "get", path, "--output-format", "json"],
                         check=True, timeout=30, capture_output=True, text=True).stdout.strip()
    if not out:
        return []
    try:
        return json.loads(out)
    except Exception:
        return out


class PVEClient:
    def __init__(self, host: HostConfig) -> None:
        self._base = host.pve_url.rstrip("/")
        self._session = requests.Session()
        self._session.verify = host.verify_ssl
        ticket, csrf = self._authenticate(host.pve_user, host.pve_password)
        self._session.headers["CSRFPreventionToken"] = csrf
        self._session.cookies["PVEAuthCookie"] = ticket

    def _authenticate(self, username: str, password: str) -> tuple[str, str]:
        resp = self._session.post(
            f"{self._base}/api2/json/access/ticket",
            data={"username": username, "password": password},
            verify=False,
        )
        resp.raise_for_status()
        d = resp.json()["data"]
        return d["ticket"], d["CSRFPreventionToken"]

    def _get(self, path: str, timeout: int | None = None) -> list | dict:
        try:
            resp = self._session.get(f"{self._base}/api2/json{path}", timeout=timeout)
            resp.raise_for_status()
            return resp.json().get("data", [])
        except requests.exceptions.ConnectionError:
            # ARM PVE intermittently drops GETs forwarded to pvedaemon too — this
            # matters most for wait_for_task's status polls during a long backup
            # (one dropped poll would otherwise fail an op whose task succeeded).
            return _pvesh_get(path)

    def _post(self, path: str, **data) -> dict:
        try:
            resp = self._session.post(f"{self._base}/api2/json{path}", json=data)
            resp.raise_for_status()
            return resp.json().get("data", {})
        except requests.exceptions.ConnectionError:
            # ARM PVE intermittently drops POSTs forwarded to pvedaemon; pvesh is
            # local and reliable. Covers backup_vm/restore_vm/stop_vm/start_vm.
            return _pvesh_create(path, data)

    def _put(self, path: str, **data) -> dict:
        resp = self._session.put(f"{self._base}/api2/json{path}", data=data)
        resp.raise_for_status()
        return resp.json().get("data", {}) or {}

    def get_nodes(self) -> list[str]:
        nodes = self._get("/nodes")
        return [n["node"] for n in nodes]

    def get_vms_and_lxcs(self) -> dict[int, dict]:
        """Returns {vmid: {name, type, os, status}} for all nodes."""
        nodes = self._get("/nodes")
        result: dict[int, dict] = {}
        for node in nodes:
            nname = node["node"]
            for vm in self._get(f"/nodes/{nname}/qemu"):
                result[vm["vmid"]] = {
                    "name": vm.get("name", f"vm-{vm['vmid']}"),
                    "type": "vm",
                    "os": _guess_os(vm.get("name", "")),
                    "status": vm.get("status", "unknown"),
                    "template": bool(vm.get("template", 0)),
                }
            for ct in self._get(f"/nodes/{nname}/lxc"):
                result[ct["vmid"]] = {
                    "name": ct.get("name", f"ct-{ct['vmid']}"),
                    "type": "lxc",
                    "os": "linux",
                    "status": ct.get("status", "unknown"),
                }
        return result

    def get_backup_schedules(self) -> list[dict]:
        """Returns enabled vzdump backup jobs with their schedule strings and next-run left."""
        try:
            jobs = self._get("/cluster/backup")
            result = []
            for j in (jobs if isinstance(jobs, list) else []):
                if not j.get("enabled", 1):
                    continue
                schedule = j.get("schedule") or _format_starttime(j.get("starttime"))
                result.append({
                    "id":       j.get("id", ""),
                    "schedule": schedule,
                    "left":     _schedule_left(schedule) if schedule else None,
                    "storage":  j.get("storage", ""),
                })
            return result
        except Exception:
            return []

    def get_backup_vm_selection(self, job_id: str) -> dict:
        """Return {mode, vmids} for the named vzdump job.

        mode="exclude": job has all=1; vmids is the exclude list.
        mode="include": job has no all; vmids is the explicit include list.
        """
        try:
            jobs = self._get("/cluster/backup")
            for j in (jobs if isinstance(jobs, list) else []):
                if j.get("id") != job_id:
                    continue
                if j.get("all"):
                    raw = j.get("exclude", "")
                    vmids = [int(v) for v in str(raw).split(",") if v.strip().isdigit()]
                    return {"mode": "exclude", "vmids": vmids}
                else:
                    raw = j.get("vmid", "")
                    vmids = [int(v) for v in str(raw).split(",") if v.strip().isdigit()]
                    return {"mode": "include", "vmids": vmids}
        except Exception:
            pass
        return {"mode": "exclude", "vmids": []}

    def set_backup_vm_selection(self, job_id: str, mode: str, vmids: list[int]) -> None:
        """Set backup VM selection mode and vmid list for a vzdump job.

        PVE ignores the vmid field when all=1 and ignores exclude when all=0,
        so we never need to delete the opposing field explicitly.
        """
        ids = ",".join(str(v) for v in vmids)
        if mode == "include":
            params: dict = {"all": 0}
            if ids:
                params["vmid"] = ids
        else:
            params = {"all": 1}
            if ids:
                params["exclude"] = ids
        self._put(f"/cluster/backup/{job_id}", **params)

    def set_backup_schedule(self, job_id: str, schedule: str) -> None:
        """Update the schedule of an existing vzdump backup job."""
        self._put(f"/cluster/backup/{job_id}", schedule=schedule)

    def is_backup_running(self) -> bool:
        """Return True if a vzdump task is currently running on any node."""
        try:
            for node in self.get_nodes():
                resp = self._session.get(
                    f"{self._base}/api2/json/nodes/{node}/tasks",
                    params={"typefilter": "vzdump", "statusfilter": "running"},
                )
                resp.raise_for_status()
                if resp.json().get("data"):
                    return True
        except Exception:
            pass
        return False

    def backup_vm(self, vmid: int, vm_type: str, storage: str, node: str) -> str:
        """Trigger vzdump backup of a single VM/LXC to PBS storage. Returns PVE task UPID."""
        params: dict = dict(storage=storage, mode="snapshot", compress="zstd", remove=0)
        if vmid:
            params["vmid"] = str(vmid)
        else:
            params["all"] = 1
        resp = self._post(f"/nodes/{node}/vzdump", **params)
        return resp  # UPID string

    def restore_vm(
        self,
        vmid: int,
        vm_type: str,
        backup_time_iso: str,
        storage_id: str,
        node: str,
        pbs_datastore: str,
    ) -> str:
        """Restore VM/LXC from PBS snapshot. Returns PVE task UPID."""
        archive = f"{storage_id}:backup/{vm_type}/{vmid}/{backup_time_iso}"
        if vm_type == "ct":
            resp = self._post(
                f"/nodes/{node}/lxc",
                vmid=vmid,
                ostemplate=archive,
                restore=1,
                force=1,
            )
        else:
            resp = self._post(
                f"/nodes/{node}/qemu",
                vmid=vmid,
                archive=archive,
                force=1,
            )
        return resp  # UPID string

    def stop_vm(self, vmid: int, vm_type: str, node: str) -> None:
        """Stop a running VM/LXC. Silently succeeds if already stopped."""
        import time
        endpoint = (f"/nodes/{node}/lxc/{vmid}/status/stop" if vm_type == "ct"
                    else f"/nodes/{node}/qemu/{vmid}/status/stop")
        try:
            upid = self._post(endpoint)
            if upid:
                self.wait_for_task(node, upid, lambda _: None, poll_interval=2)
        except Exception:
            pass  # Already stopped or other non-critical error

    def start_vm(self, vmid: int, vm_type: str, node: str) -> None:
        """Start a VM/LXC. Silently succeeds if already running."""
        endpoint = (f"/nodes/{node}/lxc/{vmid}/status/start" if vm_type == "ct"
                    else f"/nodes/{node}/qemu/{vmid}/status/start")
        try:
            upid = self._post(endpoint)
            if upid:
                self.wait_for_task(node, upid, lambda _: None, poll_interval=2)
        except Exception:
            pass  # Already running or other non-critical error

    def wait_for_task(self, node: str, upid: str, log: callable, poll_interval: int = 3) -> bool:
        """Poll a PVE task until completion. Returns True on success."""
        import time
        from urllib.parse import quote
        encoded = quote(upid, safe="")
        while True:
            status = self._get(f"/nodes/{node}/tasks/{encoded}/status")
            if status.get("status") == "stopped":
                exit_status = status.get("exitstatus", "unknown")
                if exit_status == "OK":
                    log(f"Task completed: {exit_status}")
                    return True
                else:
                    log(f"Task failed: {exit_status}")
                    return False
            time.sleep(poll_interval)


def _format_starttime(t) -> str | None:
    """Convert legacy PVE starttime (seconds from midnight) to HH:MM string."""
    if t is None:
        return None
    h, m = divmod(int(t) // 60, 60)
    return f"daily {h:02d}:{m:02d}"


def _schedule_left(schedule: str, now=None) -> str | None:
    """Return 'in Xh' / 'in Xd' until next run from a systemd calendar expression.

    Handles: 'HH:MM', 'daily HH:MM', 'sat HH:MM', 'mon,wed HH:MM', 'weekly', 'monthly'.
    now: override current time (for testing); defaults to datetime.now() (local time).
    """
    import re
    from datetime import datetime, timedelta

    if now is None:
        now = datetime.now()  # local time — PVE schedules are in server local time
    s = schedule.strip().lower()
    s = re.sub(r"^daily\s+", "", s)  # strip "daily " prefix from legacy format

    day_map = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}

    m = re.match(r"^(?:([a-z,]+)\s+)?(\d{1,2}):(\d{2})$", s)
    if m:
        days_str, h, mn = m.group(1), int(m.group(2)), int(m.group(3))
        if days_str:
            target_days = {day_map[d.strip()] for d in days_str.split(",") if d.strip() in day_map}
            if not target_days:
                return None
        else:
            target_days = set(range(7))

        for delta in range(8):
            candidate = (now + timedelta(days=delta)).replace(
                hour=h, minute=mn, second=0, microsecond=0
            )
            if candidate > now and candidate.weekday() in target_days:
                secs = (candidate - now).total_seconds()
                if secs < 3600:
                    return f"in {int(secs // 60)}m"
                if secs < 86400:
                    return f"in {int(secs // 3600)}h"
                return f"in {int(secs // 86400)}d"

    return None


def schedule_interval_hours(schedule: str) -> float | None:
    """Estimate the max expected gap (hours) between two runs of a vzdump
    schedule string, for deriving a 'backup is overdue' threshold.

    Same parsing as _schedule_left (HH:MM / 'daily HH:MM' / '<days> HH:MM'),
    but returns the interval itself rather than time-until-next-run. Returns
    None if the schedule can't be parsed — callers should treat that as
    "unknown", not silently assume a default, since a wrong guess here means
    a wrong red/green status.
    """
    import re

    if not isinstance(schedule, str) or not schedule:
        return None
    s = schedule.strip().lower()
    s = re.sub(r"^daily\s+", "", s)

    day_map = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}

    m = re.match(r"^(?:([a-z,]+)\s+)?(\d{1,2}):(\d{2})$", s)
    if not m:
        return None
    days_str = m.group(1)
    if not days_str:
        return 24.0
    target_days = sorted({day_map[d.strip()] for d in days_str.split(",") if d.strip() in day_map})
    if not target_days:
        return None
    if len(target_days) == 7:
        return 24.0
    gaps = [
        (target_days[(i + 1) % len(target_days)] - target_days[i]) % 7 or 7
        for i in range(len(target_days))
    ]
    return max(gaps) * 24.0


def _guess_os(name: str) -> str:
    name_lower = name.lower()
    if "win" in name_lower:
        return "windows"
    if "mac" in name_lower or "osx" in name_lower:
        return "macos"
    if "haos" in name_lower or "homeassistant" in name_lower:
        return "haos"
    return "linux"
