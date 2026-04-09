"""Proxmox VE API client — fetches VM/LXC names and status."""
from __future__ import annotations

import requests
import urllib3

from config import HostConfig

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
        resp = self._session.get(f"{self._base}/api2/json{path}", timeout=timeout)
        resp.raise_for_status()
        return resp.json().get("data", [])

    def _post(self, path: str, **data) -> dict:
        resp = self._session.post(f"{self._base}/api2/json{path}", json=data)
        resp.raise_for_status()
        return resp.json().get("data", {})

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
        """Returns enabled vzdump backup jobs with their schedule strings."""
        try:
            jobs = self._get("/cluster/backup")
            return [
                {
                    "id": j.get("id", ""),
                    "schedule": j.get("schedule") or _format_starttime(j.get("starttime")),
                    "storage": j.get("storage", ""),
                }
                for j in (jobs if isinstance(jobs, list) else [])
                if j.get("enabled", 1)
            ]
        except Exception:
            return []

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


def _guess_os(name: str) -> str:
    name_lower = name.lower()
    if "win" in name_lower:
        return "windows"
    if "mac" in name_lower or "osx" in name_lower:
        return "macos"
    if "haos" in name_lower or "homeassistant" in name_lower:
        return "haos"
    return "linux"
