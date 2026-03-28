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

    def _get(self, path: str) -> list | dict:
        resp = self._session.get(f"{self._base}/api2/json{path}")
        resp.raise_for_status()
        return resp.json().get("data", [])

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


def _guess_os(name: str) -> str:
    name_lower = name.lower()
    if "win" in name_lower:
        return "windows"
    if "mac" in name_lower or "osx" in name_lower:
        return "macos"
    if "haos" in name_lower or "homeassistant" in name_lower:
        return "haos"
    return "linux"
