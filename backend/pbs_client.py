"""Proxmox Backup Server API client."""
from __future__ import annotations

import requests
from requests.auth import HTTPBasicAuth
from functools import lru_cache
from datetime import datetime, timezone

from config import HostConfig


class PBSClient:
    def __init__(self, host: HostConfig) -> None:
        self._base = host.pbs_url.rstrip("/")
        self._datastore = host.pbs_datastore
        self._session = requests.Session()
        self._session.verify = host.verify_ssl
        # PBS uses ticket-based auth; for API tokens use Authorization header
        # Support both: if pbs_user contains '!' it's an API token
        if "!" in host.pbs_user:
            self._session.headers["Authorization"] = (
                f"PBSAPIToken {host.pbs_user}:{host.pbs_password}"
            )
        else:
            ticket, csrf = self._authenticate(host.pbs_user, host.pbs_password)
            self._session.headers["CSRFPreventionToken"] = csrf
            self._session.cookies["PBSAuthCookie"] = ticket

    def _authenticate(self, username: str, password: str) -> tuple[str, str]:
        resp = self._session.post(
            f"{self._base}/api2/json/access/ticket",
            data={"username": username, "password": password},
        )
        resp.raise_for_status()
        d = resp.json()["data"]
        return d["ticket"], d["CSRFPreventionToken"]

    def _get(self, path: str, **params) -> dict:
        resp = self._session.get(f"{self._base}/api2/json{path}", params=params)
        resp.raise_for_status()
        return resp.json().get("data", {})

    def get_storage_info(self) -> dict:
        """Returns local_used and local_total in GB."""
        data = self._get(f"/admin/datastore/{self._datastore}/status")
        total = data.get("total", 0)
        used = data.get("used", 0)
        return {
            "local_used": round(used / 1024**3, 1),
            "local_total": round(total / 1024**3, 1),
        }

    def get_snapshots(self) -> list[dict]:
        """Returns all backup groups with their snapshots."""
        groups = self._get(f"/admin/datastore/{self._datastore}/groups")
        result = []
        for group in groups:
            backup_type = group["backup-type"]   # vm or ct
            backup_id = group["backup-id"]
            snaps = self._get(
                f"/admin/datastore/{self._datastore}/snapshots",
                **{"backup-type": backup_type, "backup-id": backup_id},
            )
            snapshots = []
            for snap in sorted(snaps, key=lambda s: s["backup-time"], reverse=True):
                ts = snap["backup-time"]
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                size_bytes = sum(
                    f.get("size", 0) for f in snap.get("files", [])
                )
                snapshots.append({
                    "backup_time": ts,
                    "date": dt.strftime("%Y-%m-%d %H:%M"),
                    "size_bytes": size_bytes,
                    "size": _fmt_size(size_bytes),
                    "incremental": True,  # PBS always uses incremental chunking
                    "local": True,
                    "cloud": False,
                })
            result.append({
                "pve_id": int(backup_id),
                "backup_type": backup_type,  # vm / ct
                "snapshots": snapshots,
            })
        return result

    def get_versions(self) -> dict:
        """Returns PBS version string."""
        try:
            data = self._get("/version")
            return {"pbs": data.get("version", "unknown")}
        except Exception:
            return {"pbs": "unknown"}


def _fmt_size(size_bytes: int) -> str:
    if size_bytes >= 1024**3:
        return f"{size_bytes / 1024**3:.1f} GB"
    if size_bytes >= 1024**2:
        return f"{size_bytes / 1024**2:.1f} MB"
    return f"{size_bytes / 1024:.1f} KB"
