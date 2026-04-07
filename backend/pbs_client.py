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

    def _delete(self, path: str, **params) -> dict:
        resp = self._session.delete(f"{self._base}/api2/json{path}", params=params)
        resp.raise_for_status()
        return resp.json().get("data", {})

    def get_storage_info(self) -> dict:
        """Returns local_used, local_total in GB, and PBS dedup factor."""
        data = self._get(f"/admin/datastore/{self._datastore}/status")
        total = data.get("total", 0)
        used = data.get("used", 0)

        dedup = None
        try:
            gc_list = self._get("/admin/gc")
            gc = next((e for e in gc_list if e.get("store") == self._datastore), None)
            if gc:
                index_bytes = gc.get("index-data-bytes", 0)  # sum of all source sizes
                disk_bytes  = gc.get("disk-bytes", 0)         # actual chunks on disk
                if disk_bytes > 0:
                    dedup = round(index_bytes / disk_bytes, 1)
        except Exception:
            pass

        return {
            "local_used": round(used / 1024**3, 1),
            "local_total": round(total / 1024**3, 1),
            "dedup_factor": dedup,
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

    def delete_snapshot(self, backup_type: str, backup_id: str, backup_time: int) -> None:
        """Delete a single PBS snapshot. Raises on failure."""
        self._delete(
            f"/admin/datastore/{self._datastore}/snapshots",
            **{"backup-type": backup_type, "backup-id": backup_id, "backup-time": backup_time},
        )

    def delete_all_snapshots_for_vm(self, backup_type: str, backup_id: str, log) -> int:
        """Delete all PBS snapshots for a given VM/LXC. Returns number deleted."""
        snaps = self._get(
            f"/admin/datastore/{self._datastore}/snapshots",
            **{"backup-type": backup_type, "backup-id": backup_id},
        )
        # Sort oldest-first — conventional safe order for incremental chains
        sorted_snaps = sorted(snaps, key=lambda s: s["backup-time"])
        for snap in sorted_snaps:
            ts = snap["backup-time"]
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            log(f"Deleting PBS snapshot {backup_type}/{backup_id} @ {dt}...")
            self.delete_snapshot(backup_type, backup_id, ts)
        log(f"Deleted {len(sorted_snaps)} PBS snapshot(s).")
        return len(sorted_snaps)

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
