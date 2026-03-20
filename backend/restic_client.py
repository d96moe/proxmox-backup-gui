"""restic wrapper — runs restic as a subprocess and parses JSON output."""
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone

from config import HostConfig


class ResticClient:
    def __init__(self, host: HostConfig) -> None:
        self._repo = host.restic_repo
        # Extract rclone remote name (e.g. "rclone:gdrive:bu/x" → "gdrive")
        parts = host.restic_repo.split(":")
        self._gdrive_remote = parts[1] if len(parts) >= 2 else "gdrive"
        self._env = {
            **os.environ,
            "RESTIC_REPOSITORY": host.restic_repo,
            "RESTIC_PASSWORD": host.restic_password,
            **host.restic_env,
        }

    def _run(self, *args: str) -> list | dict:
        result = subprocess.run(
            ["restic", *args, "--json"],
            env=self._env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            raise RuntimeError(f"restic error: {result.stderr.strip()}")
        return json.loads(result.stdout)

    def get_snapshots(self) -> list[dict]:
        """Returns snapshots grouped by hostname+path tag."""
        raw = self._run("snapshots")
        result = []
        for snap in raw:
            # Extract PVE VM/CT id from tags or hostname
            tags = snap.get("tags") or []
            pve_id = None
            backup_type = None
            for tag in tags:
                if tag.startswith("vm-"):
                    pve_id = int(tag[3:])
                    backup_type = "vm"
                elif tag.startswith("ct-"):
                    pve_id = int(tag[3:])
                    backup_type = "ct"

            if pve_id is None:
                continue

            dt = datetime.fromisoformat(snap["time"].replace("Z", "+00:00"))
            size_bytes = snap.get("summary", {}).get("data_added", 0)
            result.append({
                "snapshot_id": snap["id"][:8],
                "pve_id": pve_id,
                "backup_type": backup_type,
                "date": dt.strftime("%Y-%m-%d %H:%M"),
                "backup_time": int(dt.timestamp()),
                "size_bytes": size_bytes,
                "size": _fmt_size(size_bytes),
                "cloud": True,
                "local": False,
            })
        return result

    def get_stats(self) -> dict:
        """Returns restic repo size + Google Drive quota via rclone about."""
        result = {"cloud_used": 0, "cloud_total": None}
        try:
            data = self._run("stats")
            result["cloud_used"] = round(data.get("total_size", 0) / 1024**3, 1)
        except Exception:
            pass
        try:
            r = subprocess.run(
                ["rclone", "about", f"{self._gdrive_remote}:", "--json"],
                env=self._env,
                capture_output=True,
                text=True,
                timeout=15,
            )
            if r.returncode == 0:
                about = json.loads(r.stdout)
                total = about.get("total", 0)
                used = about.get("used", 0)
                if total:
                    result["cloud_total"] = round(total / 1024**3, 1)
                    result["cloud_quota_used"] = round(used / 1024**3, 1)
        except Exception:
            pass
        return result

    def get_version(self) -> str:
        try:
            result = subprocess.run(
                ["restic", "version"],
                capture_output=True, text=True, timeout=10,
            )
            # "restic 0.18.0 compiled with ..."
            return result.stdout.split()[1] if result.stdout else "unknown"
        except Exception:
            return "unknown"


def _fmt_size(size_bytes: int) -> str:
    if size_bytes >= 1024**3:
        return f"{size_bytes / 1024**3:.1f} GB"
    if size_bytes >= 1024**2:
        return f"{size_bytes / 1024**2:.1f} MB"
    return f"{size_bytes / 1024:.1f} KB"
