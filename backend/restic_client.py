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

    def get_coverage(self) -> dict[int, int]:
        """Returns {pve_id: latest_restic_backup_timestamp}.

        Since restic backs up the whole PBS datastore (not individual VMs),
        each tagged snapshot covers all VMs present at that time.
        We return the latest restic run time per VM so app.py can mark
        any PBS snapshot taken before that time as cloud=True.
        """
        raw = self._run("snapshots")
        coverage: dict[int, int] = {}
        for snap in raw:
            tags = snap.get("tags") or []
            dt = datetime.fromisoformat(snap["time"].replace("Z", "+00:00"))
            ts = int(dt.timestamp())
            for tag in tags:
                pve_id = None
                if tag.startswith("vm-") and tag[3:].isdigit():
                    pve_id = int(tag[3:])
                elif tag.startswith("ct-") and tag[3:].isdigit():
                    pve_id = int(tag[3:])
                if pve_id is not None:
                    coverage[pve_id] = max(coverage.get(pve_id, 0), ts)
        return coverage

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
