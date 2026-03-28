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

    def _is_locked(self) -> bool:
        """Return True only if a recent lock exists (< 8h old = active backup).
        Stale locks from crashed backups are ignored so the GUI is not permanently blocked.
        """
        try:
            from datetime import timedelta
            repo_path = ":".join(self._repo.split(":")[1:])
            r = subprocess.run(
                ["rclone", "lsjson", f"{repo_path}/locks"],
                env=self._env,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if r.returncode != 0 or not r.stdout.strip():
                return False
            locks = json.loads(r.stdout)
            cutoff = datetime.now(timezone.utc) - timedelta(hours=8)
            return any(
                datetime.fromisoformat(lock["ModTime"].replace("Z", "+00:00")) > cutoff
                for lock in locks
                if lock.get("ModTime")
            )
        except Exception:
            return False

    def _run(self, *args: str) -> list | dict:
        if self._is_locked():
            raise RuntimeError("restic repo is locked — backup in progress, skipping")
        result = subprocess.run(
            ["restic", *args, "--json", "--no-lock"],
            env=self._env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            raise RuntimeError(f"restic error: {result.stderr.strip()}")
        return json.loads(result.stdout)

    def get_snapshots_by_vm(self) -> tuple[dict[int, list[dict]], list[dict]]:
        """Returns ({pve_id: [{ts, id, short_id}]}, untagged_snaps).

        Tagged snapshots (vm-N/ct-N) map to specific VMs.
        Untagged snapshots are full-datastore backups covering all VMs — all returned
        so old pruned-local snapshots can appear as cloud-only entries.
        """
        raw = self._run("snapshots")
        by_vm: dict[int, list[dict]] = {}
        untagged: list[dict] = []

        for snap in raw:
            tags = snap.get("tags") or []
            dt = datetime.fromisoformat(snap["time"].replace("Z", "+00:00"))
            ts = int(dt.timestamp())
            snap_id = snap.get("id", "")
            short_id = snap.get("short_id", snap_id[:8] if snap_id else "")

            vm_tags = [t for t in tags if
                       (t.startswith("vm-") and t[3:].isdigit()) or
                       (t.startswith("ct-") and t[3:].isdigit())]

            if vm_tags:
                for tag in vm_tags:
                    pve_id = int(tag[3:])
                    by_vm.setdefault(pve_id, []).append(
                        {"ts": ts, "id": snap_id, "short_id": short_id}
                    )
            else:
                untagged.append({"ts": ts, "id": snap_id, "short_id": short_id})

        return by_vm, untagged

    def get_coverage(self) -> tuple[dict[int, int], int]:
        """Returns ({pve_id: latest_restic_ts}, untagged_latest) for cloud marking."""
        by_vm, untagged = self.get_snapshots_by_vm()
        coverage = {vid: max(e["ts"] for e in entries) for vid, entries in by_vm.items()}
        untagged_latest = max((e["ts"] for e in untagged), default=0)
        return coverage, untagged_latest

    def get_stats(self) -> dict:
        """Returns restic repo size + Google Drive quota via rclone about."""
        result = {"cloud_used": 0, "cloud_total": None}

        # Actual backup folder size via rclone size (fast — Drive API, no restic dedup calc)
        try:
            # repo = "rclone:gdrive:bu/proxmox_home" → path = "gdrive:bu/proxmox_home"
            repo_path = ":".join(self._repo.split(":")[1:])
            r = subprocess.run(
                ["rclone", "size", repo_path, "--json"],
                env=self._env,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if r.returncode == 0:
                size_data = json.loads(r.stdout)
                result["cloud_used"] = round(size_data.get("bytes", 0) / 1024**3, 1)
        except Exception:
            pass

        # Family quota via rclone about
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
                other = about.get("other", 0)  # other family members' usage
                if total:
                    result["cloud_total"] = round(total / 1024**3, 1)
                    result["cloud_quota_used"] = round((used + other) / 1024**3, 1)
        except Exception:
            pass
        return result

    def restore_datastore(self, snapshot_id: str, target: str, log: callable) -> None:
        """Restore a restic snapshot to target path (destructive — overwrites PBS datastore)."""
        import subprocess
        cmd = [
            "restic", "restore", snapshot_id,
            "--target", target,
            "--no-lock",
        ]
        log(f"Running: {' '.join(cmd)}")
        proc = subprocess.Popen(
            cmd,
            env=self._env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for line in proc.stdout:
            log(line.rstrip())
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"restic restore failed (exit {proc.returncode})")

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
