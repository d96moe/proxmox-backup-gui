"""restic wrapper — all restic/rclone commands run on the PVE host via SSH.

The web UI (LXC) never runs restic or rclone locally.  It SSHes to the PVE
host where both the PBS datastore and the restic/rclone binaries live.
"""
from __future__ import annotations

import json
import shlex
import subprocess
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from config import HostConfig


class ResticClient:
    def __init__(self, host: HostConfig) -> None:
        self._repo = host.restic_repo
        # Extract rclone remote name (e.g. "rclone:gdrive:bu/x" → "gdrive")
        parts = host.restic_repo.split(":")
        self._gdrive_remote = parts[1] if len(parts) >= 2 else "gdrive"

        # SSH target — all restic/rclone commands execute here
        self._ssh_host = (
            host.pve_ssh_host or urlparse(host.pve_url).hostname or host.pve_url
        )

        # Environment prefix for remote shell commands
        env_vars = {
            "RESTIC_REPOSITORY": host.restic_repo,
            "RESTIC_PASSWORD": host.restic_password,
            **host.restic_env,
        }
        self._env_prefix = " ".join(
            f"{k}={shlex.quote(v)}" for k, v in env_vars.items()
        )

    # ──────────────────────────────────────────────
    # SSH helpers
    # ──────────────────────────────────────────────

    def _ssh_run(self, remote_cmd: str, timeout: int = 30) -> str:
        """Run remote_cmd on the PVE host, return stdout. Raises on non-zero exit."""
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
             f"root@{self._ssh_host}", remote_cmd],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"SSH restic command failed on {self._ssh_host}: {result.stderr.strip()}"
            )
        return result.stdout

    def _ssh_stream(self, remote_cmd: str, log) -> None:
        """Run remote_cmd on PVE host, streaming output through log(). Raises on non-zero exit."""
        proc = subprocess.Popen(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
             f"root@{self._ssh_host}", remote_cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for line in proc.stdout:
            log(line.rstrip())
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(
                f"SSH restic command on {self._ssh_host} failed (exit {proc.returncode})"
            )

    # ──────────────────────────────────────────────
    # restic helpers
    # ──────────────────────────────────────────────

    def _is_locked(self) -> bool:
        """Return True only if a recent lock exists (< 8h old = active backup)."""
        try:
            repo_path = ":".join(self._repo.split(":")[1:])
            # Pass env_prefix so rclone can reach Google Drive on the PVE host
            stdout = self._ssh_run(
                f"{self._env_prefix} rclone lsjson {shlex.quote(repo_path + '/locks')}"
                " 2>/dev/null || echo '[]'",
                timeout=15,
            )
            locks = json.loads(stdout.strip() or "[]")
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
        quoted_args = " ".join(shlex.quote(a) for a in args)
        stdout = self._ssh_run(
            f"{self._env_prefix} restic {quoted_args} --json --no-lock",
            timeout=30,
        )
        return json.loads(stdout)

    # ──────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────

    def get_snapshots_by_vm(self) -> tuple[dict[int, list[dict]], list[dict]]:
        """Returns ({pve_id: [{ts, id, short_id}]}, untagged_snaps).

        Tagged snapshots (vm-N/ct-N) map to specific VMs.
        Untagged snapshots are full-datastore backups covering all VMs.
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

            vm_tags = [
                t for t in tags
                if (t.startswith("vm-") and t[3:].isdigit())
                or (t.startswith("ct-") and t[3:].isdigit())
            ]

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
        """Returns restic repo size + Google Drive quota via rclone on PVE host."""
        result = {"cloud_used": 0, "cloud_total": None}

        repo_path = ":".join(self._repo.split(":")[1:])

        try:
            stdout = self._ssh_run(
                f"{self._env_prefix} rclone size {shlex.quote(repo_path)} --json",
                timeout=30,
            )
            size_data = json.loads(stdout)
            result["cloud_used"] = round(size_data.get("bytes", 0) / 1024**3, 1)
        except Exception:
            pass

        try:
            stdout = self._ssh_run(
                f"{self._env_prefix} rclone about {shlex.quote(self._gdrive_remote + ':')} --json",
                timeout=15,
            )
            about = json.loads(stdout)
            total = about.get("total", 0)
            used = about.get("used", 0)
            other = about.get("other", 0)
            if total:
                result["cloud_total"] = round(total / 1024**3, 1)
                result["cloud_quota_used"] = round((used + other) / 1024**3, 1)
        except Exception:
            pass

        return result

    def backup_datastore(self, datastore_path: str, log) -> None:
        """Back up the PBS datastore to the restic repo on the PVE host."""
        log("Stopping PBS...")
        self._ssh_run(
            "systemctl stop proxmox-backup proxmox-backup-proxy 2>/dev/null || true"
        )
        try:
            path = shlex.quote(datastore_path)
            log(f"Running restic backup of {datastore_path}...")
            self._ssh_stream(
                f"{self._env_prefix} restic backup {path} --no-lock",
                log,
            )
        finally:
            log("Starting PBS...")
            self._ssh_run(
                "systemctl start proxmox-backup proxmox-backup-proxy 2>/dev/null || true"
            )
        log("Restic backup complete.")

    def restore_datastore(self, snapshot_id: str, log) -> None:
        """Restore a restic snapshot to the PBS datastore path on the PVE host."""
        snap = shlex.quote(snapshot_id)
        self._ssh_stream(
            f"{self._env_prefix} restic restore {snap} --target / --no-lock",
            log,
        )

    def get_version(self) -> str:
        try:
            stdout = self._ssh_run("restic version", timeout=10)
            # "restic 0.18.0 compiled with ..."
            return stdout.split()[1] if stdout.strip() else "unknown"
        except Exception:
            return "unknown"
