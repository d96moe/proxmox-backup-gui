"""restic wrapper — all restic/rclone commands run on the PVE host via SSH.

The web UI (LXC) never runs restic or rclone locally.  It SSHes to the PVE
host where both the PBS datastore and the restic/rclone binaries live.
"""
from __future__ import annotations

import json
import re
import shlex
import subprocess
import threading
import time
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
            # Emit progress even when stdout is not a TTY (SSH pipe)
            "RESTIC_PROGRESS_FPS": "0.5",
            # Skip GDrive trash on delete — prune/forget frees space immediately
            # instead of files accumulating in trash and counting against quota
            "RCLONE_DRIVE_USE_TRASH": "false",
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

    def _ssh_stream(self, remote_cmd: str, log, heartbeat_secs: int = 10,
                    parse_json: bool = False, _check_interval: float = 1.0) -> None:
        """Run remote_cmd on PVE host, streaming output through log(). Raises on non-zero exit.

        heartbeat_secs: inject "[Xs elapsed]" when no output received for this long.
        parse_json: parse restic --json status lines into human-readable progress messages.
        """
        proc = subprocess.Popen(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
             f"root@{self._ssh_host}", remote_cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        start = time.monotonic()
        last_output = [start]
        stop_event = threading.Event()

        def _heartbeat():
            while not stop_event.wait(_check_interval):
                if time.monotonic() - last_output[0] >= heartbeat_secs:
                    elapsed = int(time.monotonic() - start)
                    log(f"[{elapsed}s elapsed]")
                    last_output[0] = time.monotonic()

        hb = threading.Thread(target=_heartbeat, daemon=True)
        hb.start()
        try:
            for raw in proc.stdout:
                last_output[0] = time.monotonic()
                line = raw.rstrip()
                if parse_json and line.startswith('{'):
                    try:
                        d = json.loads(line)
                        mtype = d.get('message_type', '')
                        if mtype == 'status':
                            pct = int(d.get('percent_done', 0) * 100)
                            elapsed = int(d.get('seconds_elapsed', 0))
                            files_done = d.get('files_done', 0)
                            total_files = d.get('total_files', 0)
                            mb = round(d.get('bytes_done', 0) / 1024 ** 2)
                            log(f"[{pct}% — {files_done}/{total_files} files, {mb} MB ({elapsed}s elapsed)]")
                            continue
                        elif mtype == 'summary':
                            added = d.get('data_added_packed', d.get('data_added', 0))
                            mb = round(added / 1024 ** 2)
                            log(f"Uploaded {mb} MB to cloud.")
                            continue
                        elif mtype == 'verbose_status':
                            # emitted by forget --prune during repacking phase
                            pct = int(d.get('percent_done', 0) * 100)
                            done = d.get('packs_done', 0)
                            total = d.get('total_packs', 0)
                            elapsed = int(d.get('seconds_elapsed', 0))
                            mb = round(d.get('bytes_done', 0) / 1024 ** 2)
                            log(f"[Repacking: {pct}% — {done}/{total} packs, {mb} MB ({elapsed}s elapsed)]")
                            continue
                        elif mtype == 'remove_snapshot':
                            sid = d.get('short_id', d.get('id', '?')[:8])
                            log(f"Removed snapshot {sid}")
                            continue
                    except (json.JSONDecodeError, KeyError, TypeError):
                        pass
                log(line)
        finally:
            stop_event.set()
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
            timeout=120,
        )
        return json.loads(stdout)

    # ──────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────

    def get_snapshots_by_vm(self) -> tuple[dict[int, list[dict]], list[dict]]:
        """Returns ({pve_id: [{ts, pbs_time, id, short_id}]}, untagged_snaps).

        New tag format "ct-301-1775554738": pbs_time is the exact PBS backup timestamp.
        Old tag format "ct-301": pbs_time is None (fallback to ts-based matching).
        Untagged snapshots are full-datastore backups (legacy, no per-VM info).
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

            vm_tags: list[tuple[int, int | None]] = []
            for t in tags:
                # New format: ct-301-1775554738
                m = re.match(r'^(?:vm|ct)-(\d+)-(\d+)$', t)
                if m:
                    vm_tags.append((int(m.group(1)), int(m.group(2))))
                    continue
                # Old format: ct-301 or vm-100
                if (t.startswith("vm-") and t[3:].isdigit()) or \
                   (t.startswith("ct-") and t[3:].isdigit()):
                    vm_tags.append((int(t[3:]), None))

            if vm_tags:
                for pve_id, pbs_time in vm_tags:
                    entry: dict = {"ts": ts, "id": snap_id, "short_id": short_id}
                    if pbs_time is not None:
                        entry["pbs_time"] = pbs_time
                    by_vm.setdefault(pve_id, []).append(entry)
            else:
                untagged.append({"ts": ts, "id": snap_id, "short_id": short_id})

        return by_vm, untagged

    def get_snapshots_flat(self) -> list[dict]:
        """Returns all restic snapshots as a flat list, newest first.

        Each entry: { id, short_id, ts, size_bytes, covers: [{type, vmid, pbs_time}] }
        size_bytes is the incremental packed data added by this snapshot.
        """
        raw = self._run("snapshots")
        result = []
        for snap in raw:
            tags = snap.get("tags") or []
            dt = datetime.fromisoformat(snap["time"].replace("Z", "+00:00"))
            ts = int(dt.timestamp())
            snap_id = snap.get("id", "")
            short_id = snap.get("short_id", snap_id[:8] if snap_id else "")
            summary = snap.get("summary", {})
            size_bytes = summary.get("data_added_packed", summary.get("data_added", 0))

            covers: list[dict] = []
            seen: set[tuple] = set()
            for t in tags:
                m = re.match(r'^(vm|ct)-(\d+)-(\d+)$', t)
                if m:
                    key = (m.group(1), int(m.group(2)), int(m.group(3)))
                    if key not in seen:
                        seen.add(key)
                        covers.append({"type": key[0], "vmid": key[1], "pbs_time": key[2]})
                    continue
                for prefix in ("vm-", "ct-"):
                    if t.startswith(prefix) and t[len(prefix):].isdigit():
                        key = (prefix[:-1], int(t[len(prefix):]), None)
                        if key not in seen:
                            seen.add(key)
                            covers.append({"type": key[0], "vmid": key[1], "pbs_time": None})

            result.append({
                "id": snap_id,
                "short_id": short_id,
                "ts": ts,
                "size_bytes": size_bytes,
                "covers": covers,
            })
        return sorted(result, key=lambda x: x["ts"], reverse=True)

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

    def backup_datastore(self, datastore_path: str, log,
                         pbs_snapshots: list[tuple[str, int, int]] | None = None) -> None:
        """Back up the PBS datastore to the restic repo on the PVE host.

        pbs_snapshots: list of (backup_type, vmid, backup_time) for every snapshot
        currently in the PBS datastore, e.g. [('ct', 301, 1775554738)].
        Each becomes a restic tag "ct-301-1775554738" so that get_snapshots_by_vm()
        can match cloud entries back to exact PBS snapshots.
        """
        log("Stopping PBS...")
        self._ssh_run(
            "systemctl stop proxmox-backup proxmox-backup-proxy 2>/dev/null || true"
        )
        try:
            path = shlex.quote(datastore_path)
            tag_flags = ""
            if pbs_snapshots:
                tags = " ".join(
                    f"--tag {shlex.quote(f'{btype}-{vmid}-{btime}')}"
                    for btype, vmid, btime in pbs_snapshots
                )
                tag_flags = f" {tags}"
            log(f"Running restic backup of {datastore_path}...")
            self._ssh_stream(
                f"{self._env_prefix} restic backup {path}{tag_flags} --no-lock --json",
                log,
                parse_json=True,
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
            f"{self._env_prefix} restic restore {snap} --target / --no-lock --verbose",
            log,
        )

    def forget_snapshots(self, snapshot_ids: list[str], log) -> None:
        """Remove specific restic snapshots and prune unreferenced data.

        Passes the exact snapshot IDs rather than a retention policy to avoid
        accidentally removing snapshots other than the intended ones.
        """
        if not snapshot_ids:
            log("No restic snapshots to forget.")
            return
        ids_quoted = " ".join(shlex.quote(sid) for sid in snapshot_ids)
        log(f"Forgetting restic snapshot(s): {', '.join(s[:8] for s in snapshot_ids)}...")
        self._ssh_stream(
            f"{self._env_prefix} restic forget {ids_quoted} --prune --verbose --json",
            log,
            parse_json=True,
        )
        log("Restic forget + prune complete.")
        # Deleted packs end up in GDrive trash and count against quota until emptied.
        try:
            repo_path = ":".join(self._repo.split(":")[1:])
            log("Emptying cloud trash...")
            self._ssh_run(
                f"{self._env_prefix} rclone cleanup {shlex.quote(repo_path)} 2>/dev/null || true",
                timeout=30,
            )
        except Exception:
            pass

    def is_running(self) -> bool:
        """Return True if a restic backup is currently in progress (active lock exists)."""
        return self._is_locked()

    def get_next_run(self) -> dict | None:
        """Return next scheduled run of the restic systemd timer on the PVE host.

        Parses `systemctl list-timers` output. Returns
        {"next": "Mon 2026-04-06 03:00:00 CEST", "left": "11h"} or None.
        """
        try:
            stdout = self._ssh_run(
                "systemctl list-timers --no-pager 2>/dev/null | grep -i restic | head -1",
                timeout=10,
            )
            line = stdout.strip()
            if not line:
                return None
            # NEXT (3 tokens: day date time) + TZ + LEFT (Nh) + "left" + ...
            tokens = line.split()
            if len(tokens) >= 6:
                next_at = " ".join(tokens[:4])   # "Mon 2026-04-06 03:00:00 CEST"
                left    = tokens[4]               # "11h" / "3h" etc.
                return {"next": next_at, "left": left}
        except Exception:
            pass
        return None

    def get_restic_schedule(self) -> str | None:
        """Read the OnCalendar value from restic-backup.timer on the PVE host."""
        try:
            stdout = self._ssh_run(
                "systemctl cat restic-backup.timer 2>/dev/null | grep -i OnCalendar | head -1",
                timeout=10,
            )
            line = stdout.strip()
            if not line:
                return None
            _, _, value = line.partition("=")
            return value.strip() or None
        except Exception:
            return None

    def set_restic_schedule(self, on_calendar: str) -> None:
        """Write a new OnCalendar value into restic-backup.timer and reload."""
        safe = on_calendar.replace("'", "")
        script = (
            f"sed -i 's|^OnCalendar=.*|OnCalendar={safe}|' "
            "/etc/systemd/system/restic-backup.timer && "
            "systemctl daemon-reload && "
            "systemctl restart restic-backup.timer"
        )
        self._ssh_run(script, timeout=15)

    def get_retention(self) -> dict:
        """Read RESTIC_RETENTION_KEEP_* settings from config.env on PVE host."""
        try:
            stdout = self._ssh_run(
                "cat /etc/proxmox-backup-restore/config.env 2>/dev/null || true",
                timeout=10,
            )
            mapping = {
                "RESTIC_RETENTION_KEEP_LAST":    "keep-last",
                "RESTIC_RETENTION_KEEP_DAILY":   "keep-daily",
                "RESTIC_RETENTION_KEEP_WEEKLY":  "keep-weekly",
                "RESTIC_RETENTION_KEEP_MONTHLY": "keep-monthly",
                "RESTIC_RETENTION_KEEP_YEARLY":  "keep-yearly",
            }
            result = {}
            for line in stdout.splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k in mapping and v:
                    result[mapping[k]] = v
            return result
        except Exception:
            return {}

    def get_pbs_prune_jobs(self) -> list[dict]:
        """Read PBS prune job settings via proxmox-backup-manager SSH on PVE host."""
        try:
            stdout = self._ssh_run(
                "proxmox-backup-manager prune-job list --output-format json 2>/dev/null || echo '[]'",
                timeout=10,
            )
            jobs = json.loads(stdout.strip() or "[]")
            return jobs if isinstance(jobs, list) else []
        except Exception:
            return []

    def get_version(self) -> str:
        try:
            stdout = self._ssh_run("restic version", timeout=10)
            # "restic 0.18.0 compiled with ..."
            return stdout.split()[1] if stdout.strip() else "unknown"
        except Exception:
            return "unknown"
