"""Flask backend for Proxmox Backup GUI.

Endpoints:
  GET /api/hosts           → list of configured hosts
  GET /api/host/<id>/items → VMs + LXCs with merged PBS + restic snapshots
  GET /api/info            → PBS + restic version info for current host
"""
from __future__ import annotations

import re
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

from datetime import timezone, datetime
from flask import Flask, jsonify, send_from_directory, abort, request
from flask_cors import CORS

from config import load_hosts, HostConfig
from pbs_client import PBSClient
from restic_client import ResticClient
from pve_client import PVEClient
from jobs import create_job, get_job, run_job


app = Flask(__name__, static_folder=None)
CORS(app)

HOSTS: dict[str, HostConfig] = {h.id: h for h in load_hosts()}
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


def _detect_self_vmid() -> int | None:
    """Return our own LXC VMID if running inside a Proxmox container, else None."""
    try:
        with open("/proc/1/cgroup") as f:
            for line in f:
                m = re.search(r"/lxc/(\d+)", line)
                if m:
                    return int(m.group(1))
    except Exception:
        pass
    return None


SELF_VMID: int | None = _detect_self_vmid()

# Simple in-process cache (ttl=60s) so the page stays snappy
_cache: dict[str, tuple[float, object]] = {}
CACHE_TTL = 60

# Per-host restic lock — prevents concurrent restic backup/restore operations
_restic_locks: dict[str, threading.Lock] = {}
_restic_locks_guard = threading.Lock()


def _get_restic_lock(host_id: str) -> threading.Lock:
    with _restic_locks_guard:
        if host_id not in _restic_locks:
            _restic_locks[host_id] = threading.Lock()
        return _restic_locks[host_id]


def _cached(key: str, fn):
    now = time.monotonic()
    if key in _cache:
        ts, val = _cache[key]
        if now - ts < CACHE_TTL:
            return val
    val = fn()
    _cache[key] = (now, val)
    return val


# ──────────────────────────────────────────────


# ──────────────────────────────────────────────
# Serve frontend
# ──────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(FRONTEND_DIR, "index.html")


@app.route("/<path:filename>")
def static_files(filename: str):
    return send_from_directory(FRONTEND_DIR, filename)


# ──────────────────────────────────────────────
# API
# ──────────────────────────────────────────────

@app.get("/api/hosts")
def get_hosts():
    return jsonify([
        {"id": h.id, "label": h.label, "self_vmid": SELF_VMID}
        for h in HOSTS.values()
    ])


@app.get("/api/host/<host_id>/items")
def get_items(host_id: str):
    host = HOSTS.get(host_id)
    if not host:
        abort(404, f"Host '{host_id}' not configured")

    def fetch():
        try:
            pbs = PBSClient(host)
        except Exception as e:
            abort(503, f"PBS unavailable — restic backup may be running ({e})")
        try:
            pve = PVEClient(host)
        except Exception as e:
            abort(503, f"PVE unavailable ({e})")

        pve_meta = pve.get_vms_and_lxcs()
        pbs_snaps = pbs.get_snapshots()
        storage = pbs.get_storage_info()

        # Restic: per-VM snapshot timestamps + untagged (full-datastore) latest
        restic_by_vm: dict[int, list[dict]] = {}
        untagged_snaps: list[dict] = []
        if host.restic_repo:
            try:
                restic = ResticClient(host)
                restic_by_vm, untagged_snaps = restic.get_snapshots_by_vm()
            except Exception as e:
                app.logger.warning("restic unavailable: %s", e)

        untagged_latest = max((e["ts"] for e in untagged_snaps), default=0)

        # Build per-vmid snapshot lists from PBS, mark cloud where covered
        snap_map: dict[int, list] = {}
        for group in pbs_snaps:
            vid = group["pve_id"]
            all_entries = sorted(
                restic_by_vm.get(vid, []) + untagged_snaps,
                key=lambda e: e["ts"], reverse=True,
            )
            for snap in group["snapshots"]:
                snap["local"] = True
                covering = next((e for e in all_entries if e["ts"] > snap["backup_time"]), None)
                if covering:
                    snap["cloud"] = True
                    snap["restic_id"] = covering.get("id")
                    snap["restic_short_id"] = covering.get("short_id")
            snap_map.setdefault(vid, []).extend(group["snapshots"])

        # Add cloud-only restic snapshots (contain PBS snapshots pruned locally)
        pbs_times: dict[int, set[int]] = {
            vid: {s["backup_time"] for s in snaps}
            for vid, snaps in snap_map.items()
        }
        all_restic_vmids = set(restic_by_vm.keys()) | (
            set(pve_meta.keys()) if untagged_snaps else set()
        )
        for vid in all_restic_vmids:
            # Tagged entries per VM + all untagged (full-datastore) snapshots
            entries = restic_by_vm.get(vid, []) + untagged_snaps
            known = pbs_times.get(vid, set())
            oldest_local = min(known) if known else None
            for e in entries:
                ts = e["ts"]
                # Only add cloud-only entry if restic snapshot is OLDER than oldest local PBS
                # snapshot — meaning it contains PBS snapshots that have since been pruned.
                if oldest_local is None or ts < oldest_local:
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    snap_map.setdefault(vid, []).append({
                        "backup_time": ts,
                        "date": dt,
                        "local": False,
                        "cloud": True,
                        "incremental": True,
                        "size": "—",
                        "restic_id": e.get("id"),
                        "restic_short_id": e.get("short_id"),
                    })

        # Sort snapshots newest-first per vm
        for snaps in snap_map.values():
            snaps.sort(key=lambda s: s["backup_time"], reverse=True)

        # Attach human-readable age
        now_ts = time.time()
        for snaps in snap_map.values():
            for s in snaps:
                s["age"] = _human_age(now_ts - s["backup_time"])

        # Build final item list
        vms, lxcs = [], []
        all_vmids = set(pve_meta.keys()) | set(snap_map.keys())

        for vmid in sorted(all_vmids):
            meta = pve_meta.get(vmid, {
                "name": f"id-{vmid}", "type": "vm", "os": "linux", "status": "unknown"
            })
            item = {
                "id": vmid,
                "name": meta["name"],
                "type": meta["type"],
                "os": meta["os"],
                "status": meta["status"],
                "template": meta.get("template", False),
                "in_pve": vmid in pve_meta,
                "snapshots": snap_map.get(vmid, []),
            }
            if meta["type"] == "lxc":
                lxcs.append(item)
            else:
                vms.append(item)

        return {
            "storage": {**storage, "cloud_used": 0},
            "vms": vms,
            "lxcs": lxcs,
        }

    return jsonify(_cached(f"items:{host_id}", fetch))


@app.get("/api/host/<host_id>/storage")
def get_storage(host_id: str):
    """PBS + restic storage sizes. Restic stats can be slow — called async by frontend."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)

    def fetch():
        pbs = PBSClient(host)
        result = pbs.get_storage_info()
        result["cloud_used"] = 0
        if host.restic_repo:
            try:
                restic = ResticClient(host)
                stats = restic.get_stats()
                app.logger.warning("restic stats: %s", stats)
                result.update(stats)
            except Exception as e:
                app.logger.warning("restic stats unavailable: %s", e)
        return result

    return jsonify(_cached(f"storage:{host_id}", fetch))


@app.get("/api/host/<host_id>/ha/sensors")
def get_ha_sensors(host_id: str):
    """HA-friendly flat sensor dict — abstracts PBS/restic as local/cloud."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)

    def fetch():
        pbs = PBSClient(host)
        pve = PVEClient(host)
        pve_meta = pve.get_vms_and_lxcs()
        pbs_snaps = pbs.get_snapshots()
        storage = pbs.get_storage_info()

        restic_coverage: dict[int, int] = {}
        untagged_latest: int = 0
        cloud_stats = {"cloud_used": 0, "cloud_total": None, "cloud_quota_used": None}
        if host.restic_repo:
            try:
                restic = ResticClient(host)
                restic_coverage, untagged_latest = restic.get_coverage()
                cloud_stats.update(restic.get_stats())
            except Exception as e:
                app.logger.warning("restic unavailable for HA sensors: %s", e)

        # Build snap_map, mark cloud where restic ran after PBS snapshot
        snap_map: dict[int, list] = {}
        for group in pbs_snaps:
            vid = group["pve_id"]
            latest_restic = max(restic_coverage.get(vid, 0), untagged_latest)
            for snap in group["snapshots"]:
                if latest_restic > snap["backup_time"]:
                    snap["cloud"] = True
            snap_map.setdefault(vid, []).extend(group["snapshots"])

        now_ts = time.time()
        out = {}

        # ── Global storage ────────────────────────────────────────────────────
        local_pct = round(storage["local_used"] / storage["local_total"] * 100) \
            if storage["local_total"] else 0
        out["storage_local_used_pct"] = local_pct
        out["storage_local_used_gb"]  = storage["local_used"]
        out["storage_local_total_gb"] = storage["local_total"]

        if cloud_stats.get("cloud_total"):
            cloud_pct = round(
                cloud_stats["cloud_quota_used"] / cloud_stats["cloud_total"] * 100
            )
            out["storage_cloud_used_pct"]   = cloud_pct
            out["storage_cloud_used_gb"]    = cloud_stats["cloud_quota_used"]
            out["storage_cloud_total_gb"]   = cloud_stats["cloud_total"]
        else:
            out["storage_cloud_used_pct"] = None
            out["storage_cloud_used_gb"]  = cloud_stats.get("cloud_used", 0)
            out["storage_cloud_total_gb"] = None

        # ── Per VM/LXC sensors ────────────────────────────────────────────────
        all_ages_h = []
        total_local = 0
        total_cloud = 0
        unprotected = 0

        all_vmids = set(pve_meta.keys()) | set(snap_map.keys())
        for vmid in sorted(all_vmids):
            meta  = pve_meta.get(vmid, {"name": f"id-{vmid}", "type": "vm"})
            snaps = snap_map.get(vmid, [])
            snaps.sort(key=lambda s: s["backup_time"], reverse=True)

            key = meta["name"].lower().replace("-", "_").replace(" ", "_")

            has_local  = any(s.get("local") for s in snaps)
            has_cloud  = any(s.get("cloud") for s in snaps)
            n_local    = sum(1 for s in snaps if s.get("local"))
            n_cloud    = sum(1 for s in snaps if s.get("cloud"))
            n_both     = sum(1 for s in snaps if s.get("local") and s.get("cloud"))

            total_local += n_local
            total_cloud += n_cloud
            if not has_cloud:
                unprotected += 1

            if snaps:
                age_h = round((now_ts - snaps[0]["backup_time"]) / 3600, 1)
                all_ages_h.append(age_h)
                status = "ok" if age_h < 26 and has_local else \
                         "warning" if age_h < 48 else "error"
            else:
                age_h  = None
                status = "error"

            out[f"{key}_age_hours"]      = age_h
            out[f"{key}_has_local"]      = has_local
            out[f"{key}_has_cloud"]      = has_cloud
            out[f"{key}_snapshot_count"] = len(snaps)
            out[f"{key}_local_count"]    = n_local
            out[f"{key}_cloud_count"]    = n_cloud
            out[f"{key}_both_count"]     = n_both
            out[f"{key}_status"]         = status

        # ── Global summary ────────────────────────────────────────────────────
        out["oldest_backup_age_hours"]  = round(max(all_ages_h), 1) if all_ages_h else None
        out["newest_backup_age_hours"]  = round(min(all_ages_h), 1) if all_ages_h else None
        out["unprotected_count"]        = unprotected
        out["total_snapshot_count"]     = sum(len(s) for s in snap_map.values())
        out["total_local_count"]        = total_local
        out["total_cloud_count"]        = total_cloud
        out["all_protected"]            = unprotected == 0

        return out

    return jsonify(_cached(f"ha:{host_id}", fetch))


@app.get("/api/host/<host_id>/info")
def get_info(host_id: str):
    host = HOSTS.get(host_id)
    if not host:
        abort(404)

    def fetch():
        pbs = PBSClient(host)
        restic = ResticClient(host)
        return {**pbs.get_versions(), "restic": restic.get_version()}

    return jsonify(_cached(f"info:{host_id}", fetch))


# ──────────────────────────────────────────────
# Backup / Restore
# ──────────────────────────────────────────────

@app.post("/api/host/<host_id>/backup/pbs")
def backup_pbs(host_id: str):
    """Trigger an immediate PBS backup of a single VM/LXC."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    body = request.get_json() or {}
    vmid             = int(body.get("vmid", 0))
    vm_type          = body.get("type", "vm")   # vm or ct
    run_restic_after = body.get("run_restic_after", False)
    if not vmid:
        abort(400, "vmid required")

    if run_restic_after and host.restic_repo:
        restic_lock = _get_restic_lock(host_id)
        if not restic_lock.acquire(blocking=False):
            abort(409, "A restic operation is already running for this host")
    else:
        restic_lock = None

    label = f"PBS + cloud backup {vm_type}/{vmid}" if run_restic_after else f"PBS backup {vm_type}/{vmid}"
    job_id = create_job(label)

    def work(log):
        try:
            pve = PVEClient(host)
            nodes = pve.get_nodes()
            node = nodes[0]
            log(f"Triggering PBS backup: {vm_type}/{vmid} on node {node}")
            upid = pve.backup_vm(vmid, vm_type, host.pbs_storage_id, node)
            log(f"Task started: {upid}")
            if isinstance(upid, str) and upid.startswith("UPID:"):
                ok = pve.wait_for_task(node, upid, log)
                if not ok:
                    raise RuntimeError("PBS backup task failed")
            log("Backup complete.")
            if run_restic_after and host.restic_repo:
                restic = ResticClient(host)
                restic.backup_datastore(host.pbs_datastore_path, log)
            _cache.pop(f"items:{host_id}", None)
        finally:
            if restic_lock:
                restic_lock.release()

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/restore")
def restore(host_id: str):
    """Restore a VM/LXC from PBS (local) or restic (cloud).

    Body:
      vmid            int
      type            "vm" | "ct"
      backup_time     int  (unix timestamp — for local PBS restore)
      source          "local" | "cloud"
      restic_id       str  (restic snapshot id — for cloud restore)
      run_backup_after bool (default false)
    """
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    body = request.get_json() or {}
    vmid             = int(body.get("vmid", 0))
    vm_type          = body.get("type", "vm")
    # PVE metadata uses "lxc" for containers; PBS and restore APIs use "ct"
    if vm_type == "lxc":
        vm_type = "ct"
    backup_time      = body.get("backup_time")
    source           = body.get("source", "local")
    restic_id        = body.get("restic_id")
    run_backup_after = body.get("run_backup_after", False)

    if not vmid:
        abort(400, "vmid required")

    if source == "cloud":
        restic_lock = _get_restic_lock(host_id)
        if not restic_lock.acquire(blocking=False):
            abort(409, "A restic operation is already running for this host")
    else:
        restic_lock = None

    job_id = create_job(f"Restore {vm_type}/{vmid} from {source}")

    def work(log):
        pve = PVEClient(host)
        nodes = pve.get_nodes()
        node = nodes[0]

        if source == "cloud":
            if not restic_id:
                raise ValueError("restic_id required for cloud restore")
            restic = ResticClient(host)
            log(f"Starting restic restore of snapshot {restic_id} → {host.pbs_datastore_path}")
            log("WARNING: This will overwrite the current PBS datastore.")
            log(f"Connecting to PVE host {restic._ssh_host} via SSH...")
            log("Stopping PBS...")
            restic._ssh_run(
                "systemctl stop proxmox-backup proxmox-backup-proxy 2>/dev/null || true"
            )
            try:
                log("Running restic restore...")
                restic.restore_datastore(restic_id, log)
            finally:
                log("Starting PBS...")
                restic._ssh_run(
                    "systemctl start proxmox-backup proxmox-backup-proxy 2>/dev/null || true"
                )
            log("Restic restore complete. PBS restarted.")
            # Poll until PBS is ready — systemctl start returns before PBS is
            # fully initialised and able to serve API requests (typically 5-30s).
            pbs = PBSClient(host)
            snaps = None
            for attempt in range(1, 25):
                try:
                    snaps = pbs.get_snapshots()
                    log(f"PBS ready after {attempt * 5}s.")
                    break
                except Exception as e:
                    log(f"PBS not ready yet (attempt {attempt}/24): {e}")
                    time.sleep(5)
            if snaps is None:
                raise RuntimeError("PBS did not become ready within 120s after restic restore")
            # Use latest PBS snapshot for this VM after datastore restore
            vm_snaps = next(
                (g["snapshots"] for g in snaps
                 if g["pve_id"] == vmid and g["backup_type"] == vm_type), []
            )
            if not vm_snaps:
                raise RuntimeError(f"No PBS snapshot found for {vm_type}/{vmid} after restic restore")
            backup_ts = vm_snaps[0]["backup_time"]
        else:
            backup_ts = backup_time

        # Format timestamp as ISO 8601 for PVE API
        backup_time_iso = datetime.fromtimestamp(backup_ts, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        log(f"Stopping {vm_type}/{vmid} before restore...")
        pve.stop_vm(vmid, vm_type, node)
        log(f"Restoring {vm_type}/{vmid} snapshot {backup_time_iso} from {host.pbs_storage_id}")
        upid = pve.restore_vm(vmid, vm_type, backup_time_iso, host.pbs_storage_id, node,
                              host.pbs_datastore)
        log(f"Task started: {upid}")
        ok = pve.wait_for_task(node, upid, log)
        if not ok:
            raise RuntimeError("Restore task failed")
        log("Restore complete.")

        if run_backup_after:
            log("Triggering PBS backup of all VMs...")
            upid2 = pve.backup_vm(0, vm_type, host.pbs_storage_id, node)
            pve.wait_for_task(node, upid2, log)
            log("Post-restore backup complete.")

        _cache.pop(f"items:{host_id}", None)

    def work_with_lock(log):
        try:
            work(log)
        finally:
            if restic_lock:
                restic_lock.release()

    run_job(job_id, work_with_lock)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/backup/restic")
def backup_restic(host_id: str):
    """Trigger a restic backup of the PBS datastore to cloud."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    if not host.restic_repo:
        abort(400, "restic not configured for this host")

    restic_lock = _get_restic_lock(host_id)
    if not restic_lock.acquire(blocking=False):
        abort(409, "A restic operation is already running for this host")

    job_id = create_job("Cloud backup (restic)")

    def work(log):
        try:
            restic = ResticClient(host)
            restic.backup_datastore(host.pbs_datastore_path, log)
            _cache.pop(f"items:{host_id}", None)
        finally:
            restic_lock.release()

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


@app.get("/api/job/<job_id>")
def get_job_status(job_id: str):
    job = get_job(job_id)
    if not job:
        abort(404)
    return jsonify(job)


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _human_age(seconds: float) -> str:
    s = int(seconds)
    if s < 3600:
        m = s // 60
        return f"{m} min ago" if m > 1 else "just now"
    if s < 86400:
        h = s // 3600
        return f"{h}h ago"
    d = s // 86400
    return f"{d} day{'s' if d > 1 else ''} ago"


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
