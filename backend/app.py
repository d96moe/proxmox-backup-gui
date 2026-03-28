"""Flask backend for Proxmox Backup GUI.

Endpoints:
  GET /api/hosts           → list of configured hosts
  GET /api/host/<id>/items → VMs + LXCs with merged PBS + restic snapshots
  GET /api/info            → PBS + restic version info for current host
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

from flask import Flask, jsonify, send_from_directory, abort
from flask_cors import CORS

from config import load_hosts, HostConfig
from pbs_client import PBSClient
from restic_client import ResticClient
from pve_client import PVEClient


app = Flask(__name__, static_folder=None)
CORS(app)

HOSTS: dict[str, HostConfig] = {h.id: h for h in load_hosts()}
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

# Simple in-process cache (ttl=60s) so the page stays snappy
_cache: dict[str, tuple[float, object]] = {}
CACHE_TTL = 60


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
    return jsonify([{"id": h.id, "label": h.label} for h in HOSTS.values()])


@app.get("/api/host/<host_id>/items")
def get_items(host_id: str):
    host = HOSTS.get(host_id)
    if not host:
        abort(404, f"Host '{host_id}' not configured")

    def fetch():
        pbs = PBSClient(host)
        pve = PVEClient(host)

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
            entries = restic_by_vm.get(vid, [])
            latest_restic = max([e["ts"] for e in entries] + [untagged_latest], default=0)
            for snap in group["snapshots"]:
                snap["local"] = True
                if latest_restic > snap["backup_time"]:
                    snap["cloud"] = True
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
