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
        restic = ResticClient(host)
        pve = PVEClient(host)

        pve_meta = pve.get_vms_and_lxcs()
        pbs_snaps = pbs.get_snapshots()       # [{pve_id, backup_type, snapshots:[...local]}]
        restic_snaps = restic.get_snapshots() # [{pve_id, backup_type, date, ...cloud}]
        storage = pbs.get_storage_info()
        cloud_stats = restic.get_stats()

        # Build per-vmid snapshot lists
        snap_map: dict[int, list] = {}
        for group in pbs_snaps:
            vid = group["pve_id"]
            snap_map.setdefault(vid, []).extend(group["snapshots"])

        # Merge restic snapshots
        for rs in restic_snaps:
            vid = rs["pve_id"]
            existing = snap_map.setdefault(vid, [])
            # Try to match by date; if found, mark cloud=True
            matched = False
            for s in existing:
                if s["date"] == rs["date"]:
                    s["cloud"] = True
                    matched = True
                    break
            if not matched:
                existing.append(rs)

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
                "snapshots": snap_map.get(vmid, []),
            }
            if meta["type"] == "lxc":
                lxcs.append(item)
            else:
                vms.append(item)

        return {
            "storage": {**storage, **cloud_stats},
            "vms": vms,
            "lxcs": lxcs,
        }

    return jsonify(_cached(f"items:{host_id}", fetch))


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
    app.run(host="0.0.0.0", port=port, debug=False)
