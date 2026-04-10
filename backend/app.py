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
from jobs import create_job, get_job, get_active_jobs, run_job


app = Flask(__name__, static_folder=None)
CORS(app)

HOSTS: dict[str, HostConfig] = {h.id: h for h in load_hosts()}
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


def _detect_self_vmid() -> int | None:
    """Return our own LXC VMID if running inside a Proxmox container, else None.

    Supports both cgroupv1 (path contains /lxc/<id>) and cgroupv2 (detects via
    MAC address matching against the PVE API).
    """
    # cgroupv1: path contains /lxc/<vmid>
    try:
        with open("/proc/1/cgroup") as f:
            for line in f:
                m = re.search(r"/lxc/(\d+)", line)
                if m:
                    return int(m.group(1))
    except Exception:
        pass

    # cgroupv2: cgroup path is "/.lxc" with no VMID embedded.
    # Detect by matching our ethernet MAC against PVE LXC configs.
    try:
        in_lxc = False
        with open("/proc/1/cgroup") as f:
            if "/.lxc" in f.read():
                in_lxc = True
        if not in_lxc:
            try:
                with open("/proc/1/environ", "rb") as f:
                    if b"container=lxc" in f.read():
                        in_lxc = True
            except Exception:
                pass
        if not in_lxc:
            return None

        from pathlib import Path as _Path
        mac = _Path("/sys/class/net/eth0/address").read_text().strip().upper().replace(":", "")
        if not mac:
            return None

        for h in load_hosts():
            try:
                pve = PVEClient(h)
                for node in pve._get("/nodes", timeout=5):
                    nname = node["node"]
                    for ct in pve._get(f"/nodes/{nname}/lxc", timeout=5):
                        vmid = ct["vmid"]
                        try:
                            cfg = pve._get(f"/nodes/{nname}/lxc/{vmid}/config", timeout=5)
                            for val in cfg.values():
                                if isinstance(val, str):
                                    m = re.search(r"hwaddr=([0-9A-Fa-f:]+)", val, re.IGNORECASE)
                                    if m and m.group(1).upper().replace(":", "") == mac:
                                        return vmid
                        except Exception:
                            continue
            except Exception:
                continue
    except Exception:
        pass

    return None


SELF_VMID: int | None = _detect_self_vmid()

# Simple in-process cache (ttl=60s) so the page stays snappy
_cache: dict[str, tuple[float, object]] = {}
CACHE_TTL = 60

# Persistent restic snapshot cache — no TTL, updated on every successful fetch.
# Returned as stale data when restic is busy so the UI can still show cloud info.
_restic_snap_cache: dict[str, list] = {}  # host_id → last known flat snapshot list

# Persistent PBS items cache — returned as stale data when PBS is offline (e.g. during restic backup)
_pbs_items_cache: dict[str, dict] = {}  # host_id → last known items response

# Per-host restic lock — prevents concurrent restic backup/restore operations
_restic_locks: dict[str, threading.Lock] = {}
_restic_locks_guard = threading.Lock()


def _pbs_vm_ids(host: HostConfig) -> list[tuple[str, int, int]]:
    """Return [(backup_type, vmid, backup_time), ...] for every snapshot in PBS."""
    try:
        pbs = PBSClient(host)
        groups = pbs.get_snapshots()
        return [
            (g["backup_type"], g["pve_id"], s["backup_time"])
            for g in groups
            for s in g["snapshots"]
        ]
    except Exception:
        return []


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
            cached = _pbs_items_cache.get(host_id)
            if cached:
                app.logger.warning("PBS unavailable (%s) — returning cached items", e)
                cached["pbs_stale"] = True
                return cached
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
        restic_busy = False
        if host.restic_repo:
            try:
                restic = ResticClient(host)
                if restic.is_running():
                    restic_busy = True
                    app.logger.info("restic is running — using cached cloud coverage")
                    restic_by_vm, untagged_snaps = _restic_snap_cache.get(
                        f"by_vm:{host_id}", (dict(), list()))
                else:
                    restic_by_vm, untagged_snaps = restic.get_snapshots_by_vm()
                    _restic_snap_cache[f"by_vm:{host_id}"] = (restic_by_vm, untagged_snaps)
            except Exception as e:
                app.logger.warning("restic unavailable: %s", e)

        untagged_latest = max((e["ts"] for e in untagged_snaps), default=0)

        # Build per-vmid snapshot lists from PBS, mark cloud where covered.
        #
        # New tag format (ct-301-<pbs_time>): exact lookup by pbs_time.
        # Old tag format (ct-301, no timestamp): fallback to ts > backup_time heuristic.
        restic_by_pbs_time: dict[int, dict[int, dict]] = {}  # vid → {pbs_time → entry}
        for vid, entries in restic_by_vm.items():
            for e in entries:
                pt = e.get("pbs_time")
                if pt is not None:
                    restic_by_pbs_time.setdefault(vid, {})[pt] = e

        # Map vmid → PBS backup_type ("ct"/"vm") for orphaned snapshots not in PVE.
        pbs_type_map: dict[int, str] = {
            group["pve_id"]: group.get("backup_type", "vm")
            for group in pbs_snaps
        }

        snap_map: dict[int, list] = {}
        for group in pbs_snaps:
            vid = group["pve_id"]
            old_entries_sorted = sorted(
                [e for e in restic_by_vm.get(vid, []) if "pbs_time" not in e]
                + untagged_snaps,
                key=lambda e: e["ts"], reverse=True,
            )
            for snap in group["snapshots"]:
                snap["local"] = True
                # Exact match first (new tag format)
                covering = restic_by_pbs_time.get(vid, {}).get(snap["backup_time"])
                if covering is None:
                    # Fallback: find newest old-style restic entry newer than this PBS snap
                    covering = next(
                        (e for e in old_entries_sorted if e["ts"] > snap["backup_time"]),
                        None,
                    )
                if covering:
                    snap["cloud"] = True
                    snap["restic_id"] = covering.get("id")
                    snap["restic_short_id"] = covering.get("short_id")
            snap_map.setdefault(vid, []).extend(group["snapshots"])

        # Add cloud-only restic snapshots.
        #
        # New tag format (pbs_time present): cloud-only if the exact PBS snapshot
        # no longer exists locally for this VM.
        # Old tag format / untagged: cloud-only if older than oldest local PBS (legacy).
        # Build dedup sets for cloud-only detection.
        # New-format entries: dedup by (rid, pbs_time) so a restic snapshot that
        # covers both a still-local PBS snapshot (T2) and a gone one (T1) correctly
        # shows T1 as cloud-only while T2 remains local+cloud.
        # Old-format/untagged: dedup by rid (original behaviour).
        used_restic_ids: set[str] = set()
        used_restic_pairs: set[tuple] = set()  # (rid, pbs_time)
        for snaps in snap_map.values():
            for snap in snaps:
                rid = snap.get("restic_id")
                if rid:
                    used_restic_ids.add(rid)
                    used_restic_pairs.add((rid, snap.get("backup_time")))

        pbs_times_set: dict[int, set[int]] = {
            vid: {s["backup_time"] for s in snaps}
            for vid, snaps in snap_map.items()
        }
        pbs_oldest: dict[int, int] = {
            vid: min(times) for vid, times in pbs_times_set.items() if times
        }

        all_restic_vmids = set(restic_by_vm.keys()) | (
            set(pve_meta.keys()) if untagged_snaps else set()
        )
        added_cloud_only: set[tuple] = set()  # (vid, pbs_time) already emitted
        for vid in all_restic_vmids:
            local_times = pbs_times_set.get(vid, set())
            oldest_local = pbs_oldest.get(vid)
            # Sort newest-first so the first matching entry wins as restic_id
            all_entries = sorted(
                restic_by_vm.get(vid, []) + untagged_snaps,
                key=lambda e: e["ts"], reverse=True,
            )
            for e in all_entries:
                rid = e.get("id", "")
                pbs_time = e.get("pbs_time")
                if pbs_time is not None:
                    # New format: skip if already covered as local+cloud
                    if (rid, pbs_time) in used_restic_pairs:
                        continue
                    is_cloud_only = pbs_time not in local_times
                else:
                    # Old format / untagged: dedup by rid
                    if rid in used_restic_ids:
                        continue
                    is_cloud_only = (oldest_local is None or e["ts"] < oldest_local)
                if is_cloud_only:
                    # Deduplicate: one cloud-only row per (vid, pbs_time).
                    # Entries are sorted newest-first so the first winner carries
                    # the newest restic snapshot as restic_id (best for restore).
                    cloud_key = (vid, pbs_time if pbs_time is not None else e["ts"])
                    if cloud_key in added_cloud_only:
                        continue
                    added_cloud_only.add(cloud_key)
                    ts = e["ts"]
                    display_ts = pbs_time if pbs_time is not None else ts
                    dt = datetime.fromtimestamp(display_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                    snap_map.setdefault(vid, []).append({
                        "backup_time": display_ts,
                        "date": dt,
                        "local": False,
                        "cloud": True,
                        "incremental": True,
                        "size": "—",
                        "restic_id": rid,
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
            _pbs_btype = pbs_type_map.get(vmid, "vm")
            _orphan_type = "lxc" if _pbs_btype == "ct" else "vm"
            meta = pve_meta.get(vmid, {
                "name": f"id-{vmid}", "type": _orphan_type, "os": "linux", "status": "unknown"
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

        result = {
            "storage": {**storage, "cloud_used": 0},
            "vms": vms,
            "lxcs": lxcs,
            "restic_busy": restic_busy,
            "pbs_stale": False,
        }
        _pbs_items_cache[host_id] = dict(result)
        return result

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


@app.get("/api/host/<host_id>/schedules")
def get_schedules(host_id: str):
    """Return next scheduled PBS (vzdump) and restic backup times for the host."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)

    result: dict = {
        "pbs_jobs": [],
        "pbs_running": False,
        "restic_next": None,
        "restic_running": False,
    }

    try:
        pve = PVEClient(host)
        result["pbs_jobs"] = pve.get_backup_schedules()
        result["pbs_running"] = pve.is_backup_running()
    except Exception:
        pass

    if host.restic_repo and host.restic_password:
        try:
            restic = ResticClient(host)
            result["restic_next"] = restic.get_next_run()
            result["restic_running"] = restic.is_running()
        except Exception:
            pass

    return jsonify(result)


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
        total = 2 if (run_restic_after and host.restic_repo) else 2
        try:
            pve = PVEClient(host)
            nodes = pve.get_nodes()
            node = nodes[0]
            log(f"Step 1/{total} — Triggering PBS backup: {vm_type}/{vmid} on node {node}")
            upid = pve.backup_vm(vmid, vm_type, host.pbs_storage_id, node)
            log(f"Task started: {upid}")
            if isinstance(upid, str) and upid.startswith("UPID:"):
                if not (run_restic_after and host.restic_repo):
                    log(f"Step 2/{total} — Backup running, waiting for completion...")
                ok = pve.wait_for_task(node, upid, log)
                if not ok:
                    raise RuntimeError("PBS backup task failed")
            log("Backup complete.")
            if run_restic_after and host.restic_repo:
                restic = ResticClient(host)
                log(f"Step 2/{total} — Uploading to cloud (restic)...")
                restic.backup_datastore(host.pbs_datastore_path, log, _pbs_vm_ids(host))
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

        total = (3 if source == "cloud" else 2) + (1 if run_backup_after else 0)
        step = 0
        def s(msg):
            nonlocal step; step += 1
            log(f"Step {step}/{total} — {msg}")

        if source == "cloud":
            if not restic_id:
                raise ValueError("restic_id required for cloud restore")
            restic = ResticClient(host)
            s(f"Restoring PBS datastore from restic snapshot {restic_id[:8]}...")
            log("WARNING: This will overwrite the current PBS datastore.")
            log(f"Connecting to PVE host {restic._ssh_host} via SSH...")
            log("Stopping PBS...")
            restic._ssh_run(
                "systemctl stop proxmox-backup proxmox-backup-proxy 2>/dev/null || true"
            )
            try:
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
        s(f"Stopping {vm_type}/{vmid} and initiating PBS restore...")
        pve.stop_vm(vmid, vm_type, node)
        log(f"Restoring {vm_type}/{vmid} snapshot {backup_time_iso} from {host.pbs_storage_id}")
        upid = pve.restore_vm(vmid, vm_type, backup_time_iso, host.pbs_storage_id, node,
                              host.pbs_datastore)
        log(f"Task started: {upid}")
        s("Restore running, waiting for completion...")
        ok = pve.wait_for_task(node, upid, log)
        if not ok:
            raise RuntimeError("Restore task failed")
        log("Restore complete.")
        log(f"Starting {vm_type}/{vmid}...")
        pve.start_vm(vmid, vm_type, node)

        if run_backup_after:
            s("Triggering PBS backup after restore...")
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
            log("Step 1/1 — Uploading PBS datastore to cloud (restic)...")
            restic.backup_datastore(host.pbs_datastore_path, log, _pbs_vm_ids(host))
            _cache.pop(f"items:{host_id}", None)
        finally:
            restic_lock.release()

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


# ──────────────────────────────────────────────
# Delete
# ──────────────────────────────────────────────

@app.post("/api/host/<host_id>/delete/pbs")
def delete_pbs(host_id: str):
    """Delete a single local PBS snapshot.

    Body: { vmid, type, backup_time }
    """
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    body = request.get_json() or {}
    vmid        = int(body.get("vmid", 0))
    vm_type     = body.get("type", "vm")
    if vm_type == "lxc":
        vm_type = "ct"
    backup_time = body.get("backup_time")
    if not vmid or backup_time is None:
        abort(400, "vmid and backup_time required")

    dt_label = datetime.fromtimestamp(backup_time, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    job_id = create_job(f"Delete local PBS snapshot {vm_type}/{vmid} @ {dt_label}")

    def work(log):
        pbs = PBSClient(host)
        log(f"Step 1/1 — Deleting PBS snapshot {vm_type}/{vmid} @ {dt_label}...")
        pbs.delete_snapshot(vm_type, str(vmid), backup_time)
        log("Done.")
        _cache.pop(f"items:{host_id}", None)

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/delete/pbs/all")
def delete_pbs_all(host_id: str):
    """Delete all local PBS snapshots for a VM/LXC.

    Body: { vmid, type }
    """
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    body = request.get_json() or {}
    vmid    = int(body.get("vmid", 0))
    vm_type = body.get("type", "vm")
    if vm_type == "lxc":
        vm_type = "ct"
    if not vmid:
        abort(400, "vmid required")

    job_id = create_job(f"Delete all local PBS snapshots for {vm_type}/{vmid}")

    def work(log):
        pbs = PBSClient(host)
        log(f"Step 1/1 — Deleting all PBS snapshots for {vm_type}/{vmid}...")
        count = pbs.delete_all_snapshots_for_vm(vm_type, str(vmid), log)
        log(f"Deleted {count} snapshot(s).")
        _cache.pop(f"items:{host_id}", None)

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/delete/cloud")
def delete_cloud(host_id: str):
    """Delete a snapshot that exists ONLY in cloud (no local PBS copy).

    Sequence: restore PBS from restic → delete PBS snapshot → re-backup PBS
              → forget old restic snapshot.

    Body: { vmid, type, backup_time, restic_id }
    """
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    if not host.restic_repo:
        abort(400, "restic not configured for this host")

    body = request.get_json() or {}
    vmid        = int(body.get("vmid", 0))
    vm_type     = body.get("type", "vm")
    if vm_type == "lxc":
        vm_type = "ct"
    backup_time = body.get("backup_time")
    restic_id   = body.get("restic_id")
    if not vmid or backup_time is None or not restic_id:
        abort(400, "vmid, backup_time and restic_id required")

    restic_lock = _get_restic_lock(host_id)
    if not restic_lock.acquire(blocking=False):
        abort(409, "A restic operation is already running for this host")

    dt_label = datetime.fromtimestamp(backup_time, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    job_id = create_job(f"Delete cloud-only snapshot {vm_type}/{vmid} @ {dt_label}")

    def work(log):
        restic = ResticClient(host)
        pbs    = PBSClient(host)

        try:
            # Step 1/4 — restore PBS datastore from restic
            log("Step 1/4 — Restoring PBS datastore from restic snapshot...")
            log("WARNING: PBS will be stopped during this operation.")
            restic._ssh_run(
                "systemctl stop proxmox-backup proxmox-backup-proxy 2>/dev/null || true"
            )
            try:
                restic.restore_datastore(restic_id, log)
            finally:
                restic._ssh_run(
                    "systemctl start proxmox-backup proxmox-backup-proxy 2>/dev/null || true"
                )
            log("PBS restarted. Waiting for it to become ready...")
            for attempt in range(1, 25):
                try:
                    pbs.get_snapshots()
                    log(f"PBS ready after {attempt * 5}s.")
                    break
                except Exception as e:
                    log(f"PBS not ready yet ({attempt}/24): {e}")
                    time.sleep(5)
            else:
                raise RuntimeError("PBS did not become ready within 120s after restore")

            # Step 2/4 — delete the target PBS snapshot.
            # After restore the PBS snapshot time comes from the datastore, not from
            # the restic snapshot timestamp (the two differ by a few seconds).
            # Find the closest PBS snapshot within ±120s of the restic snapshot time.
            log(f"Step 2/4 — Deleting PBS snapshot {vm_type}/{vmid} @ {dt_label}...")
            pbs_groups = pbs.get_snapshots()
            pbs_times_for_vm = [
                s["backup_time"]
                for g in pbs_groups
                if g["pve_id"] == vmid and g["backup_type"] == vm_type
                for s in g["snapshots"]
            ]
            # Pick the PBS snapshot whose time is closest to the restic snapshot time
            actual_backup_time = min(
                pbs_times_for_vm,
                key=lambda t: abs(t - backup_time),
                default=None,
            )
            if actual_backup_time is None or abs(actual_backup_time - backup_time) > 120:
                raise RuntimeError(
                    f"Could not find PBS snapshot for {vm_type}/{vmid} near {dt_label} "
                    f"(candidates: {pbs_times_for_vm})"
                )
            pbs.delete_snapshot(vm_type, str(vmid), actual_backup_time)
            log("PBS snapshot deleted.")

            # Step 3/4 — re-backup PBS datastore to restic (without the deleted snapshot)
            log("Step 3/4 — Re-uploading PBS datastore to restic...")
            restic.backup_datastore(host.pbs_datastore_path, log, _pbs_vm_ids(host))

            # Step 4/4 — forget ALL restic snapshots that carry the deleted PBS timestamp.
            # Other restic snapshots (e.g. BU3 which was taken when T1+T2+T3 all existed)
            # also contain the ct-{vmid}-{backup_time} tag and would re-surface T2 as
            # cloud-only unless forgotten here too.
            log("Step 4/4 — Finding all restic snapshots covering the deleted timestamp...")
            by_vm_post, _ = restic.get_snapshots_by_vm()
            ids_to_forget = list({
                e["id"] for e in by_vm_post.get(vmid, [])
                if e.get("pbs_time") == backup_time
            })
            if not ids_to_forget:
                ids_to_forget = [restic_id]  # fallback: old-style tags without pbs_time
            log(f"Step 4/4 — Removing {len(ids_to_forget)} restic snapshot(s) "
                f"tagged {vm_type}-{vmid}-{backup_time}...")
            restic.forget_snapshots(ids_to_forget, log)

            _cache.pop(f"items:{host_id}", None)
            log("Done.")
        finally:
            restic_lock.release()

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/delete/both")
def delete_both(host_id: str):
    """Delete a snapshot that exists locally AND in cloud.

    Sequence: delete PBS snapshot → re-backup PBS → forget old restic snapshot.
    (No restore needed since local PBS copy exists.)

    Body: { vmid, type, backup_time, restic_id }
    """
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    if not host.restic_repo:
        abort(400, "restic not configured for this host")

    body = request.get_json() or {}
    vmid        = int(body.get("vmid", 0))
    vm_type     = body.get("type", "vm")
    if vm_type == "lxc":
        vm_type = "ct"
    backup_time = body.get("backup_time")
    restic_id   = body.get("restic_id")
    if not vmid or backup_time is None or not restic_id:
        abort(400, "vmid, backup_time and restic_id required")

    restic_lock = _get_restic_lock(host_id)
    if not restic_lock.acquire(blocking=False):
        abort(409, "A restic operation is already running for this host")

    dt_label = datetime.fromtimestamp(backup_time, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    job_id = create_job(f"Delete local+cloud snapshot {vm_type}/{vmid} @ {dt_label}")

    def work(log):
        restic = ResticClient(host)
        pbs    = PBSClient(host)

        try:
            # Step 1/3 — delete local PBS snapshot
            log(f"Step 1/3 — Deleting local PBS snapshot {vm_type}/{vmid} @ {dt_label}...")
            pbs.delete_snapshot(vm_type, str(vmid), backup_time)
            log("PBS snapshot deleted.")

            # Step 2/3 — re-backup PBS datastore to restic (without the deleted snapshot)
            log("Step 2/3 — Re-uploading PBS datastore to restic...")
            restic.backup_datastore(host.pbs_datastore_path, log, _pbs_vm_ids(host))

            # Step 3/3 — forget the old restic snapshot
            log(f"Step 3/3 — Removing old restic snapshot {restic_id[:8]}...")
            restic.forget_snapshots([restic_id], log)

            _cache.pop(f"items:{host_id}", None)
            log("Done.")
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


@app.get("/api/jobs/active")
def get_active_jobs_endpoint():
    """Return all currently running or pending jobs."""
    return jsonify(get_active_jobs())


# ──────────────────────────────────────────────
# Restic snapshot list
# ──────────────────────────────────────────────

@app.get("/api/host/<host_id>/restic/snapshots")
def restic_snapshot_list(host_id: str):
    """List all restic snapshots with per-PBS-snapshot local coverage info."""
    host = HOSTS.get(host_id)
    if not host or not host.restic_repo:
        abort(404)

    restic = ResticClient(host)
    restic_busy = restic.is_running()
    snaps = None
    if restic_busy:
        snaps = _restic_snap_cache.get(f"flat:{host_id}")
        if snaps is None:
            return jsonify({"busy": True, "message": "Restic is currently running — no cached data available yet."}), 503
    else:
        try:
            snaps = restic.get_snapshots_flat()
            _restic_snap_cache[f"flat:{host_id}"] = [dict(s) for s in snaps]
        except Exception as e:
            return jsonify({"busy": True, "message": f"Could not fetch restic snapshots: {e}"}), 503

    # Annotate with PBS local coverage
    try:
        pbs = PBSClient(host)
        pbs_groups = pbs.get_snapshots()
    except Exception:
        pbs_groups = []
    local_pbs: set[tuple] = {
        (g["backup_type"], g["pve_id"], s["backup_time"])
        for g in pbs_groups
        for s in g["snapshots"]
    }

    import copy
    snaps = copy.deepcopy(snaps)
    now_ts = time.time()
    for snap in snaps:
        snap["date"] = datetime.fromtimestamp(snap["ts"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        snap["age"]  = _human_age(now_ts - snap["ts"])
        snap["size"] = _fmt_bytes(snap["size_bytes"])
        for cov in snap["covers"]:
            if cov["pbs_time"] is not None:
                cov["local"]    = (cov["type"], cov["vmid"], cov["pbs_time"]) in local_pbs
                cov["pbs_date"] = datetime.fromtimestamp(
                    cov["pbs_time"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            else:
                cov["local"]    = None
                cov["pbs_date"] = None

    return jsonify({"snaps": snaps, "restic_busy": restic_busy})


@app.post("/api/host/<host_id>/delete/restic")
def delete_restic(host_id: str):
    """Delete a specific restic snapshot by ID (forget + prune + cleanup).

    Body: { restic_id }
    """
    host = HOSTS.get(host_id)
    if not host or not host.restic_repo:
        abort(404)

    body = request.get_json() or {}
    restic_id = body.get("restic_id")
    if not restic_id:
        abort(400, "restic_id required")

    restic_lock = _get_restic_lock(host_id)
    if not restic_lock.acquire(blocking=False):
        abort(409, "A restic operation is already running for this host")

    job_id = create_job(f"Delete restic snapshot {restic_id[:8]}")

    def work(log):
        restic = ResticClient(host)
        try:
            log(f"Step 1/1 — Deleting restic snapshot {restic_id[:8]}...")
            restic.forget_snapshots([restic_id], log)
            _cache.pop(f"items:{host_id}", None)
            log("Done.")
        finally:
            restic_lock.release()

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/restore/datastore")
def restore_datastore(host_id: str):
    """Restore the full PBS datastore from a restic snapshot (no per-VM PBS restore).

    Body: { restic_id }
    """
    host = HOSTS.get(host_id)
    if not host or not host.restic_repo:
        abort(404)

    body = request.get_json() or {}
    restic_id = body.get("restic_id")
    if not restic_id:
        abort(400, "restic_id required")

    restic_lock = _get_restic_lock(host_id)
    if not restic_lock.acquire(blocking=False):
        abort(409, "A restic operation is already running for this host")

    job_id = create_job(f"Restore PBS datastore from restic {restic_id[:8]}")

    def work(log):
        restic = ResticClient(host)
        pbs    = PBSClient(host)
        try:
            log("Step 1/2 — Restoring PBS datastore from restic snapshot...")
            log("WARNING: PBS will be stopped and current datastore overwritten.")
            restic._ssh_run(
                "systemctl stop proxmox-backup proxmox-backup-proxy 2>/dev/null || true"
            )
            try:
                restic.restore_datastore(restic_id, log)
            finally:
                log("Starting PBS...")
                restic._ssh_run(
                    "systemctl start proxmox-backup proxmox-backup-proxy 2>/dev/null || true"
                )
            log("Step 2/2 — Waiting for PBS to become ready...")
            for attempt in range(1, 25):
                try:
                    pbs.get_snapshots()
                    log(f"PBS ready after {attempt * 5}s.")
                    break
                except Exception as e:
                    log(f"PBS not ready yet ({attempt}/24): {e}")
                    time.sleep(5)
            else:
                raise RuntimeError("PBS did not become ready within 120s after restore")
            _cache.pop(f"items:{host_id}", None)
            log("Done.")
        finally:
            restic_lock.release()

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


def _fmt_bytes(n: int) -> str:
    if n >= 1024**3:
        return f"{n / 1024**3:.1f} GB"
    if n >= 1024**2:
        return f"{n / 1024**2:.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.0f} KB"
    return f"{n} B"


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
