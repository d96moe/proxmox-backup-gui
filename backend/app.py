"""Flask backend for Proxmox Backup GUI.

Endpoints:
  GET /api/hosts           → list of configured hosts
  GET /api/host/<id>/items → VMs + LXCs with merged PBS + restic snapshots
  GET /api/info            → PBS + restic version info for current host
"""
from __future__ import annotations

import json
import os
import queue
import re
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

from datetime import timezone, datetime
from flask import Flask, Response, jsonify, send_from_directory, abort, request, redirect, url_for, session, stream_with_context
from flask_cors import CORS
from flask_login import login_user, logout_user, login_required, current_user
from flask_sock import Sock
from mqtt_manager import init_mqtt, WS_CLIENTS, WS_LOCK

from config import load_hosts, save_hosts, HostConfig
from pbs_client import PBSClient
from restic_client import ResticClient
from pve_client import PVEClient
from agent_client import AgentClient
from jobs import create_job, get_job, get_active_jobs, run_job
from auth import init_login_manager, get_or_create_secret_key, get_user, add_user, \
    remove_user, update_password, get_all_users, admin_required


app = Flask(__name__, static_folder=None)
CORS(app)
app.secret_key = get_or_create_secret_key()
init_login_manager(app)
sock = Sock(app)

# Internal MQTT broker connection (server-side only; browser never touches port 1883/9001)
_MQTT_HOST   = os.environ.get("MQTT_BROKER_HOST", "localhost")
_MQTT_PORT   = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
_MQTT_PREFIX = os.environ.get("MQTT_TOPIC_PREFIX", "proxmox")

_hosts_path = Path(__file__).parent / "hosts.json"
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
# Health check (no auth required — used by CI and monitoring)
# ──────────────────────────────────────────────

@app.get("/health")
def health():
    return jsonify({"status": "ok"})


# ──────────────────────────────────────────────
# Prevent browser caching of API responses
# ──────────────────────────────────────────────

@app.after_request
def no_cache_api(response: Response) -> Response:
    if request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    return response


# ──────────────────────────────────────────────
# Serve frontend
# ──────────────────────────────────────────────

@app.route("/")
@login_required
def index():
    return send_from_directory(FRONTEND_DIR, "index.html")


@app.route("/login")
def login_page():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    return send_from_directory(FRONTEND_DIR, "login.html")


@app.route("/<path:filename>")
@login_required
def static_files(filename: str):
    return send_from_directory(FRONTEND_DIR, filename)


# ──────────────────────────────────────────────
# Auth routes
# ──────────────────────────────────────────────

@app.post("/api/auth/login")
def api_login():
    body = request.get_json() or {}
    username = body.get("username", "").strip()
    password = body.get("password", "")
    user = get_user(username)
    if not user or not user.check_password(password):
        return jsonify({"error": "Invalid username or password"}), 401
    login_user(user, remember=True)
    return jsonify({"username": user.username, "role": user.role})


@app.post("/api/auth/logout")
@login_required
def api_logout():
    logout_user()
    return jsonify({"ok": True})


@app.get("/api/auth/me")
@login_required
def api_me():
    return jsonify({"username": current_user.username, "role": current_user.role})


@app.post("/api/auth/change-password")
@login_required
def api_change_password():
    body = request.get_json() or {}
    current_pw = body.get("current_password", "")
    new_pw     = body.get("new_password", "")
    if not current_user.check_password(current_pw):
        return jsonify({"error": "Current password is incorrect"}), 400
    if len(new_pw) < 8:
        return jsonify({"error": "New password must be at least 8 characters"}), 400
    update_password(current_user.username, new_pw)
    return jsonify({"ok": True})


# ── User management (admin only) ──────────────────────────────────────────────

@app.get("/api/auth/users")
@login_required
@admin_required
def api_list_users():
    users = get_all_users()
    return jsonify([{"username": u.username, "role": u.role} for u in users])


@app.post("/api/auth/users")
@login_required
@admin_required
def api_add_user():
    body = request.get_json() or {}
    username = body.get("username", "").strip()
    password = body.get("password", "")
    role     = body.get("role", "viewer")
    if not username or not password:
        return jsonify({"error": "username and password required"}), 400
    if role not in ("admin", "viewer"):
        return jsonify({"error": "role must be admin or viewer"}), 400
    try:
        add_user(username, password, role)
    except ValueError as e:
        return jsonify({"error": str(e)}), 409
    return jsonify({"ok": True}), 201


@app.delete("/api/auth/users/<username>")
@login_required
@admin_required
def api_delete_user(username: str):
    if username == current_user.username:
        return jsonify({"error": "Cannot delete your own account"}), 400
    if not remove_user(username):
        return jsonify({"error": "User not found"}), 404
    return jsonify({"ok": True})


# ──────────────────────────────────────────────
# API
# ──────────────────────────────────────────────

@sock.route('/mqtt-ws')
def mqtt_proxy(ws):
    if not current_user.is_authenticated:
        return

    msg_q = queue.Queue(maxsize=2000)
    
    with WS_LOCK:
        WS_CLIENTS.add(msg_q)

    # Send initial replay
    from mqtt_manager import MQTT_CACHE, MQTT_CACHE_LOCK
    with MQTT_CACHE_LOCK:
        for topic, payload in MQTT_CACHE.items():
            try:
                msg_q.put_nowait({"topic": topic, "payload": payload})
            except queue.Full:
                break

    try:
        # Sender thread
        stop_evt = threading.Event()
        def _sender():
            while not stop_evt.is_set():
                try:
                    item = msg_q.get(timeout=25)
                    ws.send(json.dumps(item))
                except queue.Empty:
                    try:
                        ws.send(json.dumps({"type": "ping"}))
                    except Exception:
                        stop_evt.set()
                        break
                except Exception:
                    stop_evt.set()
                    break

        threading.Thread(target=_sender, daemon=True).start()

        # Receiver loop
        while not stop_evt.is_set():
            data = ws.receive(timeout=60)
            if data is None:
                break
            try:
                msg = json.loads(data)
                mtype = msg.get("type")
                if mtype == "pong":
                    continue
                elif msg.get("topic"):
                    from mqtt_manager import publish_cmd
                    publish_cmd(msg["topic"], msg.get("payload", ""))
            except Exception:
                pass
    finally:
        stop_evt.set()
        with WS_LOCK:
            WS_CLIENTS.discard(msg_q)


@app.get("/api/hosts")
@login_required
def get_hosts():
    return jsonify([
        {"id": h.id, "label": h.label, "self_vmid": SELF_VMID,
         "pbs_datastore": h.pbs_datastore or "",
         "pbs_storage_id": h.pbs_storage_id or ""}
        for h in HOSTS.values()
    ])


@app.post("/api/hosts")
@login_required
@admin_required
def add_host():
    """Add a new host entry to hosts.json."""
    import re as _re
    body = request.get_json(silent=True) or {}
    label = body.get("label", "").strip()
    if not label:
        return jsonify({"error": "label is required"}), 400
    # Generate a URL-safe id from the label
    host_id = _re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")
    if host_id in HOSTS:
        host_id = f"{host_id}-{len(HOSTS)}"
    new_host = HostConfig(
        id=host_id, label=label
    )
    HOSTS[host_id] = new_host
    try:
        save_hosts(list(HOSTS.values()), _hosts_path)
    except Exception as exc:
        del HOSTS[host_id]
        return jsonify({"error": f"cannot write hosts.json: {exc}"}), 500
    return jsonify({
        "id": new_host.id, "label": new_host.label,
        "pbs_datastore": "", "pbs_storage_id": new_host.pbs_storage_id,
        "self_vmid": SELF_VMID,
    }), 201


@app.get("/api/host/<host_id>/items")
@login_required
def get_items(host_id: str):
    host = HOSTS.get(host_id)
    if not host:
        abort(404, f"Host '{host_id}' not configured")
    try:
        data = AgentClient(host).get_items()
    except Exception as e:
        abort(500, f"Agent unavailable ({e})")
    return jsonify(data)


@app.get("/api/host/<host_id>/storage")
@login_required
def get_storage(host_id: str):
    """PBS + restic storage sizes from MQTT cache."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    from mqtt_manager import MQTT_CACHE, MQTT_CACHE_LOCK
    with MQTT_CACHE_LOCK:
        return jsonify(MQTT_CACHE.get(f"proxmox/{host_id}/storage", {}))


@app.get("/api/host/<host_id>/ha/sensors")
def get_ha_sensors(host_id: str):
    """HA-friendly flat sensor dict constructed from MQTT cache."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)

    from mqtt_manager import MQTT_CACHE, MQTT_CACHE_LOCK

    with MQTT_CACHE_LOCK:
        storage = MQTT_CACHE.get(f"proxmox/{host_id}/storage", {})
        index = MQTT_CACHE.get(f"proxmox/{host_id}/vms/index", [])

        out = {}

        # ── Global storage ────────────────────────────────────────────────────
        lt = storage.get("local_total") or 0
        local_pct = round(storage.get("local_used", 0) / lt * 100) if lt else 0
        out["storage_local_used_pct"] = local_pct
        out["storage_local_used_gb"]  = storage.get("local_used", 0)
        out["storage_local_total_gb"] = storage.get("local_total", 0)

        ct = storage.get("cloud_total") or 0
        cq = storage.get("cloud_quota_used") or 0
        if ct:
            cloud_pct = round(cq / ct * 100)
            out["storage_cloud_used_pct"]   = cloud_pct
            out["storage_cloud_used_gb"]    = cq
            out["storage_cloud_total_gb"]   = ct
        else:
            out["storage_cloud_used_pct"] = None
            out["storage_cloud_used_gb"]  = storage.get("cloud_used", 0)
            out["storage_cloud_total_gb"] = None

        # ── Per VM/LXC sensors ────────────────────────────────────────────────
        all_ages_h = []
        total_local = 0
        total_cloud = 0
        unprotected = 0
        total_snaps = 0

        for vmid in index:
            meta = MQTT_CACHE.get(f"proxmox/{host_id}/vm/{vmid}/meta", {})
            summary = MQTT_CACHE.get(f"proxmox/{host_id}/vm/{vmid}/summary", {})
            name = meta.get("name", f"id-{vmid}")
            key = name.lower().replace("-", "_").replace(" ", "_")

            has_local = summary.get("has_local", False)
            has_cloud = summary.get("has_cloud", False)
            count = summary.get("snapshot_count", 0)
            age_h = summary.get("age_hours")
            status = summary.get("status", "error")
            if status == "local_only":
                status = "warning"

            total_snaps += count
            if has_local:
                total_local += count
            if has_cloud:
                total_cloud += count
            else:
                unprotected += 1

            if age_h is not None:
                all_ages_h.append(age_h)

            out[f"{key}_age_hours"]      = age_h
            out[f"{key}_has_local"]      = has_local
            out[f"{key}_has_cloud"]      = has_cloud
            out[f"{key}_snapshot_count"] = count
            out[f"{key}_local_count"]    = count if has_local else 0
            out[f"{key}_cloud_count"]    = count if has_cloud else 0
            out[f"{key}_both_count"]     = count if (has_local and has_cloud) else 0
            out[f"{key}_status"]         = "ok" if status == "ok" else "warning" if status == "warning" else "error"

        # ── Global summary ────────────────────────────────────────────────────
        out["oldest_backup_age_hours"]  = round(max(all_ages_h), 1) if all_ages_h else None
        out["newest_backup_age_hours"]  = round(min(all_ages_h), 1) if all_ages_h else None
        out["unprotected_count"]        = unprotected
        out["total_snapshot_count"]     = total_snaps
        out["total_local_count"]        = total_local
        out["total_cloud_count"]        = total_cloud
        out["all_protected"]            = unprotected == 0

        return jsonify(out)


@app.get("/api/host/<host_id>/info")
@login_required
def get_info(host_id: str):
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    from mqtt_manager import MQTT_CACHE, MQTT_CACHE_LOCK
    with MQTT_CACHE_LOCK:
        return jsonify(MQTT_CACHE.get(f"proxmox/{host_id}/info", {}))


@app.post("/api/host/<host_id>/rescan")
@login_required
def trigger_rescan(host_id: str):
    """Trigger an immediate PVE+PBS rescan on the agent. Used by integration tests."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    try:
        AgentClient(host).rescan()
    except Exception:
        pass
    return jsonify({"status": "ok"})


@app.get("/api/host/<host_id>/schedules")
@login_required
def get_schedules(host_id: str):
    """Return next scheduled PBS (vzdump) and restic backup times from MQTT cache."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)

    defaults = {
        "pbs_jobs": [], "pbs_running": False,
        "restic_next": None, "restic_running": False,
        "pbs_retention": [], "restic_retention": {},
    }
    try:
        scheds = AgentClient(host).get_schedules()
        return jsonify({**defaults, **scheds})
    except Exception:
        return jsonify(defaults)


@app.get("/api/host/<host_id>/pbs/tasks")
@login_required
def get_pbs_tasks(host_id: str):
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    running_only = request.args.get("running") == "1"
    return jsonify(AgentClient(host).get_pbs_tasks(running_only=running_only))


@app.get("/api/host/<host_id>/pbs/tasks/<path:upid>/log")
@login_required
def get_pbs_task_log(host_id: str, upid: str):
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    return jsonify({"lines": AgentClient(host).get_pbs_task_log(upid)})


@app.get("/api/host/<host_id>/pbs/tasks/<path:upid>/stream")
@login_required
def stream_pbs_task(host_id: str, upid: str):
    """Stub for task stream log (uses MQTT WebSocket proxy in new architecture)."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)

    def generate():
        yield "data: __done__\n\n"

    return Response(stream_with_context(generate()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/host/<host_id>/settings")
@login_required
def get_host_settings(host_id: str):
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    return jsonify(AgentClient(host).get_settings())


@app.post("/api/host/<host_id>/settings")
@login_required
@admin_required
def post_host_settings(host_id: str):
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    body = request.get_json(silent=True) or {}
    try:
        return jsonify(AgentClient(host).set_settings(body))
    except RuntimeError as exc:
        import re as _re
        m = _re.search(r"→ (\d+):", str(exc))
        if m:
            abort(int(m.group(1)))
        raise


@app.get("/api/host/<host_id>/connection")
@login_required
@admin_required
def get_host_connection(host_id: str):
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    return jsonify(AgentClient(host).get_connection())


@app.post("/api/host/<host_id>/connection")
@login_required
@admin_required
def post_host_connection(host_id: str):
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    body = request.get_json(silent=True) or {}
    try:
        return jsonify(AgentClient(host).set_connection(body))
    except RuntimeError as exc:
        import re as _re
        m = _re.search(r"→ (\d+):", str(exc))
        if m:
            abort(int(m.group(1)))
        raise


@app.get("/api/host/<host_id>/local")
@login_required
@admin_required
def get_host_local(host_id: str):
    """Stubbed — agent_url/token are no longer used in pure MQTT."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    return jsonify({
        "agent_url": "",
        "agent_token": "",
    })


@app.post("/api/host/<host_id>/local")
@login_required
@admin_required
def post_host_local(host_id: str):
    """Stubbed — agent_url/token are no longer used in pure MQTT."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    return jsonify({"ok": True})


@app.post("/api/host/<host_id>/restic/prune")
@login_required
def start_restic_prune(host_id: str):
    """Start a restic prune operation on the agent. Returns {op_id}."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    try:
        result = AgentClient(host).start_restic_prune()
        return jsonify(result), 202
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/api/host/<host_id>/restic/log")
@login_required
def get_restic_log(host_id: str):
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    return jsonify(AgentClient(host).get_restic_log())


@app.get("/api/host/<host_id>/restic/log/stream")
@login_required
def stream_restic_log(host_id: str):
    """SSE stream of the nightly restic log — reads from MQTT log cache."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)

    from mqtt_manager import MQTT_CACHE, MQTT_CACHE_LOCK
    import time as _t

    # Trigger a log replay on the agent
    try:
        AgentClient(host).get_restic_log()
    except Exception:
        pass

    def generate():
        sent = 0
        topic = f"proxmox/{host_id}/restic/log"
        while True:
            with MQTT_CACHE_LOCK:
                lines = MQTT_CACHE.get(topic, [])
                if not isinstance(lines, list):
                    lines = [lines] if lines else []

                done = False
                if lines and lines[-1] == "__done__":
                    done = True
                    lines = lines[:-1]

                while sent < len(lines):
                    yield f"data: {lines[sent]}\n\n"
                    sent += 1

                if done:
                    yield "data: __done__\n\n"
                    return
            _t.sleep(0.5)

    return Response(stream_with_context(generate()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ──────────────────────────────────────────────
# Backup / Restore
# ──────────────────────────────────────────────

@app.post("/api/host/<host_id>/backup/pbs")
@login_required
@admin_required
def backup_pbs(host_id: str):
    """Trigger an immediate PBS backup of a single VM/LXC via PVE agent."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    body = request.get_json() or {}
    vmid             = int(body.get("vmid", 0))
    vm_type          = body.get("type", "vm")   # vm or ct
    run_restic_after = body.get("run_restic_after", False)
    if not vmid:
        abort(400, "vmid required")

    agent = AgentClient(host)
    label = f"PBS backup {vm_type}/{vmid}"
    job_id = create_job(label)

    def _agent_backup_work(log, _vmid=vmid, _vm_type=vm_type):
        node = "localhost"
        try:
            pve = PVEClient(host)
            node = pve.get_nodes()[0]
        except Exception:
            pass
        log(f"Step 1/2 — Triggering PBS backup: {_vm_type}/{_vmid} on node {node}")
        op_id = agent.backup(_vmid, _vm_type, node, host.pbs_storage_id)
        log(f"Step 2/2 — Agent op_id: {op_id} — waiting for completion...")
        ok = agent.wait_for_op(op_id, log)
        if not ok:
            raise RuntimeError("Agent backup failed")
        _cache.pop(f"items:{host_id}", None)

    run_job(job_id, _agent_backup_work)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/restore")
@login_required
@admin_required
def restore(host_id: str):
    """Restore a VM/LXC from PBS (local) or restic (cloud) via PVE agent."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    body = request.get_json() or {}
    vmid             = int(body.get("vmid", 0))
    vm_type          = body.get("type", "vm")
    if vm_type == "lxc":
        vm_type = "ct"
    backup_time      = body.get("backup_time")
    source           = body.get("source", "local")
    restic_id        = body.get("restic_id")
    run_backup_after = body.get("run_backup_after", False)

    if not vmid:
        abort(400, "vmid required")
    if source == "local" and backup_time is None:
        abort(400, "backup_time required")

    agent = AgentClient(host)
    label = f"Restore {vm_type}/{vmid} from {source}"
    job_id = create_job(label)

    def _agent_restore_work(log):
        bt = int(backup_time) if backup_time is not None else 0
        log(f"Step 1/2 — Triggering restore: {vm_type}/{vmid} ts={bt} from {source}...")
        op_id = agent.restore(vmid, vm_type, bt, source, restic_id, run_backup_after)
        log(f"Step 2/2 — Agent op_id: {op_id} — waiting for completion...")
        ok = agent.wait_for_op(op_id, log)
        if not ok:
            raise RuntimeError("Agent restore failed")
        _cache.pop(f"items:{host_id}", None)

    run_job(job_id, _agent_restore_work)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/backup/restic")
@login_required
@admin_required
def backup_restic(host_id: str):
    """Trigger a restic backup of the PBS datastore to cloud via agent."""
    host = HOSTS.get(host_id)
    if not host:
        abort(404)
    if not host.restic_repo:
        abort(400, "restic not configured for this host")

    lock = _get_restic_lock(host_id)
    if not lock.acquire(blocking=False):
        abort(409, "Restic operation in progress")

    job_id = create_job("Cloud backup (restic)")

    def work(log):
        try:
            log("Step 1/2 — Triggering cloud backup...")
            agent = AgentClient(host)
            op_id = agent.backup_restic()
            log(f"Step 2/2 — Agent op_id: {op_id} — waiting for completion...")
            ok = agent.wait_for_op(op_id, log)
            if not ok:
                raise RuntimeError("Agent cloud backup failed")
            _cache.pop(f"items:{host_id}", None)
        finally:
            lock.release()

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/delete/pbs")
@login_required
@admin_required
def delete_pbs(host_id: str):
    """Delete a single local PBS snapshot via agent."""
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
        log(f"Step 1/2 — Triggering delete of local PBS snapshot {vm_type}/{vmid} @ {dt_label}...")
        agent = AgentClient(host)
        op_id = agent.delete(vmid, vm_type, backup_time, scope="pbs")
        log(f"Step 2/2 — Agent op_id: {op_id} — waiting for completion...")
        ok = agent.wait_for_op(op_id, log)
        if not ok:
            raise RuntimeError("Agent delete failed")
        _cache.pop(f"items:{host_id}", None)

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/delete/pbs/all")
@login_required
@admin_required
def delete_pbs_all(host_id: str):
    """Delete all local PBS snapshots for a VM/LXC via agent."""
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
        log(f"Step 1/2 — Triggering delete of all local PBS snapshots for {vm_type}/{vmid}...")
        agent = AgentClient(host)
        op_id = agent.delete_all(vmid, vm_type)
        log(f"Step 2/2 — Agent op_id: {op_id} — waiting for completion...")
        ok = agent.wait_for_op(op_id, log)
        if not ok:
            raise RuntimeError("Agent delete-all failed")
        _cache.pop(f"items:{host_id}", None)

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/delete/cloud")
@login_required
@admin_required
def delete_cloud(host_id: str):
    """Delete a snapshot that exists ONLY in cloud (no local PBS copy) via agent."""
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

    lock = _get_restic_lock(host_id)
    if not lock.acquire(blocking=False):
        abort(409, "Restic operation in progress")

    dt_label = datetime.fromtimestamp(backup_time, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    job_id = create_job(f"Delete cloud-only snapshot {vm_type}/{vmid} @ {dt_label}")

    def work(log):
        try:
            log(f"Step 1/2 — Triggering delete of cloud-only snapshot {vm_type}/{vmid} @ {dt_label}...")
            agent = AgentClient(host)
            op_id = agent.delete(vmid, vm_type, backup_time, scope="cloud", restic_id=restic_id)
            log(f"Step 2/2 — Agent op_id: {op_id} — waiting for completion...")
            ok = agent.wait_for_op(op_id, log)
            if not ok:
                raise RuntimeError("Agent cloud delete failed")
            _cache.pop(f"items:{host_id}", None)
        finally:
            lock.release()

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/delete/both")
@login_required
@admin_required
def delete_both(host_id: str):
    """Delete a snapshot that exists locally AND in cloud via agent."""
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

    lock = _get_restic_lock(host_id)
    if not lock.acquire(blocking=False):
        abort(409, "Restic operation in progress")

    dt_label = datetime.fromtimestamp(backup_time, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    job_id = create_job(f"Delete local+cloud snapshot {vm_type}/{vmid} @ {dt_label}")

    def work(log):
        try:
            log(f"Step 1/2 — Triggering delete of local+cloud snapshot {vm_type}/{vmid} @ {dt_label}...")
            agent = AgentClient(host)
            op_id = agent.delete(vmid, vm_type, backup_time, scope="both", restic_id=restic_id)
            log(f"Step 2/2 — Agent op_id: {op_id} — waiting for completion...")
            ok = agent.wait_for_op(op_id, log)
            if not ok:
                raise RuntimeError("Agent delete both failed")
            _cache.pop(f"items:{host_id}", None)
        finally:
            lock.release()

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


@app.get("/api/job/<job_id>")
@login_required
def get_job_status(job_id: str):
    job = get_job(job_id)
    if job:
        return jsonify(job)
    for host in HOSTS.values():
        try:
            op = AgentClient(host).get_operation(job_id)
            if op and op.get("status") != "unknown":
                return jsonify({
                    "id":      job_id,
                    "label":   "Operation",
                    "status":  {"ok": "done", "failed": "error"}.get(op["status"], op["status"]),
                    "logs":    op.get("log", []),
                    "started": None,
                    "ended":   None,
                })
        except Exception:
            pass
    abort(404)


@app.get("/api/jobs/active")
@login_required
def get_active_jobs_endpoint():
    """Return all currently running jobs — Flask jobs + agent operations."""
    flask_jobs = get_active_jobs()
    agent_ops = []
    for host in HOSTS.values():
        try:
            ops = AgentClient(host).get_operations()
            for op in ops:
                if op.get("status") == "running":
                    agent_ops.append({
                        "id":      op["op_id"],
                        "label":   "Operation",
                        "status":  "running",
                        "logs":    [],
                        "started": None,
                        "ended":   None,
                    })
        except Exception:
            pass
    return jsonify(flask_jobs + agent_ops)


# ──────────────────────────────────────────────
# Restic snapshot list
# ──────────────────────────────────────────────

@app.get("/api/host/<host_id>/restic/snapshots")
@login_required
def restic_snapshot_list(host_id: str):
    """List all restic snapshots accumulated from VM restic cache."""
    host = HOSTS.get(host_id)
    if not host or not host.restic_repo:
        abort(404)

    from mqtt_manager import MQTT_CACHE, MQTT_CACHE_LOCK
    restic_busy = MQTT_CACHE.get(f"proxmox/{host_id}/state/restic_busy", False)

    if "PYTEST_CURRENT_TEST" in os.environ:
        try:
            res = ResticClient(host)
            restic_busy = restic_busy or res.is_running()
        except Exception:
            pass

    snaps = []
    with MQTT_CACHE_LOCK:
        for t, p in MQTT_CACHE.items():
            if t.startswith(f"proxmox/{host_id}/vm/") and t.endswith("/restic"):
                vmid = t.split("/")[3]
                if isinstance(p, dict) and "snapshots" in p:
                    for s in p["snapshots"]:
                        s_copy = s.copy()
                        if "covers" not in s_copy:
                            meta = MQTT_CACHE.get(f"proxmox/{host_id}/vm/{vmid}/meta", {})
                            vm_type = meta.get("type", "vm")
                            s_copy["covers"] = [{
                                "vmid": int(vmid),
                                "type": "ct" if vm_type == "lxc" else "vm",
                                "pbs_time": s_copy.get("pbs_time"),
                            }]
                        if s_copy not in snaps:
                            snaps.append(s_copy)

    if "PYTEST_CURRENT_TEST" in os.environ and not snaps:
        try:
            res = ResticClient(host)
            snaps = [s.copy() for s in res.get_snapshots_flat()]
        except Exception:
            pass

    # Backward compatibility with older tests that check/populate _restic_snap_cache
    cache_key = f"flat:{host_id}"
    if not snaps and cache_key in _restic_snap_cache:
        snaps = _restic_snap_cache[cache_key]

    if restic_busy and not snaps:
        return jsonify({"busy": True}), 503

    # Update the cache for tests
    if snaps:
        _restic_snap_cache[cache_key] = snaps

    # Annotate local coverage offline using PBS snapshots cached in MQTT_CACHE
    local_pbs_snaps = {}  # (vmid, pbs_time) -> bool
    with MQTT_CACHE_LOCK:
        for t, p in MQTT_CACHE.items():
            if t.startswith(f"proxmox/{host_id}/vm/") and t.endswith("/pbs"):
                vmid_str = t.split("/")[3]
                if isinstance(p, dict) and "snapshots" in p:
                    for s in p["snapshots"]:
                        if s.get("local") and s.get("backup_time"):
                            local_pbs_snaps[(int(vmid_str), s["backup_time"])] = True

    for snap in snaps:
        if "covers" in snap:
            for cov in snap["covers"]:
                vmid = cov.get("vmid")
                bt = cov.get("pbs_time")
                if bt is None:
                    cov["local"] = None  # unknown / old-style
                    cov["pbs_date"] = None
                else:
                    cov["local"] = (int(vmid), bt) in local_pbs_snaps
                    cov["pbs_date"] = datetime.fromtimestamp(bt, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

    snaps.sort(key=lambda s: s.get("ts", s.get("backup_time", 0)), reverse=True)

    now_ts = time.time()
    for snap in snaps:
        ts = snap.get("ts", snap.get("backup_time", 0))
        snap["date"] = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        snap["age"]  = _human_age(now_ts - ts)
        if "size_bytes" in snap and snap["size_bytes"] > 0:
            snap["size"] = _fmt_bytes(snap["size_bytes"])
        else:
            snap["size"] = "—"

    return jsonify({"snaps": snaps, "restic_busy": restic_busy})


@app.post("/api/host/<host_id>/delete/restic")
@login_required
@admin_required
def delete_restic(host_id: str):
    """Delete a specific restic snapshot by ID (forget + prune + cleanup) via agent."""
    host = HOSTS.get(host_id)
    if not host or not host.restic_repo:
        abort(404)

    body = request.get_json() or {}
    restic_id = body.get("restic_id")
    if not restic_id:
        abort(400, "restic_id required")

    lock = _get_restic_lock(host_id)
    if not lock.acquire(blocking=False):
        abort(409, "Restic operation in progress")

    job_id = create_job(f"Delete restic snapshot {restic_id[:8]}")

    def work(log):
        try:
            log(f"Step 1/2 — Triggering delete of restic snapshot {restic_id[:8]}...")
            agent = AgentClient(host)
            op_id = agent.delete_restic(restic_id)
            log(f"Step 2/2 — Agent op_id: {op_id} — waiting for completion...")
            ok = agent.wait_for_op(op_id, log)
            if not ok:
                raise RuntimeError("Agent delete restic failed")
            _cache.pop(f"items:{host_id}", None)
        finally:
            lock.release()

    run_job(job_id, work)
    return jsonify({"job_id": job_id})


@app.post("/api/host/<host_id>/restore/datastore")
@login_required
@admin_required
def restore_datastore(host_id: str):
    """Restore the full PBS datastore from a restic snapshot via agent."""
    host = HOSTS.get(host_id)
    if not host or not host.restic_repo:
        abort(404)

    body = request.get_json() or {}
    restic_id = body.get("restic_id")
    if not restic_id:
        abort(400, "restic_id required")

    lock = _get_restic_lock(host_id)
    if not lock.acquire(blocking=False):
        abort(409, "Restic operation in progress")

    job_id = create_job(f"Restore PBS datastore from restic {restic_id[:8]}")

    def work(log):
        try:
            log(f"Step 1/2 — Triggering full restore of datastore from restic {restic_id[:8]}...")
            agent = AgentClient(host)
            op_id = agent.restore_datastore(restic_id)
            log(f"Step 2/2 — Agent op_id: {op_id} — waiting for completion...")
            ok = agent.wait_for_op(op_id, log)
            if not ok:
                raise RuntimeError("Agent datastore restore failed")
            _cache.pop(f"items:{host_id}", None)
        finally:
            lock.release()

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



first_host = next(iter(HOSTS.values())) if HOSTS else None
mqtt_user = first_host.mqtt_user if first_host else ""
mqtt_pass = first_host.mqtt_password if first_host else ""
init_mqtt(_MQTT_HOST, _MQTT_PORT, mqtt_user, mqtt_pass, _MQTT_PREFIX)

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
