"""PVE Agent — lightweight HTTP JSON API running on the PVE host.

Replaces SSH + scattered API calls with a single local endpoint that the
GUI backend (in LXC 300) can talk to over the internal vmbr0 network.

Inspired by Backrest's operation model:
  - Every long-running task (backup / restore) gets an op_id
  - Status tracked: pending → running → ok | failed
  - SSE stream delivers log lines live; replays full log when op is done

Bind to 10.10.0.1:8099 so only LXC containers on vmbr0 can reach it.
"""
from __future__ import annotations

import json
import re
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterator

from flask import Flask, Response, jsonify, request, stream_with_context

# Re-use existing client code — agent runs on PVE host alongside them.
from pbs_client import PBSClient
from pve_client import PVEClient

VERSION = "0.1.0"
_start_time = time.monotonic()

app = Flask(__name__)


@app.before_request
def _check_auth():
    """Require Bearer token when agent_token is configured."""
    if not _cfg or not _cfg.agent_token:
        return  # open — no token configured
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {_cfg.agent_token}":
        return jsonify({"error": "Unauthorized"}), 401


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AgentConfig:
    pve_url: str
    pve_user: str
    pve_password: str
    pbs_url: str
    pbs_user: str
    pbs_password: str
    pbs_datastore: str
    pbs_storage_id: str
    pbs_datastore_path: str
    pve_ssh_host: str
    restic_repo: str
    restic_password: str
    verify_ssl: bool = False
    restic_env: dict = field(default_factory=dict)
    agent_token: str = ""        # if set, all requests must present "Authorization: Bearer <token>"

    def to_host_config(self):
        """Return a HostConfig-compatible object for existing clients."""
        from config import HostConfig
        return HostConfig(
            id="agent",
            label="Agent",
            pve_url=self.pve_url,
            pve_user=self.pve_user,
            pve_password=self.pve_password,
            pbs_url=self.pbs_url,
            pbs_user=self.pbs_user,
            pbs_password=self.pbs_password,
            pbs_datastore=self.pbs_datastore,
            pbs_storage_id=self.pbs_storage_id,
            pbs_datastore_path=self.pbs_datastore_path,
            pve_ssh_host=self.pve_ssh_host,
            restic_repo=self.restic_repo,
            restic_password=self.restic_password,
            verify_ssl=self.verify_ssl,
            restic_env=self.restic_env,
        )


# Module-level config — replaced by tests via patch("pve_agent._cfg", ...)
_cfg: AgentConfig | None = None


def _host():
    """Return HostConfig from current _cfg (raises if not configured)."""
    if _cfg is None:
        raise RuntimeError("Agent not configured — set pve_agent._cfg")
    return _cfg.to_host_config()


# ─────────────────────────────────────────────────────────────────────────────
# LocalResticClient — subprocess-based, no SSH (agent runs on PVE host)
# ─────────────────────────────────────────────────────────────────────────────

class LocalResticClient:
    """Run restic and related commands directly via subprocess — no SSH."""

    def __init__(self, cfg: AgentConfig) -> None:
        self._repo = cfg.restic_repo
        self._env = {
            "RESTIC_REPOSITORY": cfg.restic_repo,
            "RESTIC_PASSWORD": cfg.restic_password,
            "RESTIC_PROGRESS_FPS": "0.5",
            "RCLONE_DRIVE_USE_TRASH": "false",
            **cfg.restic_env,
        }
        # Merge with minimal OS environment for PATH resolution
        import os
        self._full_env = {**os.environ, **self._env}

    def _run(self, cmd: list[str], timeout: int = 120) -> str:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=timeout, env=self._full_env,
        )
        if result.returncode != 0:
            raise RuntimeError(f"{cmd[0]} failed: {result.stderr.strip()}")
        return result.stdout

    def get_snapshots_flat(self) -> list[dict]:
        """Returns all restic snapshots as a flat list, newest first.

        Each entry: { id, short_id, ts, size_bytes, covers: [{type, vmid, pbs_time}] }
        """
        raw = json.loads(self._run(["restic", "snapshots", "--json", "--no-lock"]))
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
                "id": snap_id, "short_id": short_id,
                "ts": ts, "size_bytes": size_bytes, "covers": covers,
            })
        return sorted(result, key=lambda x: x["ts"], reverse=True)

    def get_next_run(self) -> dict | None:
        """Return next scheduled run of the restic systemd timer."""
        try:
            stdout = self._run(
                ["systemctl", "list-timers", "--no-pager"], timeout=10
            )
            for line in stdout.splitlines():
                if "restic" in line.lower():
                    tokens = line.split()
                    if len(tokens) >= 5:
                        next_at = " ".join(tokens[:4])
                        left = tokens[4]
                        return {"next": next_at, "left": left}
        except Exception:
            pass
        return None

    def is_running(self) -> bool:
        """Return True if a restic process is currently running."""
        try:
            result = subprocess.run(
                ["pgrep", "-x", "restic"],
                capture_output=True, timeout=5,
            )
            return result.returncode == 0
        except Exception:
            return False

    def get_retention(self) -> dict:
        """Read RESTIC_RETENTION_KEEP_* settings from config.env."""
        try:
            with open("/etc/proxmox-backup-restore/config.env") as f:
                content = f.read()
        except OSError:
            return {}
        mapping = {
            "RESTIC_RETENTION_KEEP_LAST":    "keep-last",
            "RESTIC_RETENTION_KEEP_DAILY":   "keep-daily",
            "RESTIC_RETENTION_KEEP_WEEKLY":  "keep-weekly",
            "RESTIC_RETENTION_KEEP_MONTHLY": "keep-monthly",
            "RESTIC_RETENTION_KEEP_YEARLY":  "keep-yearly",
        }
        result = {}
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k in mapping and v:
                result[mapping[k]] = v
        return result

    def get_pbs_prune_jobs(self) -> list[dict]:
        """Read PBS prune job settings via proxmox-backup-manager."""
        try:
            stdout = self._run(
                ["proxmox-backup-manager", "prune-job", "list", "--output-format", "json"],
                timeout=10,
            )
            jobs = json.loads(stdout.strip() or "[]")
            return jobs if isinstance(jobs, list) else []
        except Exception:
            return []


# ─────────────────────────────────────────────────────────────────────────────
# Operation store  (in-memory, Backrest-inspired)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Operation:
    op_id: str
    type: str                       # "backup" | "restore" | "prune"
    status: str = "pending"         # pending → running → ok | failed
    log: list[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None

    def append_log(self, line: str) -> None:
        self.log.append(line)

    def to_dict(self, *, full=True) -> dict:
        d = {
            "op_id":      self.op_id,
            "type":       self.type,
            "status":     self.status,
            "created_at": self.created_at,
        }
        if self.finished_at is not None:
            d["finished_at"] = self.finished_at
        if full:
            d["log"] = list(self.log)
        return d


_operations: dict[str, Operation] = {}
_ops_lock = threading.Lock()


def _new_op(op_type: str) -> Operation:
    op = Operation(op_id=str(uuid.uuid4()), type=op_type)
    with _ops_lock:
        _operations[op.op_id] = op
    return op


def _get_op(op_id: str) -> Operation | None:
    with _ops_lock:
        return _operations.get(op_id)


# ─────────────────────────────────────────────────────────────────────────────
# Background task runner
# ─────────────────────────────────────────────────────────────────────────────

def _run_in_background(op: Operation, fn) -> None:
    """Execute fn(op) in a daemon thread; set status on finish."""
    def _worker():
        op.status = "running"
        try:
            fn(op)
            op.status = "ok"
        except Exception as exc:
            op.append_log(f"ERROR: {exc}")
            op.status = "failed"
        finally:
            op.finished_at = time.time()

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


# ─────────────────────────────────────────────────────────────────────────────
# Routes — health
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({
        "status":  "ok",
        "version": VERSION,
        "uptime":  time.monotonic() - _start_time,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Routes — VMs
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/vms")
def vms():
    try:
        pve = PVEClient(_host())
        raw = pve.get_vms_and_lxcs()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    result = []
    for vmid, info in raw.items():
        result.append({"vmid": vmid, **info})
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# Routes — items (full combined view: VMs + annotated snapshots + cloud-only)
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/items")
def items():
    """Return all VMs/LXCs with PBS snapshots annotated for cloud coverage.

    Single call replaces N calls to /snapshots/<type>/<vmid> — one PBS query
    and one restic query total, then builds the full items structure including
    cloud-only entries (restic snapshots where the PBS copy has been pruned).

    Returns: {vms: [...], lxcs: [...], storage: {}, pbs_stale: false}
    """
    try:
        pve = PVEClient(_host())
        pve_meta = pve.get_vms_and_lxcs()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    # ── PBS: all snapshot groups ──────────────────────────────────────────────
    pbs_groups: list[dict] = []
    try:
        pbs = PBSClient(_host())
        pbs_groups = pbs.get_snapshots()
    except Exception:
        pass  # PBS down — restic data may still work

    # Build snap_map: vmid → [snap_dict]
    snap_map: dict[int, list] = {}
    pbs_type_map: dict[int, str] = {}  # vmid → "ct"/"vm"
    for group in pbs_groups:
        vid = int(group["pve_id"]) if str(group["pve_id"]).isdigit() else group["pve_id"]
        btype = group.get("backup_type", "vm")
        pbs_type_map[vid] = btype
        for snap in group.get("snapshots", []):
            snap["local"] = True
            snap.setdefault("cloud", False)
        snap_map.setdefault(vid, []).extend(group.get("snapshots", []))

    # ── Restic: all snapshots in one call ─────────────────────────────────────
    restic_flat: list[dict] = []
    if _cfg and _cfg.restic_repo:
        try:
            res = LocalResticClient(_cfg)
            restic_flat = res.get_snapshots_flat()  # newest first
        except Exception:
            pass

    # Index: vmid → pbs_time → newest restic snap
    res_by_vmid_pbstime: dict[int, dict[int, dict]] = {}
    # Index: vmid → [restic snaps covering this vmid]
    res_by_vmid: dict[int, list[dict]] = {}
    for s in restic_flat:
        for cov in s.get("covers", []):
            vid = cov.get("vmid")
            pt = cov.get("pbs_time")
            if vid is None:
                continue
            res_by_vmid.setdefault(vid, []).append(s)
            if pt is not None:
                # setdefault: first wins (newest-first list → newest wins)
                res_by_vmid_pbstime.setdefault(vid, {}).setdefault(pt, s)

    # ── Annotate PBS snapshots with cloud coverage ────────────────────────────
    pbs_times_per_vm: dict[int, set[int]] = {}
    for vid, snaps in snap_map.items():
        local_times: set[int] = set()
        for snap in snaps:
            bt = snap.get("backup_time")
            local_times.add(bt)
            rs = res_by_vmid_pbstime.get(vid, {}).get(bt)
            if rs:
                snap["cloud"] = True
                snap["restic_id"] = rs["id"]
                snap["restic_short_id"] = rs.get("short_id", rs["id"][:8])
        pbs_times_per_vm[vid] = local_times

    # ── Cloud-only entries: restic covers a pbs_time no longer in PBS ─────────
    added_cloud: set[tuple] = set()  # (vid, pbs_time)
    now_ts = time.time()
    for vid, r_snaps in res_by_vmid.items():
        local_times = pbs_times_per_vm.get(vid, set())
        for s in r_snaps:  # newest first
            for cov in s.get("covers", []):
                if cov.get("vmid") != vid:
                    continue
                pt = cov.get("pbs_time")
                if pt is None or pt in local_times:
                    continue
                key = (vid, pt)
                if key in added_cloud:
                    continue
                added_cloud.add(key)
                dt = datetime.fromtimestamp(pt, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                snap_map.setdefault(vid, []).append({
                    "backup_time": pt,
                    "date": dt,
                    "local": False,
                    "cloud": True,
                    "incremental": True,
                    "size": "—",
                    "restic_id": s["id"],
                    "restic_short_id": s.get("short_id", s["id"][:8]),
                })

    # ── Sort snapshots newest-first, attach age ───────────────────────────────
    for snaps in snap_map.values():
        snaps.sort(key=lambda s: s["backup_time"], reverse=True)
        for snap in snaps:
            age_secs = now_ts - snap["backup_time"]
            snap["age"] = _human_age(age_secs)

    # ── Build final item lists ────────────────────────────────────────────────
    vms_list: list[dict] = []
    lxcs_list: list[dict] = []
    all_vmids = set(pve_meta.keys()) | set(snap_map.keys())

    for vid in sorted(all_vmids):
        _pbs_btype = pbs_type_map.get(vid, "vm")
        _orphan_type = "lxc" if _pbs_btype == "ct" else "vm"
        meta = pve_meta.get(vid, {
            "name": f"id-{vid}", "type": _orphan_type,
            "os": "linux", "status": "unknown",
        })
        item = {
            "id":        vid,
            "name":      meta.get("name", f"id-{vid}"),
            "type":      meta.get("type", _orphan_type),
            "os":        meta.get("os", "linux"),
            "status":    meta.get("status", "unknown"),
            "template":  meta.get("template", False),
            "in_pve":    vid in pve_meta,
            "snapshots": snap_map.get(vid, []),
            "restic_snaps": res_by_vmid.get(vid, []),
        }
        vm_type = meta.get("type", _orphan_type)
        if vm_type == "lxc":
            lxcs_list.append(item)
        else:
            vms_list.append(item)

    return jsonify({
        "vms":      vms_list,
        "lxcs":     lxcs_list,
        "storage":  {},
        "pbs_stale": False,
    })


def _human_age(secs: float) -> str:
    """Human-readable age string (e.g. '2h 15m', '3d')."""
    if secs < 0:
        return "0m"
    m = int(secs // 60)
    h = m // 60
    d = h // 24
    if d:
        return f"{d}d {h % 24}h" if h % 24 else f"{d}d"
    if h:
        return f"{h}h {m % 60}m" if m % 60 else f"{h}h"
    return f"{m}m"


# ─────────────────────────────────────────────────────────────────────────────
# Routes — snapshots
# ─────────────────────────────────────────────────────────────────────────────

_VALID_TYPES = {"vm", "ct"}


@app.route("/snapshots/<vm_type>/<int:vmid>")
def snapshots(vm_type: str, vmid: int):
    if vm_type not in _VALID_TYPES:
        return jsonify({"error": f"Invalid vm_type '{vm_type}' — use vm or ct"}), 400

    vmid_str = str(vmid)
    # PBS — get_snapshots() returns [{pve_id, backup_type, snapshots:[...]}]
    # Find the group for this vmid, return its snapshots list.
    pbs_result = []
    try:
        pbs = PBSClient(_host())
        groups = pbs.get_snapshots()
        for group in groups:
            if str(group.get("pve_id")) == vmid_str:
                pbs_result = group.get("snapshots", [])
                break
    except Exception:
        pass  # PBS down — return empty, restic may still work

    # Restic — get_snapshots_flat() returns {id, ts, size_bytes, covers:[{type,vmid,pbs_time}]}
    # Filter snapshots that cover this vmid; also collect which pbs_times are covered.
    restic_result = []
    restic_pbs_times: set[int] = set()
    try:
        res = LocalResticClient(_cfg)
        flat = res.get_snapshots_flat()
        for s in flat:
            covers = s.get("covers", [])
            vm_covers = [c for c in covers if str(c.get("vmid")) == vmid_str]
            if vm_covers:
                restic_result.append(s)
                for c in vm_covers:
                    if c.get("pbs_time") is not None:
                        restic_pbs_times.add(c["pbs_time"])
    except Exception:
        pass

    # Annotate PBS snapshots with cloud coverage + restic_id
    # Build pbs_time → newest restic snap (flat is already newest-first)
    restic_by_pbs_time: dict[int, dict] = {}
    for s in restic_result:
        for c in s.get("covers", []):
            pt = c.get("pbs_time")
            if pt is not None and str(c.get("vmid")) == vmid_str:
                restic_by_pbs_time.setdefault(pt, s)  # first wins (newest)

    for snap in pbs_result:
        bt = snap.get("backup_time")
        rs = restic_by_pbs_time.get(bt)
        snap["cloud"] = rs is not None
        if rs:
            snap["restic_id"] = rs["id"]
            snap["restic_short_id"] = rs.get("short_id", rs["id"][:8])

    return jsonify({"pbs": pbs_result, "restic": restic_result})


@app.route("/snapshots/<vm_type>/<vmid>")  # catches non-int vmid for 400
def snapshots_bad_type(vm_type: str, vmid: str):
    if vm_type not in _VALID_TYPES:
        return jsonify({"error": f"Invalid vm_type '{vm_type}'"}), 400
    return jsonify({"error": f"Invalid vmid '{vmid}'"}), 400


@app.route("/snapshots/<vm_type>/<int:vmid>/<ts>", methods=["DELETE"])
def delete_snapshot(vm_type: str, vmid: int, ts: str):
    if vm_type not in _VALID_TYPES:
        return jsonify({"error": f"Invalid vm_type '{vm_type}'"}), 400
    try:
        ts_int = int(ts)
    except ValueError:
        return jsonify({"error": f"Invalid timestamp '{ts}'"}), 400
    try:
        pbs = PBSClient(_host())
        pbs.delete_snapshot(vm_type, str(vmid), ts_int)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify({"ok": True})


# ─────────────────────────────────────────────────────────────────────────────
# Routes — operations
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/operations")
def ops_list():
    with _ops_lock:
        items = [op.to_dict(full=False) for op in _operations.values()]
    # newest first
    items.sort(key=lambda o: o["created_at"], reverse=True)
    return jsonify(items)


@app.route("/operations/<op_id>")
def op_get(op_id: str):
    op = _get_op(op_id)
    if op is None:
        return jsonify({"error": "Not found"}), 404
    return jsonify(op.to_dict())


@app.route("/operations/<op_id>/stream")
def op_stream(op_id: str):
    op = _get_op(op_id)
    if op is None:
        return jsonify({"error": "Not found"}), 404

    def _generate() -> Iterator[str]:
        # If already finished, replay the full log immediately.
        if op.status in ("ok", "failed"):
            for line in op.log:
                yield f"data: {line}\n\n"
            yield "data: __done__\n\n"
            return

        # Still running — stream as lines appear.
        sent = 0
        while True:
            current_log = op.log
            while sent < len(current_log):
                yield f"data: {current_log[sent]}\n\n"
                sent += 1
            if op.status in ("ok", "failed"):
                # Drain any remaining lines, then close.
                current_log = op.log
                while sent < len(current_log):
                    yield f"data: {current_log[sent]}\n\n"
                    sent += 1
                yield "data: __done__\n\n"
                return
            time.sleep(0.1)

    return Response(
        stream_with_context(_generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/operations/backup", methods=["POST"])
def op_backup():
    body = request.get_json(silent=True) or {}
    vmid    = body.get("vmid")
    node    = body.get("node")
    vm_type = body.get("vm_type", "vm")
    storage = body.get("storage")

    if vmid is None:
        return jsonify({"error": "vmid required"}), 400
    if node is None:
        return jsonify({"error": "node required"}), 400
    if storage is None:
        return jsonify({"error": "storage required"}), 400

    op = _new_op("backup")

    def _do(op: Operation):
        pve = PVEClient(_host())
        op.append_log(f"Starting backup: vmid={vmid} node={node} storage={storage}")
        upid = pve.backup_vm(int(vmid), vm_type, storage, node)
        op.append_log(f"Task started: {upid}")
        ok = pve.wait_for_task(node, upid, op.append_log)
        if not ok:
            raise RuntimeError("PVE task did not complete successfully")
        op.append_log("Backup complete")

    _run_in_background(op, _do)
    return jsonify({"op_id": op.op_id}), 202


@app.route("/operations/restore", methods=["POST"])
def op_restore():
    body = request.get_json(silent=True) or {}
    vmid            = body.get("vmid")
    vm_type         = body.get("vm_type", "vm")
    node            = body.get("node")
    storage_id      = body.get("storage_id")
    backup_time_iso = body.get("backup_time_iso")
    pbs_datastore   = body.get("pbs_datastore")

    missing = [f for f, v in [
        ("vmid", vmid), ("node", node), ("storage_id", storage_id),
        ("backup_time_iso", backup_time_iso), ("pbs_datastore", pbs_datastore),
    ] if v is None]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400

    op = _new_op("restore")

    def _do(op: Operation):
        pve = PVEClient(_host())
        op.append_log(f"Starting restore: vmid={vmid} node={node} ts={backup_time_iso}")
        upid = pve.restore_vm(int(vmid), vm_type, backup_time_iso,
                              storage_id, node, pbs_datastore)
        op.append_log(f"Task started: {upid}")
        ok = pve.wait_for_task(node, upid, op.append_log)
        if not ok:
            raise RuntimeError("PVE restore task did not complete successfully")
        op.append_log("Restore complete")

    _run_in_background(op, _do)
    return jsonify({"op_id": op.op_id}), 202


# ─────────────────────────────────────────────────────────────────────────────
# Routes — schedules
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/schedules")
def schedules():
    result = {
        "pbs_jobs":        [],
        "pbs_running":     False,
        "restic_next":     None,
        "restic_running":  False,
        "pbs_retention":   [],
        "restic_retention": {},
    }

    try:
        pve = PVEClient(_host())
        result["pbs_jobs"]    = pve.get_backup_schedules()
        result["pbs_running"] = pve.is_backup_running()
    except Exception:
        pass

    try:
        cfg = _cfg
        res = LocalResticClient(_cfg)
        result["restic_next"]      = res.get_next_run()
        result["restic_running"]   = res.is_running()
        result["restic_retention"] = res.get_retention()
        prune_jobs                 = res.get_pbs_prune_jobs()
        result["pbs_retention"]    = [
            j for j in prune_jobs
            if j.get("store") == (cfg.pbs_datastore if cfg else "")
        ]
    except Exception:
        pass

    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def _load_config(path: str = "/etc/pve-agent/config.json") -> AgentConfig:
    with open(path) as f:
        d = json.load(f)
    return AgentConfig(**d)


if __name__ == "__main__":
    import sys
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "/etc/pve-agent/config.json"
    _cfg = _load_config(cfg_path)
    # Bind to internal bridge only — not reachable from outside the PVE host
    app.run(host="10.10.0.1", port=8099, threaded=True)
