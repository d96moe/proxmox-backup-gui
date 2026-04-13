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
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterator

from flask import Flask, Response, jsonify, request, stream_with_context

# Re-use existing client code — agent runs on PVE host alongside them.
from pbs_client import PBSClient
from pve_client import PVEClient
from restic_client import ResticClient

VERSION = "0.1.0"
_start_time = time.monotonic()

app = Flask(__name__)

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
        res = ResticClient(_host())
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

    # Annotate PBS snapshots with cloud coverage
    for snap in pbs_result:
        snap["cloud"] = snap.get("backup_time") in restic_pbs_times

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
        res = ResticClient(_host())
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
