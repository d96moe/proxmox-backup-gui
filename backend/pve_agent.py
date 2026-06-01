"""PVE Agent — lightweight HTTP JSON API running on the PVE host.

Replaces SSH + scattered API calls with a single local endpoint that the
GUI backend (in LXC 300) can talk to over the internal vmbr0 network.

Inspired by Backrest's operation model:
  - Every long-running task (backup / restore) gets an op_id
  - Status tracked: pending → running → ok | failed
  - SSE stream delivers log lines live; replays full log when op is done

MQTT events (optional — requires mqtt_host in config):
  proxmox/<hostname>/agent/status         → online/offline (LWT, retained)
  proxmox/<hostname>/vm/<id>/backup/status   → idle/running/done/failed (retained)
  proxmox/<hostname>/vm/<id>/backup/progress → {"pct":42,"speed_mbps":…,"eta_s":…}
  proxmox/<hostname>/vm/<id>/backup/last_ok  → ISO timestamp (retained)
  proxmox/<hostname>/ops/<op_id>/log      → individual log lines

HA MQTT Discovery payloads published automatically per VM (lazy, on first op).

Bind to 10.10.0.1:8099 so only LXC containers on vmbr0 can reach it.
"""
from __future__ import annotations

import json
import logging
import re
import socket
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterator

# Re-use existing client code — agent runs on PVE host alongside them.
from pbs_client import PBSClient
from pve_client import PVEClient

VERSION = "0.1.0"
_start_time = time.monotonic()

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# MQTT publisher (optional — only active when mqtt_host is configured)
# ─────────────────────────────────────────────────────────────────────────────

class MQTTPublisher:
    """Thin paho-mqtt wrapper with LWT and HA Discovery support.

    All publishes are fire-and-forget (QoS 0 for progress, QoS 1 for state).
    Connection runs in a background thread via loop_start().
    """

    def __init__(self, host: str, port: int = 1883,
                 user: str = "", password: str = "",
                 hostname: str = "") -> None:
        import paho.mqtt.client as mqtt

        self._hostname = hostname or socket.gethostname()
        self._base     = f"proxmox/{self._hostname}"
        self._discovered: set[str] = set()  # vmids already announced to HA
        self._lock = threading.Lock()

        lwt_topic = f"{self._base}/agent/status"

        self._client = mqtt.Client(client_id=f"pve-agent-{self._hostname}")
        self._client.will_set(lwt_topic, "offline", retain=True, qos=1)

        if user:
            self._client.username_pw_set(user, password)

        self._client.on_connect    = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message    = self._on_message_bootstrap
        # Populated on connect from ALL retained vm/+/meta topics in the broker.
        # Covers stale VMIDs (e.g. destroyed CI clones) that were never recorded
        # in vms/index. StatePoller uses this to clear topics for gone VMIDs.
        self._bootstrap_vmids: set[str] = set()

        try:
            self._client.connect_async(host, port, keepalive=60)
            self._client.loop_start()
        except Exception as exc:
            log.warning("MQTT connect_async failed: %s", exc)

    # ── internal callbacks ────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("MQTT connected — publishing online status")
            client.publish(f"{self._base}/agent/status", "online",
                           retain=True, qos=1)
            # Subscribe to command topics
            client.subscribe(f"{self._base}/cmd/+", qos=1)
            # Subscribe to vm/+/meta to discover ALL VMIDs with retained topics —
            # not just those in vms/index. Retained messages arrive immediately on
            # subscribe; msg.retain=True distinguishes them from live publishes.
            # This catches stale VMIDs (e.g. destroyed CI clones) that were never
            # recorded in vms/index so StatePoller can clear them from the broker.
            self._bootstrap_vmids = set()
            client.subscribe(f"{self._base}/vm/+/meta", qos=0)
            client.subscribe(f"{self._base}/vms/index", qos=1)
        else:
            log.warning("MQTT connect failed rc=%s", rc)

    def _on_message_bootstrap(self, client, userdata, msg):
        """Collect VMIDs from retained broker messages on startup."""
        topic  = msg.topic
        base   = self._base
        prefix = f"{base}/vm/"

        # Collect any VMID that has a retained meta topic in the broker.
        # msg.retain=True → stored retained message (may be stale).
        # msg.retain=False → live publish from this or another agent (skip).
        if msg.retain and topic.startswith(prefix) and topic.endswith("/meta") and msg.payload:
            vmid = topic[len(prefix):-len("/meta")]
            if vmid not in self._bootstrap_vmids:
                self._bootstrap_vmids.add(vmid)
                log.debug("Bootstrap: found retained VMID %s in broker", vmid)

        # Also seed from vms/index (handles case where meta topics already cleared)
        if topic == f"{base}/vms/index" and msg.payload:
            try:
                vmids = json.loads(msg.payload)
                self._bootstrap_vmids.update(str(v) for v in vmids)
                log.debug("Bootstrap VMIDs from vms/index: %s", self._bootstrap_vmids)
            except Exception:
                pass
            client.unsubscribe(f"{base}/vms/index")

        # Forward command messages to the normal handler
        if "/cmd/" in topic:
            self._on_message(client, userdata, msg)

    def _on_message(self, client, userdata, msg):
        cmd = msg.topic.split("/")[-1]
        body: dict = {}
        try:
            body = json.loads(msg.payload) if msg.payload else {}
        except Exception:
            pass
        if cmd == "rescan" and _poller:
            log.info("MQTT cmd/rescan received — triggering immediate scan")
            _poller.rescan_now()
        elif cmd == "backup":
            threading.Thread(target=self._handle_cmd_backup, args=(body,),
                             daemon=True, name="mqtt-cmd-backup").start()
        elif cmd == "restore":
            threading.Thread(target=self._handle_cmd_restore, args=(body,),
                             daemon=True, name="mqtt-cmd-restore").start()
        elif cmd == "delete":
            threading.Thread(target=self._handle_cmd_delete, args=(body,),
                             daemon=True, name="mqtt-cmd-delete").start()
        elif cmd == "delete-all":
            threading.Thread(target=self._handle_cmd_delete_all, args=(body,),
                             daemon=True, name="mqtt-cmd-delete-all").start()
        elif cmd == "backup-restic":
            threading.Thread(target=self._handle_cmd_backup_restic, args=(body,),
                             daemon=True, name="mqtt-cmd-backup-restic").start()
        elif cmd == "delete-restic":
            threading.Thread(target=self._handle_cmd_delete_restic, args=(body,),
                             daemon=True, name="mqtt-cmd-delete-restic").start()
        elif cmd == "restore-datastore":
            threading.Thread(target=self._handle_cmd_restore_datastore, args=(body,),
                             daemon=True, name="mqtt-cmd-restore-datastore").start()
        elif cmd == "replay_op_log":
            threading.Thread(target=self._handle_cmd_replay_op_log, args=(body,),
                             daemon=True, name="mqtt-cmd-replay-op-log").start()
        elif cmd == "replay_pbs_log":
            threading.Thread(target=self._handle_cmd_replay_pbs_log, args=(body,),
                             daemon=True, name="mqtt-cmd-replay-pbs-log").start()
        elif cmd == "replay_restic_log":
            threading.Thread(target=self._handle_cmd_replay_restic_log, args=(body,),
                             daemon=True, name="mqtt-cmd-replay-restic-log").start()
        elif cmd == "replay_op_log":
            threading.Thread(target=self._handle_cmd_replay_op_log, args=(body,),
                             daemon=True, name="mqtt-cmd-replay-op-log").start()
        elif cmd == "replay_pbs_log":
            threading.Thread(target=self._handle_cmd_replay_pbs_log, args=(body,),
                             daemon=True, name="mqtt-cmd-replay-pbs-log").start()
        elif cmd == "replay_restic_log":
            threading.Thread(target=self._handle_cmd_replay_restic_log, args=(body,),
                             daemon=True, name="mqtt-cmd-replay-restic-log").start()

    def _ack(self, corr_id: str, op_id: str) -> None:
        """Publish job ACK so the browser can start polling the operation."""
        self._client.publish(
            f"{self._base}/job/{corr_id}/ack",
            json.dumps({"op_id": op_id}), retain=False, qos=1,
        )

    def _node(self) -> str:
        if _cfg and _cfg.pve_node:
            return _cfg.pve_node
        return _cfg.mqtt_hostname if _cfg and _cfg.mqtt_hostname else socket.gethostname()

    def _handle_cmd_backup(self, body: dict) -> None:
        vmid    = body.get("vmid")
        vm_type = body.get("type", "vm")
        btype   = body.get("btype", "pbs")
        corr_id = body.get("corr_id", str(uuid.uuid4()))
        run_restic_after = body.get("run_restic_after", False)
        if not vmid or not _cfg:
            return
        node    = self._node()
        storage = _cfg.pbs_storage_id
        op      = _new_op("backup", vmid=str(vmid))
        self._ack(corr_id, op.op_id)
        log.info("MQTT cmd/backup: %s/%s corr_id=%s op=%s", vm_type, vmid, corr_id, op.op_id)

        def _do(op: Operation):
            pve = PVEClient(_host())
            total = 2 if (run_restic_after and _cfg and _cfg.restic_repo) else 2
            op.append_log(f"Step 1/{total} — Starting PBS backup: {vm_type}/{vmid}")
            upid = pve.backup_vm(int(vmid), vm_type, storage, node)
            op.append_log(f"Task: {upid}")
            op.append_log(f"Step 2/{total} — Waiting for completion…")
            ok = pve.wait_for_task(node, upid, op.append_log)
            if not ok:
                raise RuntimeError("PBS backup task failed")
            op.append_log("Backup complete.")
            if run_restic_after and _cfg and _cfg.restic_repo:
                res = LocalResticClient(_cfg)
                op.append_log("Starting restic cloud backup…")
                def _prog_backup(pct, speed, eta):
                    self.publish_progress(op.op_id, str(vmid), pct, speed, eta)
                pbs_snaps = _fetch_pbs_snaps(_host())
                res.backup_datastore(_cfg.pbs_datastore_path, op.append_log, pbs_snaps,
                                     progress_fn=_prog_backup)
                op.append_log("Cloud backup complete.")

        _run_in_background(op, _do, rescan_restic=run_restic_after)

    def _handle_cmd_restore(self, body: dict) -> None:
        vmid        = body.get("vmid")
        vm_type     = _pbs_type(body.get("type", "vm"))  # normalize "lxc" → "ct" for PVE+PBS API
        backup_time = body.get("backup_time")
        source      = body.get("source", "local")        # 'local' | 'cloud'
        restic_id   = body.get("restic_id")
        corr_id     = body.get("corr_id", str(uuid.uuid4()))
        run_backup_after = body.get("run_backup_after", False)
        if not vmid or not backup_time or not _cfg:
            return
        node         = self._node()
        storage_id   = _cfg.pbs_storage_id
        pbs_ds       = _cfg.pbs_datastore
        bt_iso       = datetime.fromtimestamp(int(backup_time), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        op           = _new_op("restore", vmid=str(vmid))
        self._ack(corr_id, op.op_id)
        log.info("MQTT cmd/restore: %s/%s ts=%s source=%s corr_id=%s", vm_type, vmid, backup_time, source, corr_id)

        from_cloud = source == "cloud" and bool(restic_id and _cfg.restic_repo)
        step       = 0

        def _step(n, total, msg):
            op.append_log(f"Step {n}/{total} — {msg}")

        def _do(op: Operation):
            total = (3 if from_cloud else 2) + (1 if run_backup_after else 0)
            n = 1

            if from_cloud:
                _step(n, total, f"Restoring PBS datastore from restic snapshot {restic_id[:8]}…"); n += 1
                res = LocalResticClient(_cfg)
                res.restore_datastore(restic_id, op.append_log)
                # PBS is now running again; snapshot is back in local datastore

            pve = PVEClient(_host())
            _step(n, total, f"Stopping {vm_type}/{vmid}…"); n += 1
            pve.stop_vm(int(vmid), vm_type, node)
            _step(n, total, f"Restoring from PBS snapshot {bt_iso}…"); n += 1
            upid = pve.restore_vm(int(vmid), vm_type, bt_iso, storage_id, node, pbs_ds)
            ok   = pve.wait_for_task(node, upid, op.append_log)
            if not ok:
                raise RuntimeError("Restore task failed")
            op.append_log("Restore complete.")
            pve.start_vm(int(vmid), vm_type, node)
            op.append_log(f"Started {vm_type}/{vmid}.")

            if run_backup_after and _cfg and _cfg.restic_repo:
                _step(n, total, "Starting restic cloud backup after restore…"); n += 1
                res2 = LocalResticClient(_cfg)
                pbs_snaps = _fetch_pbs_snaps(_host())
                res2.backup_datastore(_cfg.pbs_datastore_path, op.append_log, pbs_snaps)
                op.append_log("Cloud backup complete.")

        _run_in_background(op, _do, rescan_restic=from_cloud or run_backup_after)

    def _handle_cmd_delete(self, body: dict) -> None:
        vmid        = body.get("vmid")
        vm_type     = body.get("type", "vm")
        backup_time = body.get("backup_time")
        restic_id   = body.get("restic_id")
        scope       = body.get("scope", "pbs")   # 'pbs' | 'both' | 'cloud'
        corr_id     = body.get("corr_id", str(uuid.uuid4()))
        if not vmid or backup_time is None or not _cfg:
            return
        op = _new_op("delete", vmid=str(vmid))
        self._ack(corr_id, op.op_id)
        log.info("MQTT cmd/delete: %s/%s ts=%s scope=%s", vm_type, vmid, backup_time, scope)

        do_restic = scope in ("both", "cloud") and bool(restic_id and _cfg.restic_repo)

        def _do(op: Operation):
            if scope in ("pbs", "both"):
                pbs = PBSClient(_host())
                op.append_log(f"Deleting PBS snapshot {vm_type}/{vmid} at {backup_time}…")
                deleted = pbs.delete_snapshot(_pbs_type(vm_type), str(vmid), int(backup_time))
                if deleted:
                    op.append_log("PBS snapshot deleted.")
                    op.append_log("Starting datastore GC…")
                    pbs.start_gc()
                    op.append_log("GC started (runs in background on PBS).")
                else:
                    op.append_log("PBS snapshot already gone (400/404 — check agent log for PBS response body).")
            if do_restic:
                op.append_log("Deleting cloud (restic) copy…")
                if not _restic_op_lock.acquire(blocking=False):
                    raise RuntimeError("Another restic operation is already running — try again later")
                try:
                    res = LocalResticClient(_cfg)
                    res.forget_snapshots([restic_id], op.append_log)
                    op.append_log("Cloud copy deleted.")
                finally:
                    _restic_op_lock.release()

        _run_in_background(op, _do, rescan_restic=do_restic)

    def _handle_cmd_delete_all(self, body: dict) -> None:
        vmid    = body.get("vmid")
        vm_type = body.get("type", "vm")
        corr_id = body.get("corr_id", str(uuid.uuid4()))
        if not vmid or not _cfg:
            return
        op = _new_op("delete", vmid=str(vmid))
        self._ack(corr_id, op.op_id)
        log.info("MQTT cmd/delete-all: %s/%s", vm_type, vmid)

        def _do(op: Operation):
            pbs  = PBSClient(_host())
            count = pbs.delete_all_snapshots_for_vm(_pbs_type(vm_type), str(vmid), op.append_log)
            op.append_log(f"Deleted {count} snapshot(s).")

        _run_in_background(op, _do)

    def _handle_cmd_backup_restic(self, body: dict) -> None:
        corr_id = body.get("corr_id", str(uuid.uuid4()))
        if not _cfg or not _cfg.restic_repo:
            return
        op = _new_op("backup", vmid=None)
        self._ack(corr_id, op.op_id)
        log.info("MQTT cmd/backup-restic corr_id=%s", corr_id)

        def _do(op: Operation):
            if not _restic_op_lock.acquire(blocking=False):
                raise RuntimeError("Another restic operation is already running — try again later")
            try:
                res = LocalResticClient(_cfg)
                pbs_snaps = _fetch_pbs_snaps(_host())
                def _prog_restic(pct, speed, eta):
                    self.publish_progress(op.op_id, None, pct, speed, eta)
                res.backup_datastore(_cfg.pbs_datastore_path, op.append_log, pbs_snaps,
                                     progress_fn=_prog_restic)
            finally:
                _restic_op_lock.release()

        _run_in_background(op, _do, rescan_restic=True)

    def _handle_cmd_delete_restic(self, body: dict) -> None:
        restic_id = body.get("restic_id")
        corr_id   = body.get("corr_id", str(uuid.uuid4()))
        if not restic_id or not _cfg or not _cfg.restic_repo:
            return
        op = _new_op("delete", vmid=None)
        self._ack(corr_id, op.op_id)
        log.info("MQTT cmd/delete-restic id=%s", restic_id[:8])

        def _do(op: Operation):
            if not _restic_op_lock.acquire(blocking=False):
                raise RuntimeError("Another restic operation is already running — try again later")
            try:
                res = LocalResticClient(_cfg)
                res.forget_snapshots([restic_id], op.append_log)
            finally:
                _restic_op_lock.release()

        _run_in_background(op, _do, rescan_restic=True)

    def _handle_cmd_restore_datastore(self, body: dict) -> None:
        restic_id = body.get("restic_id")
        corr_id   = body.get("corr_id", str(uuid.uuid4()))
        if not restic_id or not _cfg or not _cfg.restic_repo:
            return
        op = _new_op("restore", vmid=None)
        self._ack(corr_id, op.op_id)
        log.info("MQTT cmd/restore-datastore id=%s", restic_id[:8])

        def _do(op: Operation):
            res = LocalResticClient(_cfg)
            res.restore_datastore(restic_id, op.append_log)

        _run_in_background(op, _do)

    def _handle_cmd_replay_op_log(self, body: dict) -> None:
        op_id = body.get("op_id")
        if not op_id:
            return
        log.info("MQTT cmd/replay_op_log: op_id=%s", op_id)
        op = _get_op(op_id)
        if op is None:
            self._client.publish(f"{self._base}/ops/{op_id}/log", "__done__", qos=0)
            return
        # Replay lines
        for line in op.log:
            self.publish_log(op.op_id, line)
        if op.status in ("ok", "failed"):
            self._client.publish(f"{self._base}/ops/{op_id}/log", "__done__", qos=0)

    def _handle_cmd_replay_pbs_log(self, body: dict) -> None:
        upid = body.get("upid")
        if not upid:
            return
        log.info("MQTT cmd/replay_pbs_log: upid=%s", upid)
        lines = _get_pbs_task_log(upid)
        for line in lines:
            self._client.publish(f"{self._base}/pbs/tasks/{upid}/log", json.dumps(line), qos=0)
        self._client.publish(f"{self._base}/pbs/tasks/{upid}/log", json.dumps({"done": True}), qos=0)

    def _handle_cmd_replay_restic_log(self, body: dict) -> None:
        log.info("MQTT cmd/replay_restic_log")
        res = LocalResticClient(_cfg)
        lines = res.get_log_lines(tail=10000)
        for line in lines:
            self._client.publish(f"{self._base}/restic/log", line, qos=0)
        if not res.is_running():
            self._client.publish(f"{self._base}/restic/log", "__done__", qos=0)

    def _handle_cmd_replay_op_log(self, body: dict) -> None:
        op_id = body.get("op_id")
        if not op_id:
            return
        log.info("MQTT cmd/replay_op_log: op_id=%s", op_id)
        op = _get_op(op_id)
        if op is None:
            self._client.publish(f"{self._base}/ops/{op_id}/log", "__done__", qos=0)
            return
        # Replay lines
        for line in op.log:
            self.publish_log(op.op_id, line)
        if op.status in ("ok", "failed"):
            self._client.publish(f"{self._base}/ops/{op_id}/log", "__done__", qos=0)

    def _handle_cmd_replay_pbs_log(self, body: dict) -> None:
        upid = body.get("upid")
        if not upid:
            return
        log.info("MQTT cmd/replay_pbs_log: upid=%s", upid)
        lines = _get_pbs_task_log(upid)
        for line in lines:
            self._client.publish(f"{self._base}/pbs/tasks/{upid}/log", json.dumps(line), qos=0)
        self._client.publish(f"{self._base}/pbs/tasks/{upid}/log", json.dumps({"done": True}), qos=0)

    def _handle_cmd_replay_restic_log(self, body: dict) -> None:
        log.info("MQTT cmd/replay_restic_log")
        res = LocalResticClient(_cfg)
        lines = res.get_log_lines(tail=10000)
        for line in lines:
            self._client.publish(f"{self._base}/restic/log", line, qos=0)
        if not res.is_running():
            self._client.publish(f"{self._base}/restic/log", "__done__", qos=0)

    def setup_message_handler(self) -> None:
        self._client.on_message = self._on_message

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            log.warning("MQTT unexpected disconnect rc=%s — will auto-reconnect", rc)

    # ── public API ────────────────────────────────────────────────────────────

    def publish_op_started(self, op_id: str, vmid: str | None) -> None:
        if vmid:
            self._ensure_discovery(vmid)
            self._client.publish(f"{self._base}/vm/{vmid}/backup/status",
                                 "running", retain=True, qos=1)
        self._client.publish(f"{self._base}/ops/{op_id}/status",
                             "running", qos=1)

    def publish_op_done(self, op_id: str, vmid: str | None,
                        ok: bool, finished_at: float) -> None:
        status = "done" if ok else "failed"
        if vmid:
            self._client.publish(f"{self._base}/vm/{vmid}/backup/status",
                                 status, retain=True, qos=1)
            if ok:
                ts = datetime.fromtimestamp(finished_at, tz=timezone.utc).isoformat()
                self._client.publish(f"{self._base}/vm/{vmid}/backup/last_ok",
                                     ts, retain=True, qos=1)
        self._client.publish(f"{self._base}/ops/{op_id}/status", status, qos=1)

    def publish_progress(self, op_id: str, vmid: str | None,
                         pct: float, speed_mbps: float | None = None,
                         eta_s: int | None = None) -> None:
        payload = json.dumps({
            "pct": round(pct, 1),
            **({"speed_mbps": round(speed_mbps, 1)} if speed_mbps is not None else {}),
            **({"eta_s": eta_s} if eta_s is not None else {}),
        })
        self._client.publish(f"{self._base}/ops/{op_id}/progress", payload, qos=0)
        if vmid:
            self._client.publish(f"{self._base}/vm/{vmid}/backup/progress",
                                 payload, qos=0)

    def publish_log(self, op_id: str, line: str) -> None:
        self._client.publish(f"{self._base}/ops/{op_id}/log", line, qos=0)

    def publish_vm_idle(self, vmid: str) -> None:
        """Mark a VM's backup status as idle (e.g. after startup scan)."""
        self._ensure_discovery(vmid)
        self._client.publish(f"{self._base}/vm/{vmid}/backup/status",
                             "idle", retain=True, qos=1)

    def shutdown(self) -> None:
        try:
            self._client.publish(f"{self._base}/agent/status", "offline",
                                 retain=True, qos=1)
            time.sleep(0.2)  # let the publish flush
        finally:
            self._client.loop_stop()
            self._client.disconnect()

    # ── HA MQTT Discovery ─────────────────────────────────────────────────────

    def _ensure_discovery(self, vmid: str) -> None:
        """Publish HA Discovery payloads for a vmid once per session."""
        with self._lock:
            if vmid in self._discovered:
                return
            self._discovered.add(vmid)

        hn  = self._hostname
        dev = {"identifiers": [f"proxmox_{hn}"], "name": f"Proxmox {hn}",
               "manufacturer": "Proxmox"}

        def _pub(component: str, obj_id: str, cfg: dict) -> None:
            topic = f"homeassistant/{component}/proxmox_{hn}_{obj_id}/config"
            self._client.publish(topic, json.dumps(cfg), retain=True, qos=1)

        _pub("sensor", f"vm{vmid}_backup_status", {
            "name": f"VM {vmid} Backup Status",
            "state_topic": f"{self._base}/vm/{vmid}/backup/status",
            "unique_id": f"proxmox_{hn}_vm{vmid}_backup_status",
            "icon": "mdi:backup-restore",
            "device": dev,
        })
        _pub("sensor", f"vm{vmid}_backup_last_ok", {
            "name": f"VM {vmid} Last Backup",
            "state_topic": f"{self._base}/vm/{vmid}/backup/last_ok",
            "device_class": "timestamp",
            "unique_id": f"proxmox_{hn}_vm{vmid}_backup_last_ok",
            "device": dev,
        })
        _pub("sensor", f"vm{vmid}_backup_progress", {
            "name": f"VM {vmid} Backup Progress",
            "state_topic": f"{self._base}/vm/{vmid}/backup/progress",
            "value_template": "{{ value_json.pct }}",
            "unit_of_measurement": "%",
            "unique_id": f"proxmox_{hn}_vm{vmid}_backup_progress",
            "icon": "mdi:progress-upload",
            "device": dev,
        })
        _pub("sensor", f"vm{vmid}_age_hours", {
            "name": f"VM {vmid} Backup Age",
            "state_topic": f"{self._base}/vm/{vmid}/summary",
            "value_template": "{{ value_json.age_hours }}",
            "unit_of_measurement": "h",
            "unique_id": f"proxmox_{hn}_vm{vmid}_age_hours",
            "icon": "mdi:clock-outline",
            "device": dev,
        })
        _pub("sensor", f"vm{vmid}_snapshot_count", {
            "name": f"VM {vmid} Snapshot Count",
            "state_topic": f"{self._base}/vm/{vmid}/summary",
            "value_template": "{{ value_json.snapshot_count }}",
            "unique_id": f"proxmox_{hn}_vm{vmid}_snapshot_count",
            "icon": "mdi:database",
            "device": dev,
        })
        _pub("sensor", f"vm{vmid}_protection_status", {
            "name": f"VM {vmid} Protection Status",
            "state_topic": f"{self._base}/vm/{vmid}/summary",
            "value_template": "{{ value_json.status }}",
            "unique_id": f"proxmox_{hn}_vm{vmid}_protection_status",
            "icon": "mdi:shield-check",
            "device": dev,
        })

    def publish_agent_discovery(self) -> None:
        """Publish agent-level and host-level HA Discovery entities."""
        hn  = self._hostname
        dev = {"identifiers": [f"proxmox_{hn}"], "name": f"Proxmox {hn}",
               "manufacturer": "Proxmox"}

        def _pub(component: str, obj_id: str, cfg: dict) -> None:
            topic = f"homeassistant/{component}/proxmox_{hn}_{obj_id}/config"
            self._client.publish(topic, json.dumps(cfg), retain=True, qos=1)

        _pub("binary_sensor", "agent", {
            "name": f"Proxmox {hn} Agent",
            "state_topic": f"{self._base}/agent/status",
            "payload_on": "online",
            "payload_off": "offline",
            "unique_id": f"proxmox_{hn}_agent_status",
            "device_class": "connectivity",
            "device": dev,
        })

        # ── Storage sensors ───────────────────────────────────────────────────
        storage_topic = f"{self._base}/storage"
        for obj_id, name, tpl, unit, icon in [
            ("storage_local_used_gb",  "Storage Local Used",   "{{ value_json.local_used }}",       "GB", "mdi:harddisk"),
            ("storage_local_total_gb", "Storage Local Total",  "{{ value_json.local_total }}",      "GB", "mdi:harddisk"),
            ("storage_local_used_pct", "Storage Local Used %", "{{ value_json.local_used_pct }}",   "%",  "mdi:percent"),
            ("storage_cloud_used_gb",  "Storage Cloud Used",   "{{ value_json.cloud_used }}",       "GB", "mdi:harddisk"),
            ("storage_cloud_total_gb", "Storage Cloud Total",  "{{ value_json.cloud_total }}",      "GB", "mdi:harddisk"),
            ("storage_cloud_used_pct", "Storage Cloud Used %", "{{ value_json.cloud_used_pct }}",   "%",  "mdi:percent"),
        ]:
            _pub("sensor", obj_id, {
                "name": name,
                "state_topic": storage_topic,
                "value_template": tpl,
                "unit_of_measurement": unit,
                "unique_id": f"proxmox_{hn}_{obj_id}",
                "icon": icon,
                "device": dev,
            })

        # ── Summary sensors ───────────────────────────────────────────────────
        summary_topic = f"{self._base}/summary"
        _pub("binary_sensor", "all_protected", {
            "name": "All VMs Protected",
            "state_topic": summary_topic,
            "value_template": "{{ value_json.all_protected }}",
            "payload_on": "True",
            "payload_off": "False",
            "unique_id": f"proxmox_{hn}_all_protected",
            "icon": "mdi:shield-check",
            "device": dev,
        })
        _pub("sensor", "unprotected_count", {
            "name": "Unprotected VM Count",
            "state_topic": summary_topic,
            "value_template": "{{ value_json.unprotected_count }}",
            "unique_id": f"proxmox_{hn}_unprotected_count",
            "icon": "mdi:shield-alert",
            "device": dev,
        })


# ─────────────────────────────────────────────────────────────────────────────
# HA MQTT publisher — curated subset of topics to HA's Mosquitto broker
# ─────────────────────────────────────────────────────────────────────────────

class HAMQTTPublisher:
    """Publishes a curated subset of state + HA Discovery to HA's Mosquitto.

    Only these topics are ever sent to HA's broker:
      homeassistant/*/config  — discovery (retained, on connect)
      proxmox/<hn>/agent/status  — LWT online/offline
      proxmox/<hn>/summary        — all_protected, unprotected_count
      proxmox/<hn>/storage        — local/cloud usage

    All other topics (progress, per-VM raw, ops logs, …) stay on the GUI broker.
    """

    def __init__(self, host: str, port: int = 1883,
                 user: str = "", password: str = "",
                 hostname: str = "") -> None:
        import paho.mqtt.client as mqtt

        self._hostname = hostname or socket.gethostname()
        self._base     = f"proxmox/{self._hostname}"

        self._client = mqtt.Client(client_id=f"pve-agent-ha-{self._hostname}")
        self._client.will_set(f"{self._base}/agent/status", "offline",
                              retain=True, qos=1)
        if user:
            self._client.username_pw_set(user, password)

        self._client.on_connect    = self._on_connect
        self._client.on_disconnect = self._on_disconnect

        try:
            self._client.connect_async(host, port, keepalive=60)
            self._client.loop_start()
        except Exception as exc:
            log.warning("HA MQTT connect_async failed: %s", exc)

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("HA MQTT connected — publishing discovery + online status")
            client.publish(f"{self._base}/agent/status", "online",
                           retain=True, qos=1)
            self.publish_discovery()
        else:
            log.warning("HA MQTT connect failed rc=%s", rc)

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            log.warning("HA MQTT unexpected disconnect rc=%s — will auto-reconnect", rc)

    def publish(self, topic_suffix: str, data: dict) -> None:
        """Publish a state topic to HA's broker (always retained QoS 1)."""
        payload = json.dumps(data, separators=(",", ":"), sort_keys=True)
        self._client.publish(f"{self._base}/{topic_suffix}", payload,
                             retain=True, qos=1)

    def publish_discovery(self) -> None:
        hn  = self._hostname
        dev = {"identifiers": [f"proxmox_{hn}"], "name": f"Proxmox {hn}",
               "manufacturer": "Proxmox"}

        def _pub(component: str, obj_id: str, cfg: dict) -> None:
            topic = f"homeassistant/{component}/proxmox_{hn}_{obj_id}/config"
            self._client.publish(topic, json.dumps(cfg), retain=True, qos=1)

        _pub("binary_sensor", "agent", {
            "name": f"Proxmox {hn} Agent",
            "state_topic": f"{self._base}/agent/status",
            "payload_on": "online", "payload_off": "offline",
            "unique_id": f"proxmox_{hn}_agent_status",
            "device_class": "connectivity",
            "device": dev,
        })

        storage_topic = f"{self._base}/storage"
        for obj_id, name, tpl, unit, icon in [
            ("storage_local_used_gb",  "Storage Local Used",   "{{ value_json.local_used }}",     "GB", "mdi:harddisk"),
            ("storage_local_total_gb", "Storage Local Total",  "{{ value_json.local_total }}",    "GB", "mdi:harddisk"),
            ("storage_local_used_pct", "Storage Local Used %", "{{ value_json.local_used_pct }}", "%",  "mdi:percent"),
            ("storage_cloud_used_gb",   "Storage Cloud Used",       "{{ value_json.cloud_quota_used }}","GB", "mdi:harddisk"),
            ("storage_cloud_total_gb",  "Storage Cloud Total",      "{{ value_json.cloud_total }}",     "GB", "mdi:harddisk"),
            ("storage_cloud_used_pct",  "Storage Cloud Used %",     "{{ value_json.cloud_used_pct }}",  "%",  "mdi:percent"),
            ("storage_restic_used_gb",  "Storage Restic Repo",      "{{ value_json.cloud_used }}",      "GB", "mdi:backup-restore"),
        ]:
            _pub("sensor", obj_id, {
                "name": name,
                "state_topic": storage_topic,
                "value_template": tpl,
                "unit_of_measurement": unit,
                "unique_id": f"proxmox_{hn}_{obj_id}",
                "icon": icon,
                "device": dev,
            })

        summary_topic = f"{self._base}/summary"
        _pub("binary_sensor", "all_protected", {
            "name": "All VMs Protected",
            "state_topic": summary_topic,
            "value_template": "{{ value_json.all_protected }}",
            "payload_on": "True", "payload_off": "False",
            "unique_id": f"proxmox_{hn}_all_protected",
            "icon": "mdi:shield-check",
            "device": dev,
        })
        _pub("sensor", "unprotected_count", {
            "name": "Unprotected VM Count",
            "state_topic": summary_topic,
            "value_template": "{{ value_json.unprotected_count }}",
            "unique_id": f"proxmox_{hn}_unprotected_count",
            "icon": "mdi:shield-alert",
            "device": dev,
        })
        _pub("sensor", "protected_vm_count", {
            "name": "Protected VM Count",
            "state_topic": summary_topic,
            "value_template": "{{ value_json.protected_vm_count }}",
            "unique_id": f"proxmox_{hn}_protected_vm_count",
            "icon": "mdi:shield-check",
            "device": dev,
        })
        _pub("sensor", "vm_count", {
            "name": "Total VM Count",
            "state_topic": summary_topic,
            "value_template": "{{ value_json.vm_count }}",
            "unique_id": f"proxmox_{hn}_vm_count",
            "icon": "mdi:server",
            "device": dev,
        })
        _pub("sensor", "last_pbs_backup_age_h", {
            "name": "Last PBS Backup Age",
            "state_topic": summary_topic,
            "value_template": "{{ value_json.last_pbs_backup_age_h }}",
            "unit_of_measurement": "h",
            "unique_id": f"proxmox_{hn}_last_pbs_backup_age_h",
            "icon": "mdi:clock-outline",
            "device": dev,
        })
        _pub("sensor", "last_pbs_backup_ts", {
            "name": "Last PBS Backup",
            "state_topic": summary_topic,
            "value_template": "{{ value_json.last_pbs_backup_iso }}",
            "unique_id": f"proxmox_{hn}_last_pbs_backup_ts",
            "device_class": "timestamp",
            "device": dev,
        })
        _pub("sensor", "restic_snapshot_count", {
            "name": "Restic Snapshot Count",
            "state_topic": summary_topic,
            "value_template": "{{ value_json.restic_snapshot_count }}",
            "unique_id": f"proxmox_{hn}_restic_snapshot_count",
            "icon": "mdi:backup-restore",
            "device": dev,
        })
        _pub("sensor", "last_restic_backup_age_h", {
            "name": "Last Restic Backup Age",
            "state_topic": summary_topic,
            "value_template": "{{ value_json.last_restic_backup_age_h }}",
            "unit_of_measurement": "h",
            "unique_id": f"proxmox_{hn}_last_restic_backup_age_h",
            "icon": "mdi:clock-outline",
            "device": dev,
        })
        _pub("sensor", "last_restic_backup_ts", {
            "name": "Last Restic Backup",
            "state_topic": summary_topic,
            "value_template": "{{ value_json.last_restic_backup_iso }}",
            "unique_id": f"proxmox_{hn}_last_restic_backup_ts",
            "device_class": "timestamp",
            "device": dev,
        })

    def shutdown(self) -> None:
        try:
            self._client.publish(f"{self._base}/agent/status", "offline",
                                 retain=True, qos=1)
            time.sleep(0.2)
        finally:
            self._client.loop_stop()
            self._client.disconnect()


# Module-level publishers — None when not configured
_mqtt: MQTTPublisher | None = None
_ha_mqtt: HAMQTTPublisher | None = None


# ─────────────────────────────────────────────────────────────────────────────
# State poller — maintains current VM/PBS/restic state, publishes diffs
# ─────────────────────────────────────────────────────────────────────────────


def _compute_host_summary(
    all_vmids: set,
    pbs_groups: dict,
    restic_snaps: list,
    restic_by_vm_pbstime: dict,
    vm_selection: dict,
    exclude_from_protection: list,
    now: float,
) -> dict:
    """Pure function: compute host-level summary fields for MQTT publication."""
    vm_sel_mode  = vm_selection.get("mode", "exclude")
    vm_sel_vmids = set(str(v) for v in vm_selection.get("vmids", []))
    if vm_sel_mode == "include":
        excluded = all_vmids - vm_sel_vmids
    else:
        excluded = vm_sel_vmids
    excluded |= set(str(v) for v in (exclude_from_protection or []))

    unprotected = [
        v for v in all_vmids
        if v not in excluded and not (pbs_groups.get(v) and restic_by_vm_pbstime.get(v))
    ]
    protected_count = len(all_vmids) - len(unprotected) - len(excluded & all_vmids)

    all_pbs_times = [
        s.get("backup_time", 0)
        for snaps in pbs_groups.values()
        for s in snaps
        if s.get("backup_time")
    ]
    last_pbs_ts  = max(all_pbs_times) if all_pbs_times else None
    last_pbs_age = round(max(0.0, now - last_pbs_ts) / 3600, 1) if last_pbs_ts else None

    restic_ts_list = [s.get("ts", 0) for s in restic_snaps if s.get("ts")]
    last_restic_ts  = max(restic_ts_list) if restic_ts_list else None
    last_restic_age = round(max(0.0, now - last_restic_ts) / 3600, 1) if last_restic_ts else None

    def _iso(ts: float | None) -> str | None:
        if ts is None:
            return None
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    return {
        "all_protected":            len(unprotected) == 0,
        "unprotected_count":        len(unprotected),
        "vm_count":                 len(all_vmids),
        "protected_vm_count":       max(0, protected_count),
        "last_pbs_backup_iso":      _iso(last_pbs_ts),
        "last_pbs_backup_age_h":    last_pbs_age,
        "restic_snapshot_count":    len(restic_snaps),
        "last_restic_backup_iso":   _iso(last_restic_ts),
        "last_restic_backup_age_h": last_restic_age,
    }


class StatePoller:
    """Background poller that keeps MQTT broker state up to date.

    Topics published (all retained):
      proxmox/<host>/vm/<id>/meta    → {vmid, name, type, status, os, ...}
      proxmox/<host>/vm/<id>/pbs     → {snapshots: [...]}
      proxmox/<host>/vm/<id>/restic  → {snapshots: [...]}
      proxmox/<host>/vms/index       → ["100", "101", ...]
      proxmox/<host>/state/ready     → {"ts": ..., "pbs_ok": bool}
      proxmox/<host>/storage         → {local_used, local_total, dedup_factor, ...}
      proxmox/<host>/info            → {pbs: "3.x", restic: "0.16"}
      proxmox/<host>/schedules       → {pbs_jobs, pbs_running, restic_next, ...}
    """

    PVE_PBS_INTERVAL      = 60    # seconds — VM list, snapshots, local storage
    RESTIC_INTERVAL       = 300   # seconds — cloud snapshot listing (expensive)
    CLOUD_STORAGE_INTERVAL = 60   # seconds — cloud quota/usage (just 2 rclone calls)
    INFO_INTERVAL         = 3600  # seconds — version strings (rarely change)
    SCHEDULES_INTERVAL    = 120   # seconds — next scheduled backup
    PBS_TASKS_INTERVAL    = 15    # seconds — running PBS task poll

    PBS_TASK_TYPES = {"backup", "prune", "prunejob", "garbage_collection", "verify"}

    def __init__(self, cfg: "AgentConfig", mqtt: MQTTPublisher,
                 ha_mqtt: "HAMQTTPublisher | None" = None) -> None:
        self._cfg     = cfg
        self._mqtt    = mqtt
        self._ha_mqtt = ha_mqtt
        self._base    = mqtt._base
        self._stop  = threading.Event()
        self._hashes: dict[str, str] = {}  # topic_suffix → last published payload
        self._hash_lock = threading.Lock()
        # Keep latest restic state so PBS poll can cross-reference
        self._restic_snaps: list[dict] = []
        self._restic_lock  = threading.Lock()
        # Track VMIDs seen in previous scan so we can clear retained topics for
        # VMIDs that have disappeared (e.g. restic-only VM whose snapshots were deleted).
        # Lazily seeded from broker's retained vms/index on first scan so stale
        # VMIDs are caught even across agent restarts.
        self._known_vmids: set[str] = set()
        self._known_vmids_lock = threading.Lock()
        # Last known local PBS storage stats — merged with cloud stats in storage topic
        self._local_storage: dict = {}
        self._storage_lock  = threading.Lock()
        # Persistent memory of partial PBS backup times per vmid.
        # Accumulated (never cleared) so that after a partial is pruned locally it
        # can still be flagged as partial in cloud-only entries.
        self._partial_pbs_times: dict[str, set[int]] = {}
        self._partial_lock = threading.Lock()

    def start(self) -> None:
        # Reset running-flags on startup so stale retained "restic_running/pbs_running: true"
        # can never persist across agent restarts.
        self._pub_if_changed("schedules", {
            "pbs_jobs": [], "pbs_running": False,
            "restic_next": None, "restic_running": False,
            "pbs_retention": [], "restic_retention": {},
        })

        # Pre-populate cloud stats so _scan_storage() has real values from the first run.
        # This is just 2 rclone calls — fast enough to do synchronously before threads start.
        if self._cfg and self._cfg.restic_repo:
            try:
                stats = LocalResticClient(self._cfg).get_stats()
                self._update_cloud_storage(
                    stats.get("cloud_used", 0),
                    stats.get("cloud_total"),
                    stats.get("cloud_quota_used"),
                )
                log.info("Cloud storage pre-scan done: used=%.2fGB", stats.get("cloud_used", 0))
            except Exception as exc:
                log.warning("Cloud storage pre-scan failed: %s", exc)
        threading.Thread(target=self._loop_pve_pbs,       daemon=True, name="poller-pve-pbs").start()
        threading.Thread(target=self._loop_restic,         daemon=True, name="poller-restic").start()
        threading.Thread(target=self._loop_cloud_storage,  daemon=True, name="poller-cloud-storage").start()
        threading.Thread(target=self._loop_info,           daemon=True, name="poller-info").start()
        threading.Thread(target=self._loop_schedules,      daemon=True, name="poller-schedules").start()
        threading.Thread(target=self._loop_pbs_tasks,      daemon=True, name="poller-pbs-tasks").start()
        log.info("StatePoller started (pve/pbs %ds, restic %ds, cloud-storage %ds, info %ds, schedules %ds, pbs-tasks %ds)",
                 self.PVE_PBS_INTERVAL, self.RESTIC_INTERVAL, self.CLOUD_STORAGE_INTERVAL,
                 self.INFO_INTERVAL, self.SCHEDULES_INTERVAL, self.PBS_TASKS_INTERVAL)

    def stop(self) -> None:
        self._stop.set()

    def rescan_now(self) -> None:
        """Trigger an immediate PVE+PBS+storage rescan."""
        threading.Thread(target=self._scan_pve_pbs, daemon=True, name="rescan-now").start()

    def invalidate_vm_cache(self, vmid: str | None) -> None:
        """Clear cached hashes for a VM so the next rescan always republishes.

        Call this before rescan_now() after a delete/restore so _pub_if_changed
        does not suppress publication even if the payload briefly appears unchanged
        (e.g. PBS hasn't processed the deletion yet when the rescan races ahead).
        """
        if not vmid:
            return
        with self._hash_lock:
            for suffix in (f"vm/{vmid}/pbs", f"vm/{vmid}/meta", f"vm/{vmid}/restic"):
                self._hashes.pop(suffix, None)
        log.debug("invalidated MQTT cache for vm/%s", vmid)

    def rescan_storage_now(self) -> None:
        """Trigger immediate storage-only rescan (e.g. after delete)."""
        threading.Thread(target=self._scan_storage, daemon=True, name="rescan-storage").start()

    # ── background loops ──────────────────────────────────────────────────────

    def _loop_pve_pbs(self) -> None:
        while not self._stop.is_set():
            try:
                self._scan_pve_pbs()
            except Exception as exc:
                log.warning("PVE/PBS poll error: %s", exc)
            self._stop.wait(self.PVE_PBS_INTERVAL)

    def _loop_restic(self) -> None:
        while not self._stop.is_set():
            try:
                self._scan_restic()
            except Exception as exc:
                log.warning("Restic poll error: %s", exc)
            self._stop.wait(self.RESTIC_INTERVAL)

    def _loop_cloud_storage(self) -> None:
        cfg = self._cfg
        if not cfg or not cfg.restic_repo:
            return   # no cloud configured — skip entirely
        while not self._stop.is_set():
            try:
                stats = LocalResticClient(cfg).get_stats()
                self._update_cloud_storage(
                    stats.get("cloud_used", 0),
                    stats.get("cloud_total"),
                    stats.get("cloud_quota_used"),
                )
            except Exception as exc:
                log.warning("Cloud storage poll error: %s", exc)
            self._stop.wait(self.CLOUD_STORAGE_INTERVAL)

    def _loop_info(self) -> None:
        while not self._stop.is_set():
            try:
                self._scan_info()
            except Exception as exc:
                log.warning("Info poll error: %s", exc)
            self._stop.wait(self.INFO_INTERVAL)

    def _loop_schedules(self) -> None:
        while not self._stop.is_set():
            try:
                self._scan_schedules()
            except Exception as exc:
                log.warning("Schedules poll error: %s", exc)
            self._stop.wait(self.SCHEDULES_INTERVAL)

    def _loop_pbs_tasks(self) -> None:
        while not self._stop.is_set():
            try:
                self._scan_pbs_tasks()
            except Exception as exc:
                log.warning("PBS tasks poll error: %s", exc)
            self._stop.wait(self.PBS_TASKS_INTERVAL)

    # ── scan implementations ──────────────────────────────────────────────────

    def _scan_pve_pbs(self) -> None:
        cfg = self._cfg

        # ── PVE: get all VMs/LXCs ────────────────────────────────────────────
        pve_meta: dict = {}
        pve_ok = True
        vm_selection: dict = {"mode": "exclude", "vmids": []}
        try:
            pve = PVEClient(_host())
            pve_meta = pve.get_vms_and_lxcs()
            pbs_jobs = pve.get_backup_schedules()
            if pbs_jobs:
                vm_selection = pve.get_backup_vm_selection(pbs_jobs[0]["id"])
        except Exception as exc:
            log.warning("PVE fetch failed: %s", exc)
            pve_ok = False

        # ── PBS: get all snapshot groups ─────────────────────────────────────
        pbs_groups: dict[str, list] = {}  # vmid_str → [snap, ...]
        pbs_ok = True
        try:
            pbs = PBSClient(_host())
            for group in pbs.get_snapshots():
                vid = str(group.get("pve_id", ""))
                if vid:
                    pbs_groups[vid] = group.get("snapshots", [])
        except Exception as exc:
            log.warning("PBS fetch failed: %s", exc)
            pbs_ok = False

        # ── cross-reference restic coverage ──────────────────────────────────
        with self._restic_lock:
            restic_snaps = list(self._restic_snaps)
        restic_by_vm_pbstime = _build_restic_index(restic_snaps)

        # ── build per-VM state and publish diffs ──────────────────────────────
        # Include VMIDs from restic so cloud-only VMs (deleted from PVE+PBS but
        # still in restic) remain visible in the UI.
        all_vmids = (set(str(k) for k in pve_meta) | set(pbs_groups) | set(restic_by_vm_pbstime))

        for vmid in all_vmids:
            vid_int = int(vmid) if vmid.isdigit() else vmid
            meta = pve_meta.get(vid_int, pve_meta.get(vmid, {}))

            # meta topic
            self._pub_if_changed(f"vm/{vmid}/meta", {
                "vmid":     vmid,
                "name":     meta.get("name", f"vm-{vmid}"),
                "type":     meta.get("type", "vm"),
                "status":   meta.get("status", "unknown"),
                "os":       meta.get("os", "linux"),
                "template": meta.get("template", False),
                "in_pve":   vid_int in pve_meta or vmid in pve_meta,
            })

            # annotate PBS snapshots with cloud coverage
            raw_snaps = pbs_groups.get(vmid, [])
            vm_restic  = restic_by_vm_pbstime.get(vmid, {})
            annotated  = []
            local_times: set[int] = set()
            partial_times: set[int] = set()
            for snap in raw_snaps:
                bt = snap.get("backup_time")
                if bt is not None:
                    local_times.add(bt)
                    if snap.get("partial"):
                        partial_times.add(bt)
                rs = vm_restic.get(bt)
                annotated.append({
                    **snap,
                    "local":           True,
                    "cloud":           rs is not None,
                    "restic_id":       rs["id"]       if rs else None,
                    "restic_short_id": rs["short_id"] if rs else None,
                })

            # Maintain persistent partial-times memory so that after a partial is
            # pruned from local PBS it can still be flagged in cloud-only entries.
            # Also remove times that completed successfully (partial=False) so a
            # backup that was in-progress during a scan is not permanently flagged.
            completed_times = {
                snap.get("backup_time") for snap in raw_snaps
                if snap.get("backup_time") is not None and not snap.get("partial")
            }
            with self._partial_lock:
                known = self._partial_pbs_times.setdefault(vmid, set())
                known.update(partial_times)
                known -= completed_times
                all_partial_times = set(known)

            # Add cloud-only entries: restic covers a PBS time that no longer
            # exists locally (was pruned). These show as cloud-only in the UI.
            seen_cloud: set[int] = set()
            for bt, rs in sorted(vm_restic.items(), reverse=True):
                if bt is None or bt in local_times or bt in seen_cloud:
                    continue
                seen_cloud.add(bt)
                annotated.append({
                    "backup_time":     bt,
                    "date":            datetime.fromtimestamp(bt, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                    "local":           False,
                    "cloud":           True,
                    "incremental":     True,
                    "partial":         bt in all_partial_times,
                    "size":            "—",
                    "size_bytes":      0,
                    "restic_id":       rs["id"],
                    "restic_short_id": rs["short_id"],
                })

            annotated.sort(key=lambda s: s.get("backup_time", 0), reverse=True)

            self._pub_if_changed(f"vm/{vmid}/pbs", {"snapshots": annotated})

            # Publish restic snapshots for this VM with covers annotated by local presence.
            pbs_times = {s.get("backup_time") for s in raw_snaps}
            vmid_int_cmp = int(vmid) if vmid.isdigit() else vmid
            vm_restic_snaps = [
                {
                    **s,
                    "covers": [
                        {
                            **c,
                            "local":    c.get("pbs_time") in pbs_times,
                            "partial":  c.get("pbs_time") in partial_times,
                            "pbs_date": datetime.fromtimestamp(c["pbs_time"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                                        if c.get("pbs_time") else None,
                        }
                        for c in s.get("covers", [])
                        if c.get("vmid") == vmid_int_cmp
                    ],
                }
                for s in restic_snaps
                if any(
                    c.get("vmid") == (int(vmid) if vmid.isdigit() else vmid)
                    for c in s.get("covers", [])
                )
            ]
            if vm_restic_snaps:
                self._pub_if_changed(f"vm/{vmid}/restic", {"snapshots": vm_restic_snaps})

            # HA Discovery + per-VM backup status (idle unless job running)
            self._mqtt._ensure_discovery(vmid)

            # Per-VM summary for HA MQTT Discovery sensors
            has_local = bool(local_times)
            has_cloud = bool(vm_restic)
            newest_bt = annotated[0].get("backup_time") if annotated else None
            age_hours = round((time.time() - newest_bt) / 3600, 1) if newest_bt else None
            if has_local and has_cloud:
                vm_status = "ok"
            elif has_local:
                vm_status = "local_only"
            elif has_cloud:
                vm_status = "cloud_only"
            else:
                vm_status = "none"
            self._pub_if_changed(f"vm/{vmid}/summary", {
                "snapshot_count": len(annotated),
                "age_hours":      age_hours,
                "has_local":      has_local,
                "has_cloud":      has_cloud,
                "status":         vm_status,
            })

        # Clear retained topics for VMIDs that have disappeared since the last scan
        # (e.g. a restic-only VM whose snapshots were all deleted).  Publishing an
        # empty payload with retain=True removes the retained message from the broker.
        # On first scan, seed from the retained vms/index the broker sent on connect
        # so stale VMIDs are detected even after an agent restart.
        with self._known_vmids_lock:
            # Union of in-memory history and broker-bootstrapped list so stale
            # VMIDs are caught both within a lifecycle and across restarts.
            prev = (self._known_vmids or set()) | (self._mqtt._bootstrap_vmids or set())
            gone = prev - all_vmids
            self._known_vmids = set(all_vmids)
        for gvmid in gone:
            for suffix in (
                f"vm/{gvmid}/meta",
                f"vm/{gvmid}/pbs",
                f"vm/{gvmid}/restic",
                f"vm/{gvmid}/backup/status",
                f"vm/{gvmid}/backup/last_ok",
                f"vm/{gvmid}/backup/progress",
                f"vm/{gvmid}/summary",
            ):
                self._mqtt._client.publish(f"{self._base}/{suffix}", b"", retain=True, qos=1)
                self._hashes.pop(suffix, None)
            log.info("Cleared retained MQTT topics for disappeared VMID %s", gvmid)

        # publish index (sorted list of known vmids)
        self._pub_if_changed("vms/index", sorted(all_vmids))
        # publish overall state
        self._pub_if_changed("state/ready", {
            "ts": time.time(), "pve_ok": pve_ok, "pbs_ok": pbs_ok,
        })

        # Host-level summary for HA MQTT Discovery
        now = time.time()
        self._pub_if_changed("summary", _compute_host_summary(
            all_vmids,
            pbs_groups,
            restic_snaps,
            restic_by_vm_pbstime,
            vm_selection,
            self._cfg.exclude_from_protection,
            now,
        ))

        # Storage stats change whenever snapshots change — scan in same cycle
        self._scan_storage()

    def _scan_storage(self) -> None:
        """Fetch local PBS storage stats and publish.
        Cloud stats are updated separately by _scan_restic() since querying
        restic/rclone is slow and cloud usage changes independently of our backups.
        """
        try:
            pbs   = PBSClient(_host())
            local = pbs.get_storage_info()  # {local_used, local_total, dedup_factor}
            with self._storage_lock:
                self._local_storage.update(local)   # preserve existing cloud keys
                data = dict(self._local_storage)
                data.setdefault("cloud_used", 0)
                data.setdefault("cloud_total", None)
                data.setdefault("cloud_quota_used", None)
                # computed pct fields for MQTT Discovery sensors
                lt = data.get("local_total") or 0
                data["local_used_pct"] = round(data.get("local_used", 0) / lt * 100) if lt else None
                ct = data.get("cloud_total") or 0
                cq = data.get("cloud_quota_used") or 0
                data["cloud_used_pct"] = round(cq / ct * 100) if ct else None
            self._pub_if_changed("storage", data)
        except Exception as exc:
            log.warning("Storage scan failed: %s", exc)

    def _update_cloud_storage(self, cloud_used: float,
                              cloud_total: float | None,
                              cloud_quota_used: float | None) -> None:
        """Merge cloud stats into the storage topic (called from _scan_restic)."""
        with self._storage_lock:
            self._local_storage["cloud_used"]       = cloud_used
            self._local_storage["cloud_total"]      = cloud_total
            self._local_storage["cloud_quota_used"] = cloud_quota_used
            data = dict(self._local_storage)
            lt = data.get("local_total") or 0
            data["local_used_pct"] = round(data.get("local_used", 0) / lt * 100) if lt else None
            ct = cloud_total or 0
            cq = cloud_quota_used or 0
            data["cloud_used_pct"] = round(cq / ct * 100) if ct else None
        self._pub_if_changed("storage", data)

    def _scan_info(self) -> None:
        """Fetch version strings and publish retained. Rarely changes."""
        try:
            pbs  = PBSClient(_host())
            info = pbs.get_versions()                # {"pbs": "x.y.z"}
            cfg  = self._cfg
            if cfg and cfg.restic_repo:
                info["restic"] = LocalResticClient(cfg).get_version()
            else:
                info["restic"] = None
            self._pub_if_changed("info", info)
        except Exception as exc:
            log.warning("Info scan failed: %s", exc)

    def _scan_schedules(self) -> None:
        """Fetch PVE backup schedules + restic schedule and publish retained."""
        result: dict = {
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
        except Exception as exc:
            log.warning("Schedule PVE fetch failed: %s", exc)
        cfg = self._cfg
        if cfg and cfg.restic_repo:
            try:
                res = LocalResticClient(cfg)
                result["restic_next"]      = res.get_next_run()
                result["restic_running"]   = res.is_running()
                result["restic_retention"] = res.get_retention()
                prune_jobs                 = res.get_pbs_prune_jobs()
                result["pbs_retention"]    = [
                    j for j in prune_jobs if j.get("store") == (cfg.pbs_datastore or "")
                ]
            except Exception as exc:
                log.warning("Schedule restic fetch failed: %s", exc)
        self._pub_if_changed("schedules", result)

    def _scan_pbs_tasks(self) -> None:
        """Poll PBS for running tasks and publish via MQTT."""
        try:
            tasks = _get_pbs_tasks(running_only=True)
        except Exception as exc:
            log.warning("PBS task scan failed: %s", exc)
            return
        self._pub_if_changed("pbs/tasks/running", tasks)

    def _scan_restic(self) -> None:
        cfg = self._cfg
        if not cfg or not cfg.restic_repo:
            return
        try:
            res   = LocalResticClient(cfg)
            snaps = res.get_snapshots_flat()
        except Exception as exc:
            log.warning("Restic scan failed: %s", exc)
            return

        with self._restic_lock:
            self._restic_snaps = snaps

        # Trigger a PBS re-scan — _scan_pve_pbs publishes vm/<id>/restic with
        # proper local-annotation (whether each PBS snapshot still exists in PBS).
        self.rescan_now()

    # ── helpers ───────────────────────────────────────────────────────────────

    # Topics mirrored to HA's Mosquitto broker (all others stay on GUI broker)
    _HA_MIRROR_TOPICS = frozenset({"summary", "storage"})

    def _pub_if_changed(self, topic_suffix: str,
                        data: dict | list) -> None:
        payload = json.dumps(data, separators=(",", ":"), sort_keys=True)
        with self._hash_lock:
            if self._hashes.get(topic_suffix) == payload:
                return
            self._hashes[topic_suffix] = payload
        full = f"{self._base}/{topic_suffix}"
        self._mqtt._client.publish(full, payload, retain=True, qos=1)
        log.debug("MQTT publish %s (%d B)", full, len(payload))
        if self._ha_mqtt and topic_suffix in self._HA_MIRROR_TOPICS:
            self._ha_mqtt.publish(topic_suffix, data)


def _build_restic_index(snaps: list[dict]) -> dict[str, dict[int, dict]]:
    """Return {vmid_str: {pbs_time: newest_restic_snap}} from flat restic list."""
    idx: dict[str, dict[int, dict]] = {}
    for s in snaps:           # list is newest-first
        for cov in s.get("covers", []):
            vid = str(cov.get("vmid", ""))
            pt  = cov.get("pbs_time")
            if vid and pt is not None:
                idx.setdefault(vid, {}).setdefault(pt, s)
    return idx


# Module-level poller — None when MQTT is not configured
_poller: StatePoller | None = None





# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

_SENSITIVE = {"pve_password", "pbs_password", "restic_password", "agent_token", "mqtt_password", "mqtt_ha_password"}

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
    pve_ssh_host: str = ""
    restic_repo: str = ""
    restic_password: str = ""
    verify_ssl: bool = False
    restic_env: dict = field(default_factory=dict)
    agent_token: str = ""        # if set, all requests must present "Authorization: Bearer <token>"
    # MQTT — GUI broker (all optional — omit mqtt_host to disable)
    mqtt_host: str = ""
    mqtt_port: int = 1883
    mqtt_user: str = ""
    mqtt_password: str = ""
    mqtt_hostname: str = ""      # topic prefix override (defaults to socket.gethostname())
    # MQTT — HA broker (optional — set mqtt_ha_host to enable HA MQTT Discovery)
    mqtt_ha_host: str = ""
    mqtt_ha_port: int = 1883
    mqtt_ha_user: str = ""
    mqtt_ha_password: str = ""
    exclude_from_protection: list = field(default_factory=list)  # VMIDs that don't need backups
    pve_node: str = ""           # PVE node name for API calls (defaults to mqtt_hostname then socket.gethostname())
    agent_port: int = 8099       # HTTP API listen port (used by setup-agent.sh)

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
# Path to the config file on disk — set at startup; used by /connection POST to write back
_config_path: str = "/etc/pve-agent/config.json"


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

    _PBS_PRUNE_KEYS = (
        "keep-last", "keep-daily", "keep-weekly", "keep-monthly", "keep-yearly",
    )

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

    RESTIC_LOG_FILE = "/var/log/restic-backup.log"

    def is_running(self) -> bool:
        """Return True if a restic *backup* is currently running (not just listing)."""
        try:
            result = subprocess.run(
                ["pgrep", "-f", "restic backup"],
                capture_output=True, timeout=5,
            )
            return result.returncode == 0
        except Exception:
            return False

    def get_log_lines(self, tail: int = 500) -> list[str]:
        """Return the last N lines from the restic log file."""
        try:
            with open(self.RESTIC_LOG_FILE) as f:
                lines = f.readlines()
            return [l.rstrip("\n") for l in lines[-tail:]]
        except FileNotFoundError:
            return []
        except Exception:
            return []

    def get_version(self) -> str:
        """Return installed restic version string, e.g. '0.16.2'."""
        try:
            out = subprocess.run(
                ["restic", "version"], capture_output=True, text=True, timeout=10,
            ).stdout
            m = re.search(r"restic\s+([\d.]+)", out)
            return m.group(1) if m else "unknown"
        except Exception:
            return "unknown"

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
        # Seed with 0 for all keys so GUI shows empty fields for unset vars.
        result: dict = {label: 0 for label in mapping.values()}
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k in mapping and v:
                result[mapping[k]] = int(v) if v.isdigit() else v
        return result

    def set_retention(self, retention: dict) -> None:
        """Write RESTIC_RETENTION_KEEP_* values back to config.env.

        Only updates keys present in retention; leaves all other lines intact.
        """
        config_path = "/etc/proxmox-backup-restore/config.env"
        mapping = {
            "keep-last":    "RESTIC_RETENTION_KEEP_LAST",
            "keep-daily":   "RESTIC_RETENTION_KEEP_DAILY",
            "keep-weekly":  "RESTIC_RETENTION_KEEP_WEEKLY",
            "keep-monthly": "RESTIC_RETENTION_KEEP_MONTHLY",
            "keep-yearly":  "RESTIC_RETENTION_KEEP_YEARLY",
        }
        try:
            with open(config_path) as f:
                lines = f.readlines()
        except OSError:
            lines = []

        # Value of 0 or "" means "remove this key" — backup script skips unset vars.
        env_set    = {mapping[k]: str(v) for k, v in retention.items() if k in mapping and str(v) not in ("", "0")}
        env_remove = {mapping[k] for k, v in retention.items() if k in mapping and str(v) in ("", "0")}
        written = set()
        new_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                key = stripped.split("=", 1)[0].strip()
                if key in env_remove:
                    written.add(key)
                    continue  # drop the line
                if key in env_set:
                    new_lines.append(f'{key}={env_set[key]}\n')
                    written.add(key)
                    continue
            new_lines.append(line)
        # Append any keys not already present (only for non-zero values)
        for key, val in env_set.items():
            if key not in written:
                new_lines.append(f'{key}={val}\n')

        import os as _os
        _os.makedirs(_os.path.dirname(config_path), exist_ok=True)
        with open(config_path, "w") as f:
            f.writelines(new_lines)

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

    _RESTIC_TIMER = "/etc/systemd/system/restic-backup.timer"

    def get_restic_schedule(self) -> str | None:
        """Return the OnCalendar value from the restic systemd timer, or None."""
        try:
            with open(self._RESTIC_TIMER) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("OnCalendar="):
                        return line.split("=", 1)[1].strip()
        except OSError:
            pass
        return None

    def set_restic_schedule(self, on_calendar: str) -> None:
        """Update OnCalendar in the restic systemd timer and reload."""
        with open(self._RESTIC_TIMER) as f:
            lines = f.readlines()
        with open(self._RESTIC_TIMER, "w") as f:
            for line in lines:
                if line.startswith("OnCalendar="):
                    f.write(f"OnCalendar={on_calendar}\n")
                else:
                    f.write(line)
        subprocess.run(["systemctl", "daemon-reload"], check=True, timeout=15)
        try:
            subprocess.run(["systemctl", "restart", "restic-backup.timer"],
                           check=True, timeout=15)
        except subprocess.CalledProcessError:
            pass  # timer may not restart in CI/nested PVE; file is updated

    def set_pbs_prune_job(self, job_id: str, retention: dict) -> None:
        """Update a PBS prune job's retention policy via proxmox-backup-manager."""
        parts = ["proxmox-backup-manager", "prune-job", "update", job_id]
        to_delete = []
        for key in self._PBS_PRUNE_KEYS:
            val = retention.get(key)
            if val:
                parts += [f"--{key}", str(int(val))]
            else:
                to_delete.append(key)
        for key in to_delete:
            parts += ["--delete", key]
        subprocess.run(parts, check=True, timeout=15)

    def get_stats(self) -> dict:
        """Return cloud storage usage via rclone (same repo as restic uses).
        Runs directly on the PVE host — no SSH needed.
        Returns {cloud_used, cloud_total?, cloud_quota_used?} in GB.
        """
        result: dict = {"cloud_used": 0, "cloud_total": None, "cloud_quota_used": None}
        repo = self._env.get("RESTIC_REPOSITORY", "")
        if not repo or ":" not in repo:
            return result
        # rclone remote is everything up to the first path separator after the remote name
        # e.g. "rclone:gdrive:/backups" → remote path = "gdrive:/backups"
        repo_path = ":".join(repo.split(":")[1:])
        # Extract just the remote name (e.g. "gdrive") for quota check
        remote_name = repo_path.split(":")[0] if ":" in repo_path else repo_path.split("/")[0]
        try:
            out = subprocess.run(
                ["rclone", "size", repo_path, "--json"],
                capture_output=True, text=True, timeout=60, env=self._full_env,
            ).stdout
            size_data = json.loads(out)
            result["cloud_used"] = round(size_data.get("bytes", 0) / 1024**3, 1)
        except Exception:
            pass
        try:
            out = subprocess.run(
                ["rclone", "about", f"{remote_name}:", "--json"],
                capture_output=True, text=True, timeout=20, env=self._full_env,
            ).stdout
            about = json.loads(out)
            total = about.get("total", 0)
            used  = about.get("used",  0)
            other = about.get("other", 0)
            if total:
                result["cloud_total"]      = round(total / 1024**3, 1)
                result["cloud_quota_used"] = round((used + other) / 1024**3, 1)
        except Exception:
            pass
        return result

    def forget_snapshots(self, snapshot_ids: list[str], log_fn) -> None:
        """Forget specific restic snapshots and prune unreferenced data."""
        if not snapshot_ids:
            log_fn("No restic snapshots to forget.")
            return
        log_fn(f"Forgetting restic snapshot(s): {', '.join(s[:8] for s in snapshot_ids)}…")
        proc = subprocess.Popen(
            ["restic", "forget", "--prune", "--verbose", "--json"] + snapshot_ids,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=self._full_env,
        )
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                log_fn(line)
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"restic forget failed (rc={proc.returncode})")
        log_fn("Forget + prune complete.")

    def run_prune(self, log_fn) -> None:
        """Prune unreferenced data from the restic repo (no forget — only removes orphaned packs)."""
        log_fn("Starting restic prune…")
        proc = subprocess.Popen(
            ["restic", "prune", "--verbose=2"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=self._full_env,
        )
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                log_fn(line)
        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"restic prune failed (rc={proc.returncode})")
        log_fn("Prune complete.")

    def backup_datastore(self, datastore_path: str, log_fn,
                         pbs_snapshots: list = None,
                         progress_fn=None) -> None:
        """Back up the PBS datastore directory to the restic repo.

        progress_fn(pct, speed_mbps, eta_s) is called with parsed restic progress.
        """
        log_fn("Stopping PBS…")
        subprocess.run(["systemctl", "stop", "proxmox-backup", "proxmox-backup-proxy"],
                       env=self._full_env, timeout=30)
        try:
            cmd = ["restic", "backup", datastore_path, "--no-lock", "--json"]
            if pbs_snapshots:
                for btype, vmid, btime in pbs_snapshots:
                    cmd += ["--tag", f"{btype}-{vmid}-{btime}"]
            log_fn(f"Running restic backup of {datastore_path}…")
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    text=True, env=self._full_env)
            last_pct = -1
            for line in proc.stdout:
                line = line.rstrip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    mtype = d.get("message_type", "")
                    if mtype == "status":
                        pct       = round(d.get("percent_done", 0) * 100)
                        speed     = d.get("current_files", 0)
                        bytes_done = d.get("bytes_done", 0)
                        total_bytes = d.get("total_bytes", 0)
                        eta_s     = d.get("seconds_remaining")
                        mb_done   = bytes_done / 1e6
                        mb_total  = total_bytes / 1e6
                        secs      = d.get("seconds_elapsed", 0)
                        mbps      = (mb_done / secs) if secs > 0 else 0
                        eta_str   = f" ETA {int(eta_s)}s" if eta_s else ""
                        # Log only on meaningful progress jumps
                        if pct != last_pct and (pct % 10 == 0 or pct >= 99):
                            log_fn(f"  {pct}% — {mb_done:.0f}/{mb_total:.0f} MB @ {mbps:.1f} MB/s{eta_str}")
                            last_pct = pct
                        if progress_fn:
                            progress_fn(pct, round(mbps, 1), eta_s)
                    elif mtype == "summary":
                        files = d.get("files_new", 0) + d.get("files_changed", 0)
                        added = d.get("data_added_packed", d.get("data_added", 0)) / 1e6
                        log_fn(f"  Done — {files} file(s) changed, {added:.1f} MB added")
                    else:
                        log_fn(line)
                except (json.JSONDecodeError, KeyError):
                    log_fn(line)
            proc.wait()
            if proc.returncode != 0:
                raise RuntimeError(f"restic backup failed (rc={proc.returncode})")
            log_fn("Restic backup complete.")
        finally:
            log_fn("Starting PBS…")
            subprocess.run(["systemctl", "start", "proxmox-backup", "proxmox-backup-proxy"],
                           env=self._full_env, timeout=30)

    def restore_datastore(self, snapshot_id: str, log_fn) -> None:
        """Restore a restic snapshot to the PBS datastore path."""
        log_fn("Stopping PBS…")
        subprocess.run(["systemctl", "stop", "proxmox-backup", "proxmox-backup-proxy"],
                       env=self._full_env, timeout=30)
        try:
            log_fn(f"Restoring restic snapshot {snapshot_id[:8]}…")
            proc = subprocess.Popen(
                ["restic", "restore", snapshot_id, "--target", "/", "--no-lock", "--verbose"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, env=self._full_env,
            )
            for line in proc.stdout:
                line = line.rstrip()
                if line:
                    log_fn(line)
            proc.wait()
            if proc.returncode != 0:
                raise RuntimeError(f"restic restore failed (rc={proc.returncode})")
            log_fn("Restore complete.")
        finally:
            log_fn("Starting PBS…")
            subprocess.run(["systemctl", "start", "proxmox-backup", "proxmox-backup-proxy"],
                           env=self._full_env, timeout=30)
            log_fn("Waiting for PBS to become ready…")
            time.sleep(5)
            log_fn("PBS ready.")


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
    vmid: str | None = None         # associated VM/LXC id (for MQTT per-VM topics)

    def append_log(self, line: str) -> None:
        self.log.append(line)
        if _mqtt:
            _mqtt.publish_log(self.op_id, line)
            # Parse restic JSON progress lines
            _try_publish_restic_progress(self.op_id, self.vmid, line)

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
_restic_op_lock = threading.Lock()  # one restic process at a time


def _pbs_type(vm_type: str) -> str:
    """Normalize PVE VM type to PBS backup-type. PVE/frontend uses 'lxc', PBS API uses 'ct'."""
    return "ct" if vm_type == "lxc" else vm_type


def _fetch_pbs_snaps(host_cfg) -> list[tuple[str, str, int]]:
    """Return (backup_type, vmid_str, backup_time) for every PBS snapshot.

    Used to tag restic backups so _scan_restic can map each restic snapshot
    back to the PBS backups it covers.
    """
    try:
        pbs = PBSClient(host_cfg)
        groups = pbs.get_snapshots()
        return [
            (group.get("backup_type", "vm"), str(group.get("pve_id", "0")), s.get("backup_time", 0))
            for group in groups for s in group.get("snapshots", [])
        ]
    except Exception:
        return []


def _new_op(op_type: str, vmid: str | None = None) -> Operation:
    op = Operation(op_id=str(uuid.uuid4()), type=op_type, vmid=vmid)
    with _ops_lock:
        _operations[op.op_id] = op
    return op


def _get_op(op_id: str) -> Operation | None:
    with _ops_lock:
        return _operations.get(op_id)


# ─────────────────────────────────────────────────────────────────────────────
# Background task runner
# ─────────────────────────────────────────────────────────────────────────────

def _try_publish_restic_progress(op_id: str, vmid: str | None, line: str) -> None:
    """If line is a restic --json status event, publish MQTT progress."""
    if not _mqtt or not line.startswith("{"):
        return
    try:
        d = json.loads(line)
        if d.get("message_type") == "status":
            pct      = d.get("percent_done", 0) * 100
            bps      = d.get("bytes_per_second")
            eta_s    = d.get("seconds_remaining")
            speed    = bps / 1_048_576 if bps else None  # bytes/s → MB/s
            _mqtt.publish_progress(op_id, vmid, pct, speed, eta_s)
    except (json.JSONDecodeError, KeyError):
        pass


def _run_in_background(op: Operation, fn, rescan_restic: bool = False) -> None:
    """Execute fn(op) in a daemon thread; set status on finish.

    rescan_restic=True: after completion, refresh _restic_snaps before publishing
    (use this for restic backup ops so new cloud snapshots appear immediately).
    """
    def _worker():
        op.status = "running"
        if _mqtt:
            _mqtt.publish_op_started(op.op_id, op.vmid)
        try:
            fn(op)
            op.status = "ok"
        except Exception as exc:
            op.append_log(f"ERROR: {exc}")
            op.status = "failed"
        finally:
            op.finished_at = time.time()
            if _mqtt:
                _mqtt.publish_op_done(op.op_id, op.vmid,
                                      ok=(op.status == "ok"),
                                      finished_at=op.finished_at)
            # Re-scan immediately so updated snapshot list + storage are published.
            # For restic ops: refresh _restic_snaps first so new cloud snapshots
            # are visible; _scan_restic() calls rescan_now() internally.
            if _poller:
                if rescan_restic:
                    _poller._scan_restic()   # updates cache → triggers rescan_now()
                elif op.type in ("backup", "restore", "delete"):
                    if op.type == "delete":
                        _poller.invalidate_vm_cache(op.vmid)
                    _poller.rescan_now()

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


# ─────────────────────────────────────────────────────────────────────────────
# PBS task helpers
# ─────────────────────────────────────────────────────────────────────────────

_PBS_TASK_TYPES = {"backup", "prune", "prunejob", "garbage_collection", "verify"}


def _get_pbs_tasks(running_only: bool = False, limit: int = 50) -> list[dict]:
    """Return PBS tasks via proxmox-backup-manager task list."""
    cmd = ["proxmox-backup-manager", "task", "list", "--all", "--output-format", "json"]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=15).stdout
        tasks = json.loads(out.strip() or "[]")
    except Exception:
        return []
    tasks = [t for t in tasks if t.get("worker_type") in _PBS_TASK_TYPES]
    if running_only:
        tasks = [t for t in tasks if not t.get("endtime")]
    return tasks[:limit]


def _get_pbs_task_log(upid: str) -> list[str]:
    """Fetch log lines for a PBS task by UPID via PBS HTTP API (works for running tasks)."""
    try:
        return PBSClient(_cfg).get_task_log(upid)
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def _load_config(path: str = "/etc/pve-agent/config.json") -> AgentConfig:
    with open(path) as f:
        d = json.load(f)
    return AgentConfig(**d)


if __name__ == "__main__":
    import atexit
    import os
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "/etc/pve-agent/config.json"
    _config_path = cfg_path
    _cfg = _load_config(cfg_path)

    # Start MQTT publisher if configured
    if _cfg.mqtt_host:
        effective_hostname = _cfg.mqtt_hostname or __import__("socket").gethostname()
        log.info(
            "MQTT publisher + StatePoller starting → %s:%s  (mqtt_hostname=%r — "
            "MUST match the 'id' field in hosts.json or GUI will show 'Connecting…')",
            _cfg.mqtt_host, _cfg.mqtt_port, effective_hostname,
        )
        if not _cfg.mqtt_hostname:
            log.warning(
                "mqtt_hostname is not set in config — falling back to OS hostname %r. "
                "Set mqtt_hostname in config.json to avoid topic mismatch with hosts.json.",
                effective_hostname,
            )
        _mqtt = MQTTPublisher(
            host=_cfg.mqtt_host,
            port=_cfg.mqtt_port,
            user=_cfg.mqtt_user,
            password=_cfg.mqtt_password,
            hostname=_cfg.mqtt_hostname,
        )
        _mqtt.setup_message_handler()

        _ha_mqtt = None
        if _cfg.mqtt_ha_host:
            log.info("HA MQTT publisher starting → %s:%s (discovery + summary/storage only)",
                     _cfg.mqtt_ha_host, _cfg.mqtt_ha_port)
            _ha_mqtt = HAMQTTPublisher(
                host=_cfg.mqtt_ha_host,
                port=_cfg.mqtt_ha_port,
                user=_cfg.mqtt_ha_user,
                password=_cfg.mqtt_ha_password,
                hostname=_cfg.mqtt_hostname,
            )
            atexit.register(_ha_mqtt.shutdown)

        _poller = StatePoller(_cfg, _mqtt, ha_mqtt=_ha_mqtt)

        def _on_mqtt_ready():
            """Called 2s after startup — MQTT connection should be up."""
            _mqtt.publish_agent_discovery()
            _poller.start()

        threading.Timer(2.0, _on_mqtt_ready).start()
        atexit.register(_mqtt.shutdown)
        atexit.register(_poller.stop)
        log.info("MQTT publisher + StatePoller starting → %s:%s",
                 _cfg.mqtt_host, _cfg.mqtt_port)
    else:
        log.info("MQTT not configured — running without event publishing")

    log.info("Agent is now running in pure MQTT mode.")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
