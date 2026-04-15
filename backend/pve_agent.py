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

from flask import Flask, Response, jsonify, request, stream_with_context

# Re-use existing client code — agent runs on PVE host alongside them.
from pbs_client import PBSClient
from pve_client import PVEClient

VERSION = "0.1.0"
_start_time = time.monotonic()

app = Flask(__name__)
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
        else:
            log.warning("MQTT connect failed rc=%s", rc)

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

    def _ack(self, corr_id: str, op_id: str) -> None:
        """Publish job ACK so the browser can start polling the operation."""
        self._client.publish(
            f"{self._base}/job/{corr_id}/ack",
            json.dumps({"op_id": op_id}), retain=True, qos=1,
        )

    def _node(self) -> str:
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
                res.backup_datastore(_cfg.pbs_datastore_path, op.append_log, [],
                                     progress_fn=_prog_backup)
                op.append_log("Cloud backup complete.")

        _run_in_background(op, _do)

    def _handle_cmd_restore(self, body: dict) -> None:
        vmid        = body.get("vmid")
        vm_type     = body.get("type", "vm")
        backup_time = body.get("backup_time")
        corr_id     = body.get("corr_id", str(uuid.uuid4()))
        run_backup_after = body.get("run_backup_after", False)
        if not vmid or not backup_time or not _cfg:
            return
        node         = self._node()
        storage_id   = _cfg.pbs_storage_id
        pbs_ds       = _cfg.pbs_datastore
        bt_iso       = datetime.fromtimestamp(int(backup_time), tz=timezone.utc).isoformat()
        op           = _new_op("restore", vmid=str(vmid))
        self._ack(corr_id, op.op_id)
        log.info("MQTT cmd/restore: %s/%s ts=%s corr_id=%s", vm_type, vmid, backup_time, corr_id)

        def _do(op: Operation):
            pve = PVEClient(_host())
            total = 3 if run_backup_after else 2
            op.append_log(f"Step 1/{total} — Stopping {vm_type}/{vmid}…")
            pve.stop_vm(int(vmid), vm_type, node)
            op.append_log(f"Step 2/{total} — Restoring from PBS snapshot {bt_iso}…")
            upid = pve.restore_vm(int(vmid), vm_type, bt_iso, storage_id, node, pbs_ds)
            ok   = pve.wait_for_task(node, upid, op.append_log)
            if not ok:
                raise RuntimeError("Restore task failed")
            op.append_log("Restore complete.")
            pve.start_vm(int(vmid), vm_type, node)
            op.append_log(f"Started {vm_type}/{vmid}.")

        _run_in_background(op, _do)

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

        do_restic = scope == "both" and bool(restic_id and _cfg.restic_repo)

        def _do(op: Operation):
            if scope in ("pbs", "both"):
                pbs = PBSClient(_host())
                op.append_log(f"Deleting PBS snapshot {vm_type}/{vmid} at {backup_time}…")
                deleted = pbs.delete_snapshot(vm_type, str(vmid), int(backup_time))
                if deleted:
                    op.append_log("PBS snapshot deleted.")
                    op.append_log("Starting datastore GC…")
                    pbs.start_gc()
                    op.append_log("GC started (runs in background on PBS).")
                else:
                    op.append_log("PBS snapshot already gone (400/404 — check agent log for PBS response body).")
            if do_restic:
                op.append_log("Deleting cloud (restic) copy…")
                res = LocalResticClient(_cfg)
                res.forget_snapshots([restic_id], op.append_log)
                op.append_log("Cloud copy deleted.")

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
            count = pbs.delete_all_snapshots_for_vm(vm_type, str(vmid), op.append_log)
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
            res = LocalResticClient(_cfg)
            pbs = PBSClient(_host())
            snaps_raw = pbs.get_snapshots()
            pbs_snaps = [
                (group.get("backup_type", "vm"), str(group.get("pve_id", "0")), s.get("backup_time", 0))
                for group in snaps_raw for s in group.get("snapshots", [])
            ]
            def _prog_restic(pct, speed, eta):
                self.publish_progress(op.op_id, None, pct, speed, eta)
            res.backup_datastore(_cfg.pbs_datastore_path, op.append_log, pbs_snaps,
                                 progress_fn=_prog_restic)

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
            res = LocalResticClient(_cfg)
            res.forget_snapshots([restic_id], op.append_log)

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

    def publish_agent_discovery(self) -> None:
        """Publish agent-level HA Discovery (online/offline binary sensor)."""
        hn  = self._hostname
        dev = {"identifiers": [f"proxmox_{hn}"], "name": f"Proxmox {hn}",
               "manufacturer": "Proxmox"}
        topic = f"homeassistant/binary_sensor/proxmox_{hn}_agent/config"
        self._client.publish(topic, json.dumps({
            "name": f"Proxmox {hn} Agent",
            "state_topic": f"{self._base}/agent/status",
            "payload_on": "online",
            "payload_off": "offline",
            "unique_id": f"proxmox_{hn}_agent_status",
            "device_class": "connectivity",
            "device": dev,
        }), retain=True, qos=1)


# Module-level publisher — None when MQTT is not configured
_mqtt: MQTTPublisher | None = None


# ─────────────────────────────────────────────────────────────────────────────
# State poller — maintains current VM/PBS/restic state, publishes diffs
# ─────────────────────────────────────────────────────────────────────────────

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

    def __init__(self, cfg: "AgentConfig", mqtt: MQTTPublisher) -> None:
        self._cfg   = cfg
        self._mqtt  = mqtt
        self._base  = mqtt._base
        self._stop  = threading.Event()
        self._hashes: dict[str, str] = {}  # topic_suffix → last published payload
        self._hash_lock = threading.Lock()
        # Keep latest restic state so PBS poll can cross-reference
        self._restic_snaps: list[dict] = []
        self._restic_lock  = threading.Lock()
        # Last known local PBS storage stats — merged with cloud stats in storage topic
        self._local_storage: dict = {}
        self._storage_lock  = threading.Lock()

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
        log.info("StatePoller started (pve/pbs %ds, restic %ds, cloud-storage %ds, info %ds, schedules %ds)",
                 self.PVE_PBS_INTERVAL, self.RESTIC_INTERVAL, self.CLOUD_STORAGE_INTERVAL,
                 self.INFO_INTERVAL, self.SCHEDULES_INTERVAL)

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

    # ── scan implementations ──────────────────────────────────────────────────

    def _scan_pve_pbs(self) -> None:
        cfg = self._cfg

        # ── PVE: get all VMs/LXCs ────────────────────────────────────────────
        pve_meta: dict = {}
        pve_ok = True
        try:
            pve = PVEClient(_host())
            pve_meta = pve.get_vms_and_lxcs()
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
            for snap in raw_snaps:
                bt = snap.get("backup_time")
                if bt is not None:
                    local_times.add(bt)
                rs = vm_restic.get(bt)
                annotated.append({
                    **snap,
                    "local":           True,
                    "cloud":           rs is not None,
                    "restic_id":       rs["id"]       if rs else None,
                    "restic_short_id": rs["short_id"] if rs else None,
                })

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
                    "size":            "—",
                    "size_bytes":      0,
                    "restic_id":       rs["id"],
                    "restic_short_id": rs["short_id"],
                })

            annotated.sort(key=lambda s: s.get("backup_time", 0), reverse=True)

            self._pub_if_changed(f"vm/{vmid}/pbs", {"snapshots": annotated})

            # Publish restic snapshots for this VM with covers annotated by local presence
            pbs_times = {s.get("backup_time") for s in raw_snaps}
            vm_restic_snaps = [
                {
                    **s,
                    "covers": [
                        {
                            **c,
                            "local":    c.get("pbs_time") in pbs_times,
                            "pbs_date": datetime.fromtimestamp(c["pbs_time"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                                        if c.get("pbs_time") else None,
                        }
                        for c in s.get("covers", [])
                        if c.get("vmid") == (int(vmid) if vmid.isdigit() else vmid)
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

        # publish index (sorted list of known vmids)
        self._pub_if_changed("vms/index", sorted(all_vmids))
        # publish overall state
        self._pub_if_changed("state/ready", {
            "ts": time.time(), "pve_ok": pve_ok, "pbs_ok": pbs_ok,
        })
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
    # MQTT (all optional — omit mqtt_host to disable)
    mqtt_host: str = ""
    mqtt_port: int = 1883
    mqtt_user: str = ""
    mqtt_password: str = ""
    mqtt_hostname: str = ""      # MQTT topic prefix override (defaults to socket.gethostname())

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
        """Return True if a restic *backup* is currently running (not just listing)."""
        try:
            result = subprocess.run(
                ["pgrep", "-f", "restic backup"],
                capture_output=True, timeout=5,
            )
            return result.returncode == 0
        except Exception:
            return False

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

    op = _new_op("backup", vmid=str(vmid))

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

    op = _new_op("restore", vmid=str(vmid))

    def _do(op: Operation):
        pve = PVEClient(_host())
        op.append_log(f"Starting restore: vmid={vmid} node={node} ts={backup_time_iso}")
        # Stop VM/LXC before restore — PVE rejects overwriting a running container.
        pve.stop_vm(int(vmid), vm_type, node)
        upid = pve.restore_vm(int(vmid), vm_type, backup_time_iso,
                              storage_id, node, pbs_datastore)
        op.append_log(f"Task started: {upid}")
        ok = pve.wait_for_task(node, upid, op.append_log)
        if not ok:
            raise RuntimeError("PVE restore task did not complete successfully")
        op.append_log("Restore complete")
        # Start VM/LXC after restore — PVE does not auto-start after restore.
        # For self-restore (ct/300): agent runs on PVE host and can start ct/300
        # even after Flask (inside ct/300) has died.
        pve.start_vm(int(vmid), vm_type, node)
        op.append_log(f"Started {vm_type}/{vmid}")

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
    import atexit
    import os
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "/etc/pve-agent/config.json"
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
        _poller = StatePoller(_cfg, _mqtt)

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

    # Bind address: AGENT_BIND env overrides the default 10.10.0.1.
    # On a nested CI VM, vmbr0 is 10.10.0.1. On a real PVE host, use the
    # actual bridge IP (e.g. 192.168.0.200) or 0.0.0.0 for all interfaces.
    bind_host = os.environ.get("AGENT_BIND", "10.10.0.1")
    bind_port = int(os.environ.get("AGENT_PORT", "8099"))
    app.run(host=bind_host, port=bind_port, threaded=True)
