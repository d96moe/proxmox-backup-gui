from __future__ import annotations
import time
import uuid
import json
from typing import Callable, Iterator
from mqtt_manager import MQTT_CACHE, MQTT_CACHE_LOCK, publish_cmd

class AgentClient:
    def __init__(self, host_cfg) -> None:
        # host_cfg is a HostConfig object
        self._cfg = host_cfg
        import os
        self._prefix = os.environ.get("MQTT_TOPIC_PREFIX", "proxmox")
        # In CI, the agent node name is "gui-ci" or whatever pve_node is.
        # But wait, the GUI uses host_cfg.id.
        self._base = f"{self._prefix}/{host_cfg.id}"

    def get_items(self) -> dict:
        vms = []
        lxcs = []
        storage = {}
        pbs_stale = False
        
        with MQTT_CACHE_LOCK:
            index = MQTT_CACHE.get(f"{self._base}/vms/index", [])
            for vmid in index:
                meta = MQTT_CACHE.get(f"{self._base}/vm/{vmid}/meta", {}).copy()
                pbs = MQTT_CACHE.get(f"{self._base}/vm/{vmid}/pbs", {})
                restic = MQTT_CACHE.get(f"{self._base}/vm/{vmid}/restic", {})
                
                meta["snapshots"] = pbs.get("snapshots", []) + restic.get("snapshots", [])
                
                if meta.get("type") == "qemu":
                    vms.append(meta)
                else:
                    lxcs.append(meta)
                    
            storage = MQTT_CACHE.get(f"{self._base}/storage", {})
            ready = MQTT_CACHE.get(f"{self._base}/state/ready", {})
            pbs_stale = not ready.get("pbs_ok", True)
            
        return {
            "vms": vms,
            "lxcs": lxcs,
            "storage": storage,
            "pbs_stale": pbs_stale
        }

    def rescan(self) -> dict:
        publish_cmd(f"{self._base}/cmd/rescan", {})
        return {}

    def get_operations(self) -> list[dict]:
        ops = []
        with MQTT_CACHE_LOCK:
            # We don't have a list of all operations published. We can just return [] or read from cache if the agent publishes it.
            # Actually, we can scan for /ops/+/status
            for t, p in MQTT_CACHE.items():
                if t.startswith(f"{self._base}/ops/") and t.endswith("/status"):
                    op_id = t.split("/")[3]
                    ops.append({"op_id": op_id, "status": p})
        return ops

    def get_operation(self, op_id: str) -> dict:
        with MQTT_CACHE_LOCK:
            status = MQTT_CACHE.get(f"{self._base}/ops/{op_id}/status", "unknown")
            return {"op_id": op_id, "status": status}

    def backup(self, vmid: int, vm_type: str, node: str, storage: str) -> str:
        corr_id = str(uuid.uuid4())
        publish_cmd(f"{self._base}/cmd/backup", {
            "vmid": vmid, "vm_type": vm_type,
            "node": node, "storage": storage,
            "corr_id": corr_id
        })
        return self._wait_for_ack("backup", corr_id)

    def restore(self, vmid: int, vm_type: str, node: str,
                storage_id: str, backup_time_iso: str, pbs_datastore: str) -> str:
        corr_id = str(uuid.uuid4())
        publish_cmd(f"{self._base}/cmd/restore", {
            "vmid": vmid, "vm_type": vm_type, "node": node,
            "storage_id": storage_id, "backup_time_iso": backup_time_iso,
            "pbs_datastore": pbs_datastore,
            "corr_id": corr_id
        })
        return self._wait_for_ack("restore", corr_id)

    def start_restic_prune(self) -> dict:
        corr_id = str(uuid.uuid4())
        publish_cmd(f"{self._base}/cmd/restic-prune", {"corr_id": corr_id})
        op_id = self._wait_for_ack("restic-prune", corr_id)
        return {"op_id": op_id}

    def _wait_for_ack(self, cmd: str, corr_id: str, timeout: int = 5) -> str:
        start = time.time()
        while time.time() - start < timeout:
            with MQTT_CACHE_LOCK:
                ack = MQTT_CACHE.get(f"{self._base}/cmd/{cmd}/ack", {})
                if ack.get("corr_id") == corr_id:
                    return ack.get("op_id", "")
            time.sleep(0.1)
        return ""

    def get_settings(self) -> dict:
        with MQTT_CACHE_LOCK:
            return MQTT_CACHE.get(f"{self._base}/settings", {})

    def set_settings(self, settings: dict) -> dict:
        publish_cmd(f"{self._base}/cmd/settings", settings)
        return settings

    def get_connection(self) -> dict:
        # Connection settings aren't actively polled over MQTT, they are configured per agent.
        # But Phase 3 implies removing this from GUI or using MQTT.
        return {}

    def set_connection(self, settings: dict) -> dict:
        return {}
        
    def get_schedules(self) -> dict:
        with MQTT_CACHE_LOCK:
            return MQTT_CACHE.get(f"{self._base}/schedules", {})

    def get_pbs_tasks(self, running_only: bool = False) -> list[dict]:
        with MQTT_CACHE_LOCK:
            tasks = MQTT_CACHE.get(f"{self._base}/pbs/tasks", [])
            if running_only:
                tasks = [t for t in tasks if t.get("status") == "running"]
            return tasks

    def get_pbs_task_log(self, upid: str) -> list[str]:
        publish_cmd(f"{self._base}/cmd/replay_pbs_log", {"upid": upid})
        # The GUI now uses SSE to stream or poll the log. Wait, Phase 3 removes SSE!
        # The frontend will subscribe to the topic directly. So returning empty here is fine,
        # or we return what we have so far.
        return []

    def get_restic_log(self) -> dict:
        publish_cmd(f"{self._base}/cmd/replay_restic_log", {})
        return {"lines": []}
