import sys
import json
import uuid
import time
from pathlib import Path
from unittest.mock import patch, MagicMock, ANY

import pytest

BACKEND = Path(__file__).parent.parent.parent / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from pve_agent import MQTTPublisher, StatePoller, AgentConfig, Operation, _new_op

@pytest.fixture
def mock_cfg():
    cfg = AgentConfig(
        pve_url="http://pve",
        pve_user="root@pam",
        pve_password="pve",
        pbs_url="http://pbs",
        pbs_user="root@pam",
        pbs_password="pbs",
        pbs_datastore="pbs-store",
        pbs_storage_id="pbs-storage",
        pbs_datastore_path="/mnt/datastore/pbs-store"
    )
    cfg.mqtt_hostname = "test-node"
    cfg.pbs_storage_id = "pbs-storage"
    cfg.restic_repo = "/path/to/restic"
    cfg.pve_node = "pve1"
    return cfg

@pytest.fixture
def mock_mqtt_client():
    with patch("paho.mqtt.client.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        yield mock_client

class TestMQTTPublisher:
    
    @patch("pve_agent._cfg")
    def test_on_connect_subscribes_and_publishes_online(self, mock_global_cfg, mock_cfg, mock_mqtt_client):
        mock_global_cfg.return_value = mock_cfg
        pub = MQTTPublisher("127.0.0.1", hostname="test-node")
        
        pub._on_connect(mock_mqtt_client, None, None, 0)
        
        mock_mqtt_client.publish.assert_any_call("proxmox/test-node/agent/status", "online", retain=True, qos=1)
        mock_mqtt_client.subscribe.assert_any_call("proxmox/test-node/cmd/+", qos=1)
        mock_mqtt_client.subscribe.assert_any_call("proxmox/test-node/vm/+/meta", qos=0)
        mock_mqtt_client.subscribe.assert_any_call("proxmox/test-node/vms/index", qos=1)

    @patch("pve_agent._cfg")
    @patch("pve_agent.threading.Thread")
    def test_on_message_routes_commands_to_threads(self, mock_thread, mock_global_cfg, mock_cfg, mock_mqtt_client):
        pub = MQTTPublisher("127.0.0.1", hostname="test-node")
        
        msg = MagicMock()
        msg.topic = "proxmox/test-node/cmd/backup"
        msg.payload = b'{"vmid": "100"}'
        
        pub._on_message(mock_mqtt_client, None, msg)
        
        mock_thread.assert_called_once()
        args, kwargs = mock_thread.call_args
        assert kwargs["target"] == pub._handle_cmd_backup
        assert kwargs["args"][0] == {"vmid": "100"}
        mock_thread.return_value.start.assert_called_once()

    @patch("pve_agent.PVEClient")
    def test_handle_cmd_backup_happy_path(self, mock_pve_cls, mock_cfg, mock_mqtt_client):
        with patch("pve_agent._cfg", new=mock_cfg), \
             patch("pve_agent._run_in_background") as mock_run_bg:
            
            pub = MQTTPublisher("127.0.0.1", hostname="test-node")
            body = {"vmid": "101", "type": "qemu", "corr_id": "corr-123"}
            pub._handle_cmd_backup(body)
            
            # Check the background task function
            mock_run_bg.assert_called_once()
            op = mock_run_bg.call_args[0][0]
            func = mock_run_bg.call_args[0][1]
            
            assert op.type == "backup"
            assert op.vmid == "101"
            
            mock_pve = mock_pve_cls.return_value
            mock_pve.backup_vm.return_value = "UPID:node1:101:backup"
            mock_pve.wait_for_task.return_value = True
            
            with patch("pve_agent._host", return_value=mock_cfg):
                func(op)
            
            mock_pve.backup_vm.assert_called_once_with(101, "qemu", "pbs-storage", "pve1")
            mock_pve.wait_for_task.assert_called_once()

    @patch("pve_agent.PVEClient")
    def test_handle_cmd_restore_happy_path(self, mock_pve_cls, mock_cfg, mock_mqtt_client):
        with patch("pve_agent._cfg", new=mock_cfg), \
             patch("pve_agent._run_in_background") as mock_run_bg:
             
            pub = MQTTPublisher("127.0.0.1", hostname="test-node")
            body = {
                "vmid": "102", "type": "lxc", "backup_time": 1700000000, 
                "source": "local", "corr_id": "corr-rest"
            }
            pub._handle_cmd_restore(body)
            
            mock_run_bg.assert_called_once()
            op = mock_run_bg.call_args[0][0]
            func = mock_run_bg.call_args[0][1]
            
            mock_pve = mock_pve_cls.return_value
            mock_pve.restore_vm.return_value = "UPID:task"
            mock_pve.wait_for_task.return_value = True
            
            with patch("pve_agent._host", return_value=mock_cfg):
                func(op)
            
            mock_pve.stop_vm.assert_called_once_with(102, "ct", "pve1")
            mock_pve.restore_vm.assert_called_once()
            mock_pve.start_vm.assert_called_once_with(102, "ct", "pve1")

    @patch("pve_agent.PBSClient")
    def test_handle_cmd_delete_happy_path(self, mock_pbs_cls, mock_cfg, mock_mqtt_client):
        with patch("pve_agent._cfg", new=mock_cfg), \
             patch("pve_agent._run_in_background") as mock_run_bg:
            
            pub = MQTTPublisher("127.0.0.1", hostname="test-node")
            body = {"vmid": "103", "type": "qemu", "backup_time": 1700000000, "scope": "pbs", "corr_id": "corr-del"}
            pub._handle_cmd_delete(body)
            
            mock_run_bg.assert_called_once()
            op = mock_run_bg.call_args[0][0]
            func = mock_run_bg.call_args[0][1]
            
            mock_pbs = mock_pbs_cls.return_value
            mock_pbs.delete_snapshot.return_value = True
            
            with patch("pve_agent._host", return_value=mock_cfg):
                func(op)
            
            mock_pbs.delete_snapshot.assert_called_once_with("qemu", "103", 1700000000)
            mock_pbs.start_gc.assert_called_once()

    @patch("pve_agent.PBSClient")
    def test_handle_cmd_delete_all_happy_path(self, mock_pbs_cls, mock_cfg, mock_mqtt_client):
        with patch("pve_agent._cfg", new=mock_cfg), \
             patch("pve_agent._run_in_background") as mock_run_bg:
            
            pub = MQTTPublisher("127.0.0.1", hostname="test-node")
            body = {"vmid": "104", "type": "qemu", "corr_id": "corr-delall"}
            pub._handle_cmd_delete_all(body)
            
            mock_run_bg.assert_called_once()
            op = mock_run_bg.call_args[0][0]
            func = mock_run_bg.call_args[0][1]
            
            mock_pbs = mock_pbs_cls.return_value
            mock_pbs.delete_all_snapshots_for_vm.return_value = 5
            
            with patch("pve_agent._host", return_value=mock_cfg):
                func(op)
            
            mock_pbs.delete_all_snapshots_for_vm.assert_called_once_with("qemu", "104", ANY)

    @patch("pve_agent._cfg")
    @patch("pve_agent.threading.Thread")
    def test_on_message_routes_settings_to_handler(self, mock_thread, mock_global_cfg, mock_cfg, mock_mqtt_client):
        pub = MQTTPublisher("127.0.0.1", hostname="test-node")
        msg = MagicMock()
        msg.topic = "proxmox/test-node/cmd/settings"
        msg.payload = b'{"retention": {"keep-last": 5}}'
        pub._on_message(mock_mqtt_client, None, msg)
        mock_thread.assert_called_once()
        _, kwargs = mock_thread.call_args
        assert kwargs["target"] == pub._handle_cmd_settings

    @patch("pve_agent.LocalResticClient")
    def test_handle_cmd_settings_applies_retention_and_acks(self, mock_restic_cls, mock_cfg, mock_mqtt_client):
        with patch("pve_agent._cfg", new=mock_cfg), \
             patch("pve_agent._poller") as mock_poller:
            pub = MQTTPublisher("127.0.0.1", hostname="test-node")
            pub._handle_cmd_settings({"retention": {"keep-last": 7}, "corr_id": "c-set"})

            mock_restic_cls.return_value.set_retention.assert_called_once_with({"keep-last": 7})
            # republishes settings + schedules so the GUI reflects the write
            mock_poller._scan_settings.assert_called_once()
            mock_poller._scan_schedules.assert_called_once()
            # acks the corr_id so the GUI stops waiting
            ack_calls = [c for c in mock_mqtt_client.publish.call_args_list
                         if "job/c-set/ack" in c[0][0]]
            assert ack_calls, "settings write must ack the corr_id"

    @patch("pve_agent._pvesh_set_backup_vm_selection")
    @patch("pve_agent._pvesh_set_backup_schedule")
    @patch("pve_agent.PVEClient")
    @patch("pve_agent.LocalResticClient")
    def test_handle_cmd_settings_falls_back_to_pvesh_on_arm_put_drop(
            self, mock_restic_cls, mock_pve_cls, mock_pvesh_sched, mock_pvesh_vmsel,
            mock_cfg, mock_mqtt_client):
        """Regression: PVE 9.0.x on ARM (the cabin Pi 5) drops HTTP PUT
        /cluster/backup/<id> (RemoteDisconnected). The settings handler must fall
        back to pvesh — the REST-era fallback (4fb9aaf) that was lost when settings
        moved to the MQTT cmd handler — and still ack SUCCESS, not 500."""
        pve = mock_pve_cls.return_value
        pve.set_backup_schedule.side_effect = Exception("Remote end closed connection")
        pve.set_backup_vm_selection.side_effect = Exception("Remote end closed connection")
        pve.get_backup_schedules.return_value = [{"id": "nightly-backup"}]
        with patch("pve_agent._cfg", new=mock_cfg), patch("pve_agent._poller"):
            pub = MQTTPublisher("127.0.0.1", hostname="test-node")
            pub._handle_cmd_settings({
                "pbs_schedule": {"id": "nightly-backup", "schedule": "02:00"},
                "vm_selection": {"mode": "exclude", "vmids": []},
                "corr_id": "c-arm",
            })
        # HTTP PUT raised → fell back to pvesh for both writes
        mock_pvesh_sched.assert_called_once_with("nightly-backup", "02:00")
        mock_pvesh_vmsel.assert_called_once_with("nightly-backup", "exclude", [])
        # …and acked SUCCESS (no error), because the fallback handled the drop
        ack_calls = [c for c in mock_mqtt_client.publish.call_args_list
                     if "job/c-arm/ack" in c[0][0]]
        assert ack_calls, "must ack the corr_id"
        payload = ack_calls[-1][0][1]
        assert "error" not in payload, \
            f"settings must succeed via pvesh fallback, got: {payload}"

    @patch("pve_agent._cfg")
    @patch("pve_agent.threading.Thread")
    def test_on_message_routes_connection_to_handler(self, mock_thread, mock_global_cfg, mock_cfg, mock_mqtt_client):
        pub = MQTTPublisher("127.0.0.1", hostname="test-node")
        msg = MagicMock()
        msg.topic = "proxmox/test-node/cmd/connection"
        msg.payload = b'{"pbs_user": "x"}'
        pub._on_message(mock_mqtt_client, None, msg)
        mock_thread.assert_called_once()
        _, kwargs = mock_thread.call_args
        assert kwargs["target"] == pub._handle_cmd_connection

    @patch("pve_agent.AgentConfig")
    def test_handle_cmd_connection_writes_keeps_empty_secret_and_acks(
            self, mock_agentcfg, mock_cfg, mock_mqtt_client, tmp_path):
        cfgfile = tmp_path / "config.json"
        cfgfile.write_text(json.dumps(
            {"pve_url": "http://old", "pbs_user": "u", "pbs_password": "secret"}))
        with patch("pve_agent._cfg", new=mock_cfg), \
             patch("pve_agent._config_path", str(cfgfile)), \
             patch("pve_agent._poller") as mock_poller:
            pub = MQTTPublisher("127.0.0.1", hostname="test-node")
            pub._handle_cmd_connection(
                {"pbs_user": "newuser", "pbs_password": "", "corr_id": "c-con"})

        written = json.loads(cfgfile.read_text())
        assert written["pbs_user"] == "newuser"
        assert written["pbs_password"] == "secret"   # empty posted secret = unchanged
        mock_poller._scan_connection.assert_called_once()
        ack = [c for c in mock_mqtt_client.publish.call_args_list if "job/c-con/ack" in c[0][0]]
        assert ack, "connection write must ack the corr_id"


class TestStatePoller:
    
    @patch("pve_agent.PVEClient")
    @patch("pve_agent.PBSClient")
    def test_scan_pve_pbs_publishes_vms(self, mock_pbs_cls, mock_pve_cls, mock_cfg, mock_mqtt_client):
        pub = MQTTPublisher("127.0.0.1", hostname="test-node")
        poller = StatePoller(mock_cfg, pub)
        
        mock_pve = mock_pve_cls.return_value
        mock_pve.get_vms_and_lxcs.return_value = {
            100: {"name": "VM100", "status": "running"},
            101: {"name": "VM101", "status": "stopped"}
        }
        
        mock_pbs = mock_pbs_cls.return_value
        mock_pbs.get_snapshots.return_value = [
            {"vmid": 100, "backup_time": 1700000000, "size": 1024}
        ]
        
        with patch("pve_agent._host", return_value=mock_cfg):
            poller._scan_pve_pbs()
        
        mock_mqtt_client.publish.assert_any_call(
            "proxmox/test-node/vms/index", '["100","101"]', retain=True, qos=1
        )
        
        call_args_list = mock_mqtt_client.publish.call_args_list
        meta_100 = [c for c in call_args_list if "vm/100/meta" in c[0][0]]
        assert len(meta_100) > 0
        assert "VM100" in meta_100[0][0][1]

    @patch("pve_agent.LocalResticClient")
    @patch("pve_agent.PVEClient")
    def test_scan_settings_publishes_settings_topic(self, mock_pve_cls, mock_restic_cls, mock_cfg, mock_mqtt_client):
        pub = MQTTPublisher("127.0.0.1", hostname="test-node")
        poller = StatePoller(mock_cfg, pub)

        mock_pve = mock_pve_cls.return_value
        mock_pve.get_backup_schedules.return_value = [{"id": "backup-1", "schedule": "02:00"}]
        mock_pve.get_backup_vm_selection.return_value = {"mode": "exclude", "vmids": [105]}

        mock_res = mock_restic_cls.return_value
        mock_res.get_retention.return_value = {"keep-last": 5}
        mock_res.get_restic_schedule.return_value = "03:00"
        mock_res.get_pbs_prune_jobs.return_value = []

        with patch("pve_agent._host", return_value=mock_cfg):
            poller._scan_settings()

        settings_pub = [c for c in mock_mqtt_client.publish.call_args_list
                        if c[0][0] == "proxmox/test-node/settings"]
        assert settings_pub, "_scan_settings must publish the settings topic"
        payload = settings_pub[0][0][1]
        assert "keep-last" in payload and "backup-1" in payload

    def test_scan_connection_publishes_redacted(self, mock_cfg, mock_mqtt_client, tmp_path):
        cfgfile = tmp_path / "config.json"
        cfgfile.write_text(json.dumps(
            {"pve_url": "http://pve", "pbs_user": "u", "pbs_password": "secret"}))
        with patch("pve_agent._config_path", str(cfgfile)):
            pub = MQTTPublisher("127.0.0.1", hostname="test-node")
            poller = StatePoller(mock_cfg, pub)
            poller._scan_connection()
        conn_pub = [c for c in mock_mqtt_client.publish.call_args_list
                    if c[0][0] == "proxmox/test-node/connection"]
        assert conn_pub, "_scan_connection must publish the connection topic"
        payload = conn_pub[0][0][1]
        assert "http://pve" in payload      # non-secret fields present
        assert "secret" not in payload      # secret redacted
