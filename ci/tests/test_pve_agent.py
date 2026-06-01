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
    cfg = AgentConfig()
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
            
            assert op.op_type == "backup"
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

class TestStatePoller:
    
    @patch("pve_agent.PVEClient")
    @patch("pve_agent.PBSClient")
    def test_scan_pve_pbs_publishes_vms(self, mock_pbs_cls, mock_pve_cls, mock_cfg, mock_mqtt_client):
        pub = MQTTPublisher("127.0.0.1", hostname="test-node")
        poller = StatePoller(mock_cfg, pub)
        
        mock_pve = mock_pve_cls.return_value
        mock_pve.get_vms.return_value = [
            {"vmid": 100, "name": "VM100", "status": "running"},
            {"vmid": 101, "name": "VM101", "status": "stopped"}
        ]
        
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
