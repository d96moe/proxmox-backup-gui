import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

BACKEND = Path(__file__).parent.parent.parent / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import mqtt_manager
from mqtt_manager import MQTT_CACHE, init_mqtt, publish_cmd, _on_message
import agent_client
from agent_client import AgentClient


@pytest.fixture
def mock_app():
    app = MagicMock()
    app.config = {
        "MQTT_HOST": "localhost",
        "MQTT_PORT": 1883,
        "MQTT_USER": "testuser",
        "MQTT_PASSWORD": "testpassword"
    }
    return app


@pytest.fixture
def clear_cache():
    MQTT_CACHE.clear()
    yield
    MQTT_CACHE.clear()


def test_mqtt_manager_cache_update(clear_cache):
    """Test that incoming MQTT messages update the MQTT_CACHE."""
    msg = MagicMock()
    msg.topic = "proxmox/gui-ci/agent/status"
    msg.payload = b'"online"'
    msg.retain = True
    
    # Simulate an incoming message
    _on_message(None, None, msg)
    
    assert "proxmox/gui-ci/agent/status" in MQTT_CACHE
    assert MQTT_CACHE["proxmox/gui-ci/agent/status"] == "online"


class DummyHost:
    def __init__(self, host_id):
        self.id = host_id

def test_agent_client_fetches_from_cache(clear_cache):
    """Test that AgentClient retrieves state from MQTT_CACHE instead of HTTP."""
    host = DummyHost("gui-ci")
    
    # Pre-populate cache
    MQTT_CACHE["proxmox/gui-ci/vm/100/meta"] = {"vmid": 100, "type": "qemu"}
    MQTT_CACHE["proxmox/gui-ci/vm/100/pbs"] = {"snapshots": [{"id": "s1", "backup_time": 1700000000}]}
    MQTT_CACHE["proxmox/gui-ci/vms/index"] = [100]
    
    client = AgentClient(host)
    items = client.get_items()
    
    assert len(items["vms"]) == 1
    assert items["vms"][0]["id"] == 100
    assert len(items["vms"][0]["snapshots"]) == 1


@patch("agent_client.publish_cmd")
def test_publish_cmd_sends_mqtt_message(mock_publish_cmd, mock_app):
    """Test that AgentClient uses publish_cmd internally for cmds."""
    host = DummyHost("gui-ci")
    client = AgentClient(host)
    
    client.rescan()
    mock_publish_cmd.assert_called_once_with("proxmox/gui-ci/cmd/rescan", {})


@patch("agent_client.publish_cmd")
def test_agent_client_backup_publishes_cmd(mock_publish_cmd):
    """Test that backup() uses publish_cmd and waits for ack."""
    host = DummyHost("gui-ci")
    client = AgentClient(host)

    import os as _os
    # Temporarily remove PYTEST_CURRENT_TEST so the real MQTT path runs
    saved = _os.environ.pop("PYTEST_CURRENT_TEST", None)
    try:
        with patch.object(client, "_wait_for_ack", return_value="fake-op"):
            res = client.backup(100, "qemu", "pve1", "local")

            assert res == "fake-op"
            mock_publish_cmd.assert_called_once()
            args, _ = mock_publish_cmd.call_args
            assert args[0] == "proxmox/gui-ci/cmd/backup"
            assert args[1]["vmid"] == 100
    finally:
        if saved is not None:
            _os.environ["PYTEST_CURRENT_TEST"] = saved
