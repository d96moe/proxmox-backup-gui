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


def test_replay_items_for_prefix_host_isolation(clear_cache):
    """Regression: the /mqtt-ws {type:"replay"} handler must return ONLY the
    requested host's retained topics. The bug was that switching to a second
    host re-delivered nothing (server ignored the request), so it stuck on
    "Connecting…" with 0 snapshots. A 'proxmox/cabin' replay must include
    cabin's vms/index + per-VM pbs, must NOT leak 'proxmox/home/*', and must
    NOT match a sibling prefix like 'proxmox/cabin2/*'."""
    import app
    MQTT_CACHE["proxmox/home/vms/index"]   = ["100"]
    MQTT_CACHE["proxmox/home/vm/100/pbs"]  = {"snapshots": [{"backup_time": 1}]}
    MQTT_CACHE["proxmox/cabin/vms/index"]  = ["200"]
    MQTT_CACHE["proxmox/cabin/vm/200/pbs"] = {"snapshots": [{"backup_time": 2}]}
    MQTT_CACHE["proxmox/cabin2/vms/index"] = ["999"]  # sibling — must NOT match

    items = dict(app._replay_items_for_prefix("proxmox/cabin"))

    assert set(items) == {"proxmox/cabin/vms/index", "proxmox/cabin/vm/200/pbs"}
    assert not any(t.startswith("proxmox/home/") for t in items)
    assert "proxmox/cabin2/vms/index" not in items
    # Payloads are JSON strings — the wire format _onMessage expects client-side.
    assert isinstance(items["proxmox/cabin/vm/200/pbs"], str)
    assert json.loads(items["proxmox/cabin/vm/200/pbs"]) == {"snapshots": [{"backup_time": 2}]}


def test_replay_items_for_prefix_empty_or_unknown(clear_cache):
    """A blank prefix returns nothing (no accidental full-cache dump), and an
    unknown host returns nothing (not a single leaked topic)."""
    import app
    MQTT_CACHE["proxmox/home/vms/index"] = ["100"]
    assert app._replay_items_for_prefix("") == []
    assert app._replay_items_for_prefix(None) == []
    assert app._replay_items_for_prefix("proxmox/__nohost__") == []


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
