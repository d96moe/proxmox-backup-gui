import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

BACKEND = Path(__file__).parent.parent.parent / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import pve_client
from pve_client import _pvesh_create, _pvesh_get


def test_pvesh_create_extracts_upid_from_streamed_log():
    """Regression: `pvesh create /nodes/<n>/vzdump` runs SYNCHRONOUSLY and streams
    the whole backup log to stdout, ending with the UPID. _pvesh_create must return
    just the UPID — not the whole log blob, which was mistaken for the UPID and made
    wait_for_task fetch /tasks/<entire-log>/status → HTTP 501 and fail an op whose
    backup actually succeeded."""
    streamed = (
        "INFO: starting new backup job: vzdump 100 --storage pbs-local\n"
        "INFO: Finished Backup of VM 100 (00:00:03)\n"
        "INFO: Backup job finished successfully\n"
        '"UPID:raspmox:00073F90:1E82807A:6A208D79:vzdump:100:root@pam:"'
    )
    with patch("pve_client.subprocess.run") as run:
        run.return_value = MagicMock(stdout=streamed)
        upid = _pvesh_create("/nodes/raspmox/vzdump", {"vmid": 100, "storage": "pbs-local"})
    assert upid == "UPID:raspmox:00073F90:1E82807A:6A208D79:vzdump:100:root@pam:"
    argv = run.call_args[0][0]
    assert argv[:3] == ["pvesh", "create", "/nodes/raspmox/vzdump"]
    assert "--vmid" in argv and "100" in argv


def test_pvesh_get_unquotes_path_for_task_upid():
    """wait_for_task URL-encodes the UPID into the path for the HTTP layer; the
    pvesh fallback must hand pvesh the RAW (unquoted) path or it 404s."""
    enc = "/nodes/raspmox/tasks/UPID%3Araspmox%3A001%3Avzdump%3A100%3Aroot%40pam%3A/status"
    with patch("pve_client.subprocess.run") as run:
        run.return_value = MagicMock(stdout='{"status":"stopped","exitstatus":"OK"}')
        out = _pvesh_get(enc)
    assert out == {"status": "stopped", "exitstatus": "OK"}
    argv = run.call_args[0][0]
    assert argv[2] == "/nodes/raspmox/tasks/UPID:raspmox:001:vzdump:100:root@pam:/status"


def test_post_falls_back_to_pvesh_on_connection_drop():
    """ARM PVE intermittently drops POSTs proxied to pvedaemon (RemoteDisconnected).
    PVEClient._post must fall back to pvesh; a real HTTP error (4xx) must NOT."""
    import requests
    c = pve_client.PVEClient.__new__(pve_client.PVEClient)  # skip __init__/auth
    c._base = "https://x:8006"
    c._session = MagicMock()
    c._session.post.side_effect = requests.exceptions.ConnectionError("RemoteDisconnected")
    with patch("pve_client._pvesh_create", return_value="UPID:fallback:") as pvesh:
        out = c._post("/nodes/n/vzdump", vmid=100)
    pvesh.assert_called_once()
    assert out == "UPID:fallback:"
