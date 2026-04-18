"""Unit tests for MQTT-related agent functionality.

Tests cover the four systems added/changed in the MQTT-first rewrite:

  STORAGE_MERGE  — StatePoller._scan_storage preserves cloud keys
  RESTIC_COVERS  — _scan_pve_pbs annotates restic covers with local flag
  RESTIC_RESCAN  — _run_in_background triggers _scan_restic after restic backup
  BACKUP_PROGRESS — LocalResticClient.backup_datastore parses --json output
"""
from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── make backend importable ──────────────────────────────────────────────────
# Fast pipeline: repo/ci/tests/test_mqtt.py → parent*3 = repo root → backend/
# Integration CI: /opt/ci/proxmox-backup-gui/tests/test_mqtt.py → parent*2 → backend/
BACKEND = Path(__file__).parent.parent.parent / "backend"
if not (BACKEND / "pve_agent.py").exists():
    BACKEND = Path(__file__).parent.parent / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import pve_agent as ag


# ─────────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def agent_cfg():
    return ag.AgentConfig(
        pve_url="https://10.10.0.1:8006",
        pve_user="root@pam",
        pve_password="testpw",
        pbs_url="https://10.10.0.1:8007",
        pbs_user="backup@pbs",
        pbs_password="pbspw",
        pbs_datastore="ci-pbs",
        pbs_storage_id="pbs-local",
        pbs_datastore_path="/mnt/ci-pbs",
        pve_ssh_host="10.10.0.1",
        restic_repo="rclone:gdrive:test",
        restic_password="respw",
    )


def _make_poller(agent_cfg):
    mqtt_mock = MagicMock()
    mqtt_mock._base = "proxmox/gui-ci"
    mqtt_mock._client = MagicMock()
    mqtt_mock._ensure_discovery = MagicMock()
    return ag.StatePoller(agent_cfg, mqtt_mock), mqtt_mock


# ─────────────────────────────────────────────────────────────────────────────
# STORAGE_MERGE — _scan_storage must preserve cloud_used / cloud_total
# ─────────────────────────────────────────────────────────────────────────────

class TestStatePollerStorageMerge:
    """Root cause of cloud storage showing '—' on page reload:
    _local_storage = local overwrote cloud keys set by _update_cloud_storage.
    Fix: .update(local) + setdefault."""

    def test_cloud_keys_survive_local_pbs_scan(self, agent_cfg):
        """cloud_used must be preserved after a subsequent local PBS scan."""
        poller, _ = _make_poller(agent_cfg)
        poller._update_cloud_storage(5.0, 100.0, 50.0)
        assert poller._local_storage["cloud_used"] == 5.0

        local = {"local_used": 20.0, "local_total": 200.0, "dedup_factor": 3.5}
        with patch("pve_agent.PBSClient") as pbs_cls, \
             patch("pve_agent._host", return_value=MagicMock()):
            pbs_cls.return_value.get_storage_info.return_value = local
            poller._scan_storage()

        assert poller._local_storage["cloud_used"] == 5.0, \
            "_scan_storage() must not reset cloud_used to 0"
        assert poller._local_storage["cloud_total"] == 100.0

    def test_local_fields_updated_after_pbs_scan(self, agent_cfg):
        """local_used and local_total must be updated by _scan_storage."""
        poller, _ = _make_poller(agent_cfg)
        local = {"local_used": 42.0, "local_total": 420.0, "dedup_factor": 2.0}
        with patch("pve_agent.PBSClient") as pbs_cls, \
             patch("pve_agent._host", return_value=MagicMock()):
            pbs_cls.return_value.get_storage_info.return_value = local
            poller._scan_storage()

        assert poller._local_storage["local_used"] == 42.0
        assert poller._local_storage["local_total"] == 420.0

    def test_storage_mqtt_topic_contains_both_local_and_cloud(self, agent_cfg):
        """MQTT storage topic payload must contain local AND cloud fields."""
        poller, mqtt_mock = _make_poller(agent_cfg)
        poller._update_cloud_storage(3.0, 50.0, 25.0)

        published = {}
        mqtt_mock._client.publish.side_effect = (
            lambda topic, payload, **kw: published.update({topic: json.loads(payload)})
        )

        local = {"local_used": 10.0, "local_total": 100.0, "dedup_factor": 4.0}
        with patch("pve_agent.PBSClient") as pbs_cls, \
             patch("pve_agent._host", return_value=MagicMock()):
            pbs_cls.return_value.get_storage_info.return_value = local
            poller._scan_storage()

        storage_data = {k: v for k, v in published.items() if "storage" in k}
        assert storage_data, "No storage topic was published"
        data = next(iter(storage_data.values()))
        assert data["local_used"] == 10.0
        assert data["cloud_used"] == 3.0
        assert data["cloud_total"] == 50.0

    def test_no_cloud_keys_before_cloud_scan(self, agent_cfg):
        """First _scan_storage before cloud loop runs must not raise KeyError,
        and the published MQTT payload must include cloud_used=0 as default."""
        poller, mqtt_mock = _make_poller(agent_cfg)
        # No _update_cloud_storage called yet

        published = {}
        mqtt_mock._client.publish.side_effect = (
            lambda topic, payload, **kw: published.update({topic: json.loads(payload)})
        )

        local = {"local_used": 5.0, "local_total": 50.0, "dedup_factor": None}
        with patch("pve_agent.PBSClient") as pbs_cls, \
             patch("pve_agent._host", return_value=MagicMock()):
            pbs_cls.return_value.get_storage_info.return_value = local
            poller._scan_storage()   # must not raise

        storage_data = {k: v for k, v in published.items() if "storage" in k}
        assert storage_data, "storage topic must be published"
        data = next(iter(storage_data.values()))
        # cloud_used must default to 0 (not KeyError), cloud_total to None
        assert data.get("cloud_used") == 0
        assert data.get("cloud_total") is None

    def test_update_cloud_storage_overwrites_previous_cloud_value(self, agent_cfg):
        """Repeated _update_cloud_storage calls must update the value each time."""
        poller, _ = _make_poller(agent_cfg)
        poller._update_cloud_storage(1.0, 100.0, None)
        poller._update_cloud_storage(9.0, 100.0, None)
        assert poller._local_storage["cloud_used"] == 9.0


# ─────────────────────────────────────────────────────────────────────────────
# RESTIC_COVERS — _scan_pve_pbs annotates covers with local flag
# ─────────────────────────────────────────────────────────────────────────────

class TestStatePollerResticCovers:
    """covers[].local drives the VM card badge colour in the cloud tab.
    True = that PBS snapshot still exists locally; False = it was pruned."""

    def _run_scan(self, poller, pve_vms, pbs_groups_dict, restic_snaps):
        """Inject test data and call _scan_pve_pbs; return {suffix: data} published."""
        poller._restic_snaps = restic_snaps
        published = {}

        orig = poller._pub_if_changed
        def _spy(suffix, data):
            published[suffix] = data
            poller._hashes.pop(suffix, None)   # bypass dedup so spy always fires
            orig(suffix, data)

        pbs_list = [
            {"pve_id": int(vid), "snapshots": snaps}
            for vid, snaps in pbs_groups_dict.items()
        ]

        with patch.object(poller, "_pub_if_changed", side_effect=_spy), \
             patch("pve_agent.PVEClient") as pve_cls, \
             patch("pve_agent.PBSClient") as pbs_cls, \
             patch("pve_agent._host", return_value=MagicMock()):
            pve_cls.return_value.get_vms_and_lxcs.return_value = pve_vms
            pbs_cls.return_value.get_snapshots.return_value = pbs_list
            poller._scan_pve_pbs()

        return published

    def test_cover_local_true_when_pbs_snap_present(self, agent_cfg):
        """covers[].local=True when the covered PBS time is still in PBS."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc",
                         "status": "running", "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 1000, "size_bytes": 512}]}
        restic_snaps = [{
            "id": "abc123", "short_id": "abc1",
            "ts": 1050, "size_bytes": 1000,
            "covers": [{"vmid": 301, "pbs_time": 1000}],
        }]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        assert "vm/301/restic" in pub
        cover = pub["vm/301/restic"]["snapshots"][0]["covers"][0]
        assert cover["local"] is True

    def test_cover_local_false_when_pbs_snap_pruned(self, agent_cfg):
        """covers[].local=False when PBS snap T=1000 was pruned; current PBS has T=2000."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc",
                         "status": "running", "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 2000, "size_bytes": 512}]}
        restic_snaps = [{
            "id": "abc123", "short_id": "abc1",
            "ts": 1050, "size_bytes": 1000,
            "covers": [{"vmid": 301, "pbs_time": 1000}],
        }]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        cover = pub["vm/301/restic"]["snapshots"][0]["covers"][0]
        assert cover["local"] is False

    def test_covers_filtered_to_matching_vmid(self, agent_cfg):
        """vm/301/restic covers must only contain entries for vmid 301."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {
            301: {"name": "ct-301", "type": "lxc", "status": "running",
                  "os": "linux", "template": False},
            302: {"name": "ct-302", "type": "lxc", "status": "running",
                  "os": "linux", "template": False},
        }
        pbs_groups = {
            "301": [{"backup_time": 1000, "size_bytes": 512}],
            "302": [{"backup_time": 1100, "size_bytes": 512}],
        }
        restic_snaps = [{
            "id": "multi", "short_id": "mul1",
            "ts": 1200, "size_bytes": 2000,
            "covers": [
                {"vmid": 301, "pbs_time": 1000},
                {"vmid": 302, "pbs_time": 1100},
            ],
        }]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        for c in pub["vm/301/restic"]["snapshots"][0]["covers"]:
            assert c["vmid"] == 301, "vm/301/restic must only contain vmid=301 covers"
        for c in pub["vm/302/restic"]["snapshots"][0]["covers"]:
            assert c["vmid"] == 302, "vm/302/restic must only contain vmid=302 covers"

    def test_no_restic_topic_when_no_snaps_for_vm(self, agent_cfg):
        """vm/<id>/restic must NOT be published when there are no restic snaps for that VM."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc", "status": "running",
                         "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 1000, "size_bytes": 512}]}
        restic_snaps = []

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        assert "vm/301/restic" not in pub

    def test_pbs_snap_cloud_true_when_restic_covers_it(self, agent_cfg):
        """PBS snapshot.cloud must be True when a restic snap covers it."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc", "status": "running",
                         "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 1000, "size_bytes": 512}]}
        restic_snaps = [{
            "id": "r1", "short_id": "r1",
            "ts": 1050, "size_bytes": 500,
            "covers": [{"vmid": 301, "pbs_time": 1000}],
        }]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        snap = pub["vm/301/pbs"]["snapshots"][0]
        assert snap["cloud"] is True
        assert snap["restic_id"] == "r1"

    def test_pbs_snap_cloud_false_when_not_covered(self, agent_cfg):
        """PBS snapshot.cloud must be False when no restic snap covers it."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc", "status": "running",
                         "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 1000, "size_bytes": 512}]}
        restic_snaps = []

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        snap = pub["vm/301/pbs"]["snapshots"][0]
        assert snap["cloud"] is False
        assert snap["restic_id"] is None


# ─────────────────────────────────────────────────────────────────────────────
# RESTIC_RESCAN — _run_in_background must call _scan_restic for restic ops
# ─────────────────────────────────────────────────────────────────────────────

class TestResticRescanAfterBackup:
    """After restic backup: new snapshot must appear immediately via MQTT.
    Bug: rescan_now() only ran _scan_pve_pbs; _restic_snaps was stale.
    Fix: pass rescan_restic=True so _scan_restic() is called first."""

    def _wait(self, event, timeout=3.0):
        return event.wait(timeout=timeout)

    def test_rescan_restic_true_calls_scan_restic(self):
        """rescan_restic=True must call _scan_restic (not only rescan_now)."""
        poller = MagicMock()
        called = threading.Event()
        poller._scan_restic.side_effect = lambda: called.set()

        op = ag._new_op("backup", vmid=None)
        with patch("pve_agent._poller", poller), patch("pve_agent._mqtt", None):
            ag._run_in_background(op, lambda o: None, rescan_restic=True)

        assert self._wait(called), "_scan_restic() not called after restic backup"

    def test_rescan_restic_true_does_not_call_rescan_now_separately(self):
        """With rescan_restic=True, rescan_now must NOT be called additionally
        — _scan_restic already calls it internally."""
        poller = MagicMock()
        rescan_called = threading.Event()
        poller.rescan_now.side_effect = lambda: rescan_called.set()
        poller._scan_restic.side_effect = lambda: None

        op = ag._new_op("backup", vmid=None)
        with patch("pve_agent._poller", poller), patch("pve_agent._mqtt", None):
            ag._run_in_background(op, lambda o: None, rescan_restic=True)

        time.sleep(0.2)  # give background thread time to run
        assert not rescan_called.is_set(), \
            "rescan_now() must not be called separately when rescan_restic=True"

    def test_rescan_restic_false_calls_rescan_now(self):
        """Default (rescan_restic=False) must call rescan_now for PBS backup."""
        poller = MagicMock()
        called = threading.Event()
        poller.rescan_now.side_effect = lambda: called.set()

        op = ag._new_op("backup", vmid=301)
        with patch("pve_agent._poller", poller), patch("pve_agent._mqtt", None):
            ag._run_in_background(op, lambda o: None, rescan_restic=False)

        assert self._wait(called), "rescan_now() not called after PBS backup"

    def test_rescan_restic_false_does_not_call_scan_restic(self):
        """PBS backup must NOT trigger _scan_restic (slow restic operation)."""
        poller = MagicMock()
        scan_called = threading.Event()
        poller._scan_restic.side_effect = lambda: scan_called.set()
        poller.rescan_now.side_effect = lambda: None

        op = ag._new_op("backup", vmid=301)
        with patch("pve_agent._poller", poller), patch("pve_agent._mqtt", None):
            ag._run_in_background(op, lambda o: None, rescan_restic=False)

        time.sleep(0.2)
        assert not scan_called.is_set(), \
            "_scan_restic() must not run for PBS-only backup"

    def test_op_status_ok_when_fn_succeeds(self):
        """op.status must be 'ok' after a successful background function."""
        op = ag._new_op("backup", vmid=None)
        done = threading.Event()
        def _fn(o):
            pass

        with patch("pve_agent._poller", None), patch("pve_agent._mqtt", None):
            ag._run_in_background(op, _fn)

        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and op.status == "running":
            time.sleep(0.02)
        assert op.status == "ok"

    def test_op_status_failed_when_fn_raises(self):
        """op.status must be 'failed' and error logged when function raises."""
        op = ag._new_op("backup", vmid=None)
        def _fn(o):
            raise ValueError("restic exploded")

        with patch("pve_agent._poller", None), patch("pve_agent._mqtt", None):
            ag._run_in_background(op, _fn)

        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and op.status == "running":
            time.sleep(0.02)
        assert op.status == "failed"
        assert any("restic exploded" in l for l in op.log)


# ─────────────────────────────────────────────────────────────────────────────
# BACKUP_PROGRESS — LocalResticClient.backup_datastore JSON parsing
# ─────────────────────────────────────────────────────────────────────────────

class TestLocalResticBackupProgress:
    """backup_datastore() must parse restic --json output into:
    - human-readable log lines (not raw JSON)
    - progress_fn(pct, speed_mbps, eta_s) calls
    - clean summary line on 'message_type: summary'
    - passthrough for non-JSON lines"""

    def _make_client(self, agent_cfg):
        client = ag.LocalResticClient.__new__(ag.LocalResticClient)
        client._cfg = agent_cfg
        client._env = {}
        client._full_env = {}
        return client

    def _run(self, agent_cfg, stdout_lines):
        client = self._make_client(agent_cfg)
        logged = []
        progress_calls = []

        class _FakeProc:
            returncode = 0
            stdout = iter(l + "\n" for l in stdout_lines)
            def wait(self): pass

        with patch("pve_agent.subprocess.Popen", return_value=_FakeProc()), \
             patch("pve_agent.subprocess.run"):
            client.backup_datastore(
                "/mnt/pbs", logged.append,
                progress_fn=lambda pct, spd, eta: progress_calls.append((pct, spd, eta)),
            )

        return logged, progress_calls

    def test_status_10pct_produces_readable_log(self, agent_cfg):
        """A status line at exactly 10% must emit one formatted log line."""
        status = json.dumps({
            "message_type": "status",
            "percent_done": 0.10,
            "bytes_done": 100_000_000,
            "total_bytes": 1_000_000_000,
            "seconds_elapsed": 10,
            "seconds_remaining": 90,
        })
        logged, _ = self._run(agent_cfg, [status])

        readable = [l for l in logged if "%" in l and "MB" in l]
        assert readable, f"Expected formatted progress line, got: {logged}"
        assert "10%" in readable[0]

    def test_no_raw_json_in_log(self, agent_cfg):
        """status and summary lines must never appear as raw JSON in the log."""
        lines = [
            json.dumps({"message_type": "status", "percent_done": 0.5,
                        "bytes_done": 500_000_000, "total_bytes": 1_000_000_000,
                        "seconds_elapsed": 50, "seconds_remaining": 50}),
            json.dumps({"message_type": "summary", "files_new": 2,
                        "files_changed": 0, "data_added_packed": 2_000_000}),
        ]
        logged, _ = self._run(agent_cfg, lines)

        raw = [l for l in logged if l.strip().startswith("{")]
        assert not raw, f"Raw JSON must never appear in job log, got: {raw}"

    def test_progress_fn_called_for_every_status_line(self, agent_cfg):
        """progress_fn must be called for every status line — not only at 10% jumps."""
        statuses = [
            json.dumps({"message_type": "status",
                        "percent_done": p / 100,
                        "bytes_done": p * 10_000_000,
                        "total_bytes": 1_000_000_000,
                        "seconds_elapsed": max(p, 1),
                        "seconds_remaining": 100 - p})
            for p in range(0, 101, 5)
        ]
        _, calls = self._run(agent_cfg, statuses)

        assert len(calls) == len(statuses), \
            "progress_fn must be called once per status line"

    def test_progress_fn_receives_correct_pct(self, agent_cfg):
        """progress_fn pct arg must match percent_done * 100, rounded."""
        status = json.dumps({
            "message_type": "status",
            "percent_done": 0.42,
            "bytes_done": 420_000_000,
            "total_bytes": 1_000_000_000,
            "seconds_elapsed": 42,
            "seconds_remaining": 58,
        })
        _, calls = self._run(agent_cfg, [status])
        assert calls[0][0] == 42

    def test_progress_fn_receives_nonzero_speed(self, agent_cfg):
        """progress_fn speed_mbps must be > 0 when seconds_elapsed > 0."""
        status = json.dumps({
            "message_type": "status",
            "percent_done": 0.50,
            "bytes_done": 500_000_000,
            "total_bytes": 1_000_000_000,
            "seconds_elapsed": 25,
            "seconds_remaining": 25,
        })
        _, calls = self._run(agent_cfg, [status])
        assert calls[0][1] > 0, "speed_mbps must be > 0"

    def test_progress_fn_receives_eta(self, agent_cfg):
        """progress_fn eta_s arg must match seconds_remaining in status JSON."""
        status = json.dumps({
            "message_type": "status",
            "percent_done": 0.50,
            "bytes_done": 500_000_000,
            "total_bytes": 1_000_000_000,
            "seconds_elapsed": 25,
            "seconds_remaining": 37,
        })
        _, calls = self._run(agent_cfg, [status])
        assert calls[0][2] == 37

    def test_summary_emits_done_line(self, agent_cfg):
        """message_type summary must produce a 'Done — N file(s)' log line."""
        summary = json.dumps({
            "message_type": "summary",
            "files_new": 3,
            "files_changed": 2,
            "data_added_packed": 52_000_000,
        })
        logged, _ = self._run(agent_cfg, [summary])

        done = [l for l in logged if "Done" in l]
        assert done, f"Expected 'Done —' line, got: {logged}"
        assert "5 file" in done[0], \
            f"Should report files_new(3) + files_changed(2) = 5, got: {done[0]}"

    def test_non_json_line_passed_through(self, agent_cfg):
        """Non-JSON lines (warnings, plain text) must appear in log unchanged."""
        logged, _ = self._run(agent_cfg, ["WARNING: lock file stale"])
        assert "WARNING: lock file stale" in logged

    def test_only_10pct_jumps_produce_log_lines(self, agent_cfg):
        """Intermediate status lines (not at 10% boundary) must not add log spam.
        E.g. 10%, 11%, 12% → only 10% gets a log line; 20% is next."""
        statuses = [
            json.dumps({"message_type": "status",
                        "percent_done": p / 100,
                        "bytes_done": p * 10_000_000,
                        "total_bytes": 1_000_000_000,
                        "seconds_elapsed": max(p, 1),
                        "seconds_remaining": 100 - p})
            for p in range(10, 16)   # 10,11,12,13,14,15
        ]
        logged, _ = self._run(agent_cfg, statuses)

        pct_lines = [l for l in logged if "%" in l and "MB" in l]
        assert len(pct_lines) == 1, \
            f"Only the 10% line should be logged, got {len(pct_lines)}: {pct_lines}"
        assert "10%" in pct_lines[0]


# ─────────────────────────────────────────────────────────────────────────────
# PROD BUG 1: mqtt_hostname determines MQTT topic base
# Regression: Pi5 cabin agent had mqtt_hostname="raspmox" but hosts.json id="cabin"
# → GUI subscribed to proxmox/cabin/# but data was published to proxmox/raspmox/#
# → result: GUI stuck on "connecting" for cabin host
# ─────────────────────────────────────────────────────────────────────────────

class TestMQTTTopicBase:
    """MQTTPublisher._base must use mqtt_hostname, which must match hosts.json id."""

    def _make_pub(self, hostname):
        """Instantiate MQTTPublisher with paho mocked out so no real TCP connect happens.
        Works even when paho is not installed (stubs sys.modules['paho.*'])."""
        mock_client = MagicMock()
        mock_client.connect_async.return_value = None
        mock_client.loop_start.return_value = None
        mock_client.will_set.return_value = None

        fake_mqtt_mod = MagicMock()
        fake_mqtt_mod.Client.return_value = mock_client

        paho_stubs = {
            "paho": MagicMock(),
            "paho.mqtt": MagicMock(),
            "paho.mqtt.client": fake_mqtt_mod,
        }
        with patch.dict(sys.modules, paho_stubs):
            pub = ag.MQTTPublisher(
                host="localhost", port=1883, user="", password="",
                hostname=hostname,
            )
        return pub

    def test_mqtt_base_uses_configured_hostname(self):
        """MQTTPublisher topic base must be proxmox/<mqtt_hostname>, not socket.gethostname()."""
        pub = self._make_pub("cabin")
        assert pub._base == "proxmox/cabin", \
            f"Expected 'proxmox/cabin', got '{pub._base}'"
        assert pub._hostname == "cabin"

    def test_mqtt_base_falls_back_to_socket_hostname_when_empty(self):
        """When hostname='' MQTTPublisher must fall back to socket.gethostname()."""
        import socket
        pub = self._make_pub("")
        assert pub._base == f"proxmox/{socket.gethostname()}"

    def test_node_uses_mqtt_hostname_from_global_cfg(self, agent_cfg):
        """MQTTPublisher._node() reads global _cfg.mqtt_hostname.
        If this returns OS hostname, topic won't match hosts.json id → 'connecting'."""
        agent_cfg.mqtt_hostname = "cabin"
        pub = self._make_pub("cabin")
        with patch("pve_agent._cfg", agent_cfg):
            assert pub._node() == "cabin", \
                "_node() must return cfg.mqtt_hostname when set"

    def test_node_falls_back_to_socket_when_not_configured(self, agent_cfg):
        """_node() must fall back to socket.gethostname() when mqtt_hostname is empty."""
        import socket
        agent_cfg.mqtt_hostname = ""
        pub = self._make_pub("")
        with patch("pve_agent._cfg", agent_cfg):
            assert pub._node() == socket.gethostname()


# ─────────────────────────────────────────────────────────────────────────────
# PROD BUG 2: pruned restic snapshots must not appear in MQTT topics after rescan
# Regression: after restic prune, old snapshot IDs persisted in retained MQTT
# messages because _restic_snaps was replaced but only changed topics re-publish.
# ─────────────────────────────────────────────────────────────────────────────

class TestResticPruneRetained:
    """After restic prune, a vm/<id>/restic topic must no longer reference the
    pruned snapshot ID — even if the topic was previously published with it."""

    def _run_scan(self, poller, pve_vms, pbs_groups_dict, restic_snaps):
        poller._restic_snaps = restic_snaps
        published = {}

        orig = poller._pub_if_changed
        def _spy(suffix, data):
            published[suffix] = data
            poller._hashes.pop(suffix, None)
            orig(suffix, data)

        pbs_list = [
            {"pve_id": int(vid), "snapshots": snaps}
            for vid, snaps in pbs_groups_dict.items()
        ]

        with patch.object(poller, "_pub_if_changed", side_effect=_spy), \
             patch("pve_agent.PVEClient") as pve_cls, \
             patch("pve_agent.PBSClient") as pbs_cls, \
             patch("pve_agent._host", return_value=MagicMock()):
            pve_cls.return_value.get_vms_and_lxcs.return_value = pve_vms
            pbs_cls.return_value.get_snapshots.return_value = pbs_list
            poller._scan_pve_pbs()

        return published

    def test_pruned_snapshot_id_absent_after_rescan(self, agent_cfg):
        """After restic prune removes snap 'old1', vm/301/restic must not list it."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc",
                         "status": "running", "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 2000, "size_bytes": 512}]}

        # First scan: two snapshots
        two_snaps = [
            {"id": "old111", "short_id": "old1", "ts": 1000, "size_bytes": 100,
             "covers": [{"vmid": 301, "pbs_time": 1000}]},
            {"id": "new222", "short_id": "new2", "ts": 2050, "size_bytes": 200,
             "covers": [{"vmid": 301, "pbs_time": 2000}]},
        ]
        pub1 = self._run_scan(poller, pve_vms, pbs_groups, two_snaps)
        snap_ids_before = [s["short_id"] for s in pub1["vm/301/restic"]["snapshots"]]
        assert "old1" in snap_ids_before

        # After prune: old1 is gone
        one_snap = [
            {"id": "new222", "short_id": "new2", "ts": 2050, "size_bytes": 200,
             "covers": [{"vmid": 301, "pbs_time": 2000}]},
        ]
        pub2 = self._run_scan(poller, pve_vms, pbs_groups, one_snap)
        snap_ids_after = [s["short_id"] for s in pub2["vm/301/restic"]["snapshots"]]
        assert "old1" not in snap_ids_after, \
            "Pruned snapshot 'old1' must not appear in vm/301/restic after rescan"
        assert "new2" in snap_ids_after

    def test_pbs_restic_id_updated_after_newer_restic_snap(self, agent_cfg):
        """After a new restic snapshot covers the same PBS time, pbs.restic_id
        must switch to the new short_id (newest-wins rule in _build_restic_index)."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc",
                         "status": "running", "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 1000, "size_bytes": 512}]}

        # First scan: pbs_time=1000 covered by 'old111'
        pub1 = self._run_scan(poller, pve_vms, pbs_groups, [
            {"id": "old111", "short_id": "old1", "ts": 900, "size_bytes": 100,
             "covers": [{"vmid": 301, "pbs_time": 1000}]},
        ])
        # restic_id = full 64-char id (for API); restic_short_id = 8-char display
        assert pub1["vm/301/pbs"]["snapshots"][0]["restic_id"] == "old111"

        # Second scan: newer restic snap also covers pbs_time=1000
        pub2 = self._run_scan(poller, pve_vms, pbs_groups, [
            {"id": "new222", "short_id": "new2", "ts": 2000, "size_bytes": 200,
             "covers": [{"vmid": 301, "pbs_time": 1000}]},
            {"id": "old111", "short_id": "old1", "ts": 900, "size_bytes": 100,
             "covers": [{"vmid": 301, "pbs_time": 1000}]},
        ])
        assert pub2["vm/301/pbs"]["snapshots"][0]["restic_id"] == "new222", \
            "Newest restic snap must win in _build_restic_index (newest-first list)"


# ─────────────────────────────────────────────────────────────────────────────
# PROD BUG 3: restic_id / restic_short_id separation in vm/pbs topic
# restic_id  = full 64-char hash → used for API delete/restore calls
# restic_short_id = 8-char display id → shown in UI
# Regression: cloud tab badge matching broke when these were swapped
# ─────────────────────────────────────────────────────────────────────────────

class TestResticIdFormat:
    """vm/pbs snapshots must carry both restic_id (full hash) and restic_short_id
    (8-char) so the UI can display the short form while the API uses the full hash."""

    def _run_scan(self, poller, pve_vms, pbs_groups_dict, restic_snaps):
        poller._restic_snaps = restic_snaps
        published = {}
        orig = poller._pub_if_changed
        def _spy(suffix, data):
            published[suffix] = data
            poller._hashes.pop(suffix, None)
            orig(suffix, data)
        pbs_list = [
            {"pve_id": int(vid), "snapshots": snaps}
            for vid, snaps in pbs_groups_dict.items()
        ]
        with patch.object(poller, "_pub_if_changed", side_effect=_spy), \
             patch("pve_agent.PVEClient") as pve_cls, \
             patch("pve_agent.PBSClient") as pbs_cls, \
             patch("pve_agent._host", return_value=MagicMock()):
            pve_cls.return_value.get_vms_and_lxcs.return_value = pve_vms
            pbs_cls.return_value.get_snapshots.return_value = pbs_list
            poller._scan_pve_pbs()
        return published

    def test_pbs_restic_id_is_full_hash(self, agent_cfg):
        """vm/pbs snapshots[].restic_id must be the full 64-char hash (for API calls)."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc",
                         "status": "running", "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 1000, "size_bytes": 512}]}
        full_id = "dd1cb644c1d3f2ee49a93dfb220c30c9519b98e0a1626e3c218cc378ac429e71"
        restic_snaps = [{
            "id": full_id, "short_id": "dd1cb644",
            "ts": 1050, "size_bytes": 500,
            "covers": [{"vmid": 301, "pbs_time": 1000}],
        }]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        snap = pub["vm/301/pbs"]["snapshots"][0]
        assert snap["restic_id"] == full_id, \
            f"restic_id must be full 64-char hash, got '{snap['restic_id']}'"
        assert snap["restic_short_id"] == "dd1cb644", \
            f"restic_short_id must be '8-char', got '{snap['restic_short_id']}'"

    def test_restic_topic_carries_both_id_fields(self, agent_cfg):
        """vm/<id>/restic snapshots must carry both id (full) and short_id fields."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc",
                         "status": "running", "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 1000, "size_bytes": 512}]}
        full_id = "abcdef1234567890" * 4  # 64 chars
        restic_snaps = [{
            "id": full_id, "short_id": "abcdef12",
            "ts": 1050, "size_bytes": 500,
            "covers": [{"vmid": 301, "pbs_time": 1000}],
        }]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        snap = pub["vm/301/restic"]["snapshots"][0]
        assert snap.get("id") == full_id, "full id must be present in restic topic"
        assert snap.get("short_id") == "abcdef12", "short_id must be present in restic topic"


# ─────────────────────────────────────────────────────────────────────────────
# PROD BUG 4: cloud-only snapshots missing from Backups tab
# Regression: PBS snapshots pruned locally but still in restic were invisible.
# _scan_pve_pbs only iterated local PBS snaps — no entry created for pruned times.
# Fix: after processing local snaps, iterate vm_restic and add cloud-only entries
# for any pbs_time not in local_times.
# ─────────────────────────────────────────────────────────────────────────────

class TestCloudOnlySnapshots:
    """vm/<id>/pbs topic must include entries with local=False, cloud=True for
    PBS times that were pruned locally but still exist in restic."""

    def _run_scan(self, poller, pve_vms, pbs_groups_dict, restic_snaps):
        poller._restic_snaps = restic_snaps
        published = {}
        orig = poller._pub_if_changed
        def _spy(suffix, data):
            published[suffix] = data
            poller._hashes.pop(suffix, None)
            orig(suffix, data)
        pbs_list = [
            {"pve_id": int(vid), "snapshots": snaps}
            for vid, snaps in pbs_groups_dict.items()
        ]
        with patch.object(poller, "_pub_if_changed", side_effect=_spy), \
             patch("pve_agent.PVEClient") as pve_cls, \
             patch("pve_agent.PBSClient") as pbs_cls, \
             patch("pve_agent._host", return_value=MagicMock()):
            pve_cls.return_value.get_vms_and_lxcs.return_value = pve_vms
            pbs_cls.return_value.get_snapshots.return_value = pbs_list
            poller._scan_pve_pbs()
        return published

    def test_cloud_only_entry_appears_when_pbs_pruned(self, agent_cfg):
        """A restic snap covering a pruned PBS time must appear in vm/pbs as
        local=False, cloud=True so the Backups tab shows it."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc",
                         "status": "running", "os": "linux", "template": False}}
        # Only PBS time=2000 exists locally; time=1000 was pruned
        pbs_groups = {"301": [{"backup_time": 2000, "size_bytes": 512}]}
        restic_snaps = [{
            "id": "cloud1", "short_id": "c1",
            "ts": 1050, "size_bytes": 800,
            "covers": [{"vmid": 301, "pbs_time": 1000}],
        }]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        snaps = pub["vm/301/pbs"]["snapshots"]
        cloud_only = [s for s in snaps if s.get("backup_time") == 1000]
        assert cloud_only, "Cloud-only entry for pruned pbs_time=1000 must appear in vm/pbs"
        entry = cloud_only[0]
        assert entry["local"] is False, "cloud-only entry must have local=False"
        assert entry["cloud"] is True,  "cloud-only entry must have cloud=True"
        assert entry["restic_id"] == "cloud1", "cloud-only restic_id must be full hash"
        assert entry["restic_short_id"] == "c1"

    def test_local_snap_has_local_true(self, agent_cfg):
        """Existing local PBS snap must have local=True (sanity check for the parallel path)."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc",
                         "status": "running", "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 2000, "size_bytes": 512}]}
        restic_snaps = [{
            "id": "r1", "short_id": "r1sh",
            "ts": 2050, "size_bytes": 900,
            "covers": [{"vmid": 301, "pbs_time": 2000}],
        }]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        local_snaps = [s for s in pub["vm/301/pbs"]["snapshots"]
                       if s.get("backup_time") == 2000]
        assert local_snaps, "Local PBS snap must appear in vm/pbs"
        assert local_snaps[0]["local"] is True
        assert local_snaps[0]["cloud"] is True

    def test_no_duplicate_cloud_only_when_multiple_restic_cover_same_pbs_time(self, agent_cfg):
        """If two restic snaps cover the same pruned PBS time, only ONE cloud-only
        entry must appear (newest restic wins)."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc",
                         "status": "running", "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 3000, "size_bytes": 512}]}
        # Two restic snaps both cover pruned pbs_time=1000
        restic_snaps = [
            {"id": "newer_id", "short_id": "newer", "ts": 2000, "size_bytes": 900,
             "covers": [{"vmid": 301, "pbs_time": 1000}]},
            {"id": "older_id", "short_id": "older", "ts": 900, "size_bytes": 500,
             "covers": [{"vmid": 301, "pbs_time": 1000}]},
        ]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        cloud_entries = [s for s in pub["vm/301/pbs"]["snapshots"]
                         if s.get("backup_time") == 1000]
        assert len(cloud_entries) == 1, \
            f"Must be exactly 1 cloud-only entry for pbs_time=1000, got {len(cloud_entries)}"
        # newest restic snap wins (_build_restic_index is sorted newest-first)
        assert cloud_entries[0]["restic_id"] == "newer_id"

    def test_cloud_only_entries_sorted_by_backup_time_newest_first(self, agent_cfg):
        """vm/pbs snapshots must be sorted newest→oldest regardless of whether
        entries are local or cloud-only."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc",
                         "status": "running", "os": "linux", "template": False}}
        # Local: time=3000; cloud-only: time=1000, time=2000
        pbs_groups = {"301": [{"backup_time": 3000, "size_bytes": 512}]}
        restic_snaps = [
            {"id": "r1", "short_id": "r1s", "ts": 1050, "size_bytes": 500,
             "covers": [{"vmid": 301, "pbs_time": 1000}]},
            {"id": "r2", "short_id": "r2s", "ts": 2050, "size_bytes": 600,
             "covers": [{"vmid": 301, "pbs_time": 2000}]},
        ]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        times = [s["backup_time"] for s in pub["vm/301/pbs"]["snapshots"]]
        assert times == sorted(times, reverse=True), \
            f"Snapshots must be sorted newest-first, got order: {times}"


# ─────────────────────────────────────────────────────────────────────────────
# PROD BUG 5: cloud-only entries missing date; restic covers missing pbs_date
# Regression: date="undefined" shown in Backups tab for cloud-only snaps;
# no hover tooltip on VM badges in Cloud tab (pbs_date was always empty).
# Fix: _scan_pve_pbs must compute date from backup_time for cloud-only entries,
# and pbs_date from pbs_time for each restic cover.
# ─────────────────────────────────────────────────────────────────────────────

class TestTimestampFields:
    """date and pbs_date fields must be present so the UI can render them."""

    def _run_scan(self, poller, pve_vms, pbs_groups_dict, restic_snaps):
        poller._restic_snaps = restic_snaps
        published = {}
        orig = poller._pub_if_changed
        def _spy(suffix, data):
            published[suffix] = data
            poller._hashes.pop(suffix, None)
            orig(suffix, data)
        pbs_list = [
            {"pve_id": int(vid), "snapshots": snaps}
            for vid, snaps in pbs_groups_dict.items()
        ]
        with patch.object(poller, "_pub_if_changed", side_effect=_spy), \
             patch("pve_agent.PVEClient") as pve_cls, \
             patch("pve_agent.PBSClient") as pbs_cls, \
             patch("pve_agent._host", return_value=MagicMock()):
            pve_cls.return_value.get_vms_and_lxcs.return_value = pve_vms
            pbs_cls.return_value.get_snapshots.return_value = pbs_list
            poller._scan_pve_pbs()
        return published

    def test_cloud_only_entry_has_date_field(self, agent_cfg):
        """Cloud-only pbs entries must include a 'date' string from backup_time.
        Bug: date field was missing → UI showed 'undefined'."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc",
                         "status": "running", "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 2000, "size_bytes": 512}]}
        restic_snaps = [{
            "id": "r1", "short_id": "r1s", "ts": 1050, "size_bytes": 500,
            "covers": [{"vmid": 301, "pbs_time": 1000}],
        }]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        cloud_only = [s for s in pub["vm/301/pbs"]["snapshots"]
                      if s.get("backup_time") == 1000]
        assert cloud_only, "Cloud-only entry must exist"
        assert "date" in cloud_only[0], "Cloud-only entry must have 'date' field"
        assert cloud_only[0]["date"] not in (None, "", "undefined"), \
            f"date must be a non-empty string, got: {cloud_only[0].get('date')!r}"
        # Verify it's a sane timestamp string
        assert "1970" in cloud_only[0]["date"] or "-" in cloud_only[0]["date"], \
            f"date must look like a timestamp, got: {cloud_only[0]['date']!r}"

    def test_restic_cover_has_pbs_date_field(self, agent_cfg):
        """Restic covers must include pbs_date string for hover tooltip.
        Bug: pbs_date was never set → hover on VM badge in Cloud tab was empty."""
        poller, _ = _make_poller(agent_cfg)
        pve_vms = {301: {"name": "ct-301", "type": "lxc",
                         "status": "running", "os": "linux", "template": False}}
        pbs_groups = {"301": [{"backup_time": 1000, "size_bytes": 512}]}
        restic_snaps = [{
            "id": "r1", "short_id": "r1s", "ts": 1050, "size_bytes": 500,
            "covers": [{"vmid": 301, "pbs_time": 1000}],
        }]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        covers = pub["vm/301/restic"]["snapshots"][0]["covers"]
        assert covers, "Covers must not be empty"
        cover = covers[0]
        assert "pbs_date" in cover, "Cover must have 'pbs_date' field for tooltip"
        assert cover["pbs_date"] not in (None, "", "undefined"), \
            f"pbs_date must be a non-empty string, got: {cover.get('pbs_date')!r}"


# ─────────────────────────────────────────────────────────────────────────────
# PROD BUG 6: VM deleted from PVE+PBS but still in restic disappears from UI
# Root cause: all_vmids = pve_meta | pbs_groups — restic-only VMs excluded.
# Fix: all_vmids also unions restic_by_vm_pbstime keys so cloud-only VMs
# remain visible with local=False, cloud=True.
# ─────────────────────────────────────────────────────────────────────────────

class TestResticOnlyVmVisible:
    """A VM that no longer exists in PVE or PBS but has restic snapshots must
    still appear in published MQTT data as a cloud-only entry."""

    def _run_scan(self, poller, pve_vms, pbs_groups_dict, restic_snaps):
        poller._restic_snaps = restic_snaps
        published = {}
        orig = poller._pub_if_changed
        def _spy(suffix, data):
            published[suffix] = data
            poller._hashes.pop(suffix, None)
            orig(suffix, data)
        pbs_list = [
            {"pve_id": int(vid), "snapshots": snaps}
            for vid, snaps in pbs_groups_dict.items()
        ]
        with patch.object(poller, "_pub_if_changed", side_effect=_spy), \
             patch("pve_agent.PVEClient") as pve_cls, \
             patch("pve_agent.PBSClient") as pbs_cls, \
             patch("pve_agent._host", return_value=MagicMock()):
            pve_cls.return_value.get_vms_and_lxcs.return_value = pve_vms
            pbs_cls.return_value.get_snapshots.return_value = pbs_list
            pbs_cls.return_value.get_storage_info.return_value = {
                "local_used": 0, "local_total": 100, "dedup_factor": 1.0}
            poller._scan_pve_pbs()
        return published

    def test_restic_only_vm_is_published(self, agent_cfg):
        """VM 9131 exists only in restic (deleted from PVE and PBS).
        Bug: all_vmids never included it → vm/9131/pbs was never published →
        MQTT broker kept stale local+cloud retained message forever."""
        poller, _ = _make_poller(agent_cfg)
        # PVE: vm-9131 is gone
        pve_vms = {}
        # PBS: no snapshot for 9131
        pbs_groups = {}
        # Restic: snapshot still exists for vm-9131
        restic_snaps = [{
            "id":       "dd1cb644c1d3f2ee49a93dfb220c30c9519b98e0a1626e3c218cc378ac429e71",
            "short_id": "dd1cb644",
            "ts": 1776211720, "size_bytes": 17179870154,
            "covers": [{"vmid": 9131, "pbs_time": 1776211720}],
        }]

        pub = self._run_scan(poller, pve_vms, pbs_groups, restic_snaps)

        assert "vm/9131/pbs" in pub, \
            "vm/9131/pbs must be published even when VM is absent from PVE and PBS"
        snaps = pub["vm/9131/pbs"]["snapshots"]
        assert len(snaps) == 1, f"Expected 1 cloud-only snapshot, got {len(snaps)}"
        s = snaps[0]
        assert s["local"] is False, "Snapshot must be local=False (not in PBS)"
        assert s["cloud"] is True,  "Snapshot must be cloud=True (in restic)"

    def test_restic_only_vm_has_no_local_snapshots(self, agent_cfg):
        """Cloud-only VM must have zero local=True snapshots."""
        poller, _ = _make_poller(agent_cfg)
        restic_snaps = [{
            "id": "aabbccdd" * 8, "short_id": "aabbccdd",
            "ts": 1000, "size_bytes": 100,
            "covers": [{"vmid": 9131, "pbs_time": 1000}],
        }]
        pub = self._run_scan(poller, {}, {}, restic_snaps)

        local_snaps = [s for s in pub.get("vm/9131/pbs", {}).get("snapshots", [])
                       if s.get("local")]
        assert not local_snaps, \
            f"Restic-only VM must have no local snapshots, got: {local_snaps}"


# ─────────────────────────────────────────────────────────────────────────────
# ACK_RETAIN — job/ack must be published with retain=False
# ─────────────────────────────────────────────────────────────────────────────

class TestAckRetain:
    """Root cause of job-modal opening on every new page load:
    _ack() published with retain=True, so every browser session received all
    accumulated ack messages from prior jobs and opened the job modal.
    Fix: retain=False — ack is a one-shot signal, not persistent state."""

    def test_ack_published_with_retain_false(self):
        """_ack() must never publish with retain=True."""
        client_mock = MagicMock()
        mqtt_pub = ag.MQTTPublisher.__new__(ag.MQTTPublisher)
        mqtt_pub._client = client_mock
        mqtt_pub._base = "proxmox/home"

        mqtt_pub._ack("corr123", "op456")

        call_kwargs = client_mock.publish.call_args
        assert call_kwargs is not None, "_ack() did not call client.publish"
        # retain must be False (either as positional or keyword arg)
        kwargs = call_kwargs.kwargs
        args   = call_kwargs.args
        retain = kwargs.get("retain", args[3] if len(args) > 3 else None)
        assert retain is False, \
            f"job/ack must be published with retain=False to avoid stale modal on reconnect, got retain={retain!r}"

    def test_ack_topic_contains_corr_id(self):
        """_ack() must publish to the job/<corr_id>/ack topic."""
        client_mock = MagicMock()
        mqtt_pub = ag.MQTTPublisher.__new__(ag.MQTTPublisher)
        mqtt_pub._client = client_mock
        mqtt_pub._base = "proxmox/home"

        mqtt_pub._ack("myCorr", "myOp")

        topic = client_mock.publish.call_args.args[0]
        assert topic == "proxmox/home/job/myCorr/ack", \
            f"Unexpected ack topic: {topic!r}"

    def test_ack_payload_contains_op_id(self):
        """_ack() payload must be JSON with op_id field."""
        client_mock = MagicMock()
        mqtt_pub = ag.MQTTPublisher.__new__(ag.MQTTPublisher)
        mqtt_pub._client = client_mock
        mqtt_pub._base = "proxmox/home"

        mqtt_pub._ack("c1", "op999")

        payload = json.loads(client_mock.publish.call_args.args[1])
        assert payload.get("op_id") == "op999", \
            f"ack payload must contain op_id, got: {payload!r}"


# ─────────────────────────────────────────────────────────────────────────────
# STALE_VMID_CLEANUP — disappeared VMIDs must have retained topics cleared
# ─────────────────────────────────────────────────────────────────────────────

class TestStaleVmidCleanup:
    """Root cause of ghost VMs in GUI after CI builds destroy cloned VMs:
    The production pve-agent publishes retained topics for CI VMs while they
    exist, but stale topics linger after the VM is destroyed if cleanup fails.
    Fix: StatePoller detects gone VMIDs (prev - all_vmids) and publishes empty
    retained payloads to clear them from the broker."""

    def _run_scan(self, poller, pve_vms, pbs_groups_dict, restic_snaps=None):
        """Run _scan_pve_pbs and return all publish() calls as list of (topic, payload, kwargs)."""
        poller._restic_snaps = restic_snaps or []
        # _bootstrap_vmids must be None (not a MagicMock) so the set-union in
        # the cleanup path works correctly: set | MagicMock returns a MagicMock
        # whose iteration yields nothing, silently skipping all cleanup.
        poller._mqtt._bootstrap_vmids = None
        calls = []

        pbs_list = [
            {"pve_id": int(vid), "snapshots": snaps}
            for vid, snaps in pbs_groups_dict.items()
        ]

        with patch("pve_agent.PVEClient") as pve_cls, \
             patch("pve_agent.PBSClient") as pbs_cls, \
             patch("pve_agent._host", return_value=MagicMock()):
            pve_cls.return_value.get_vms_and_lxcs.return_value = pve_vms
            pbs_cls.return_value.get_snapshots.return_value = pbs_list
            poller._mqtt._client.publish.side_effect = (
                lambda topic, payload=b"", **kw: calls.append((topic, payload, kw))
            )
            poller._scan_pve_pbs()

        return calls

    def _vm(self, vmid, name="ci-clone", type="vm", status="stopped"):
        """Return a pve_meta-compatible dict entry (vmid→dict)."""
        return {vmid: {"name": name, "type": type, "status": status,
                       "os": "linux", "template": False}}

    def test_disappeared_vmid_topics_cleared(self, agent_cfg):
        """When a VMID present in the previous scan vanishes, all its retained
        topics must be cleared by publishing empty payloads with retain=True."""
        poller, _ = _make_poller(agent_cfg)
        # First scan: VM 9100 is present
        self._run_scan(poller, self._vm(9100), {"9100": []})
        assert "9100" in poller._known_vmids, "9100 must be in known_vmids after first scan"

        # Second scan: VM 9100 is gone (CI build destroyed the clone)
        calls = self._run_scan(poller, {}, {})

        cleared = [t for t, p, kw in calls if "9100" in t and p == b""]
        assert cleared, \
            "No retained topics were cleared for disappeared VMID 9100"
        expected_suffixes = ["meta", "pbs", "restic", "backup/status",
                             "backup/last_ok", "backup/progress"]
        for suffix in expected_suffixes:
            assert any(f"vm/9100/{suffix}" in t for t in cleared), \
                f"vm/9100/{suffix} was not cleared after VMID disappeared"

    def test_disappeared_vmid_cleared_with_retain_true(self, agent_cfg):
        """Clearing a stale retained topic requires publishing with retain=True
        and an empty payload — without retain=True the broker keeps the old message."""
        poller, _ = _make_poller(agent_cfg)
        self._run_scan(poller, self._vm(9101, name="ci-clone-2"), {"9101": []})

        calls = self._run_scan(poller, {}, {})

        for topic, payload, kw in calls:
            if "9101" in topic and payload == b"":
                assert kw.get("retain") is True, \
                    f"Clearing {topic} must use retain=True, got {kw!r}"
                return
        pytest.fail("No clearing publish found for disappeared VMID 9101")

    def test_active_vmids_not_cleared(self, agent_cfg):
        """VMIDs still present in PVE must not have their topics cleared."""
        poller, _ = _make_poller(agent_cfg)
        pve_vm_101 = self._vm(101, name="vm-101", status="running")
        self._run_scan(poller, pve_vm_101, {})

        calls = self._run_scan(poller, pve_vm_101, {})

        empty_for_101 = [t for t, p, kw in calls if "vm/101" in t and p == b""]
        assert not empty_for_101, \
            f"Active VMID 101 must not be cleared, but got: {empty_for_101}"
