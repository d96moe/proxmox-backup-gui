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
BACKEND = Path(__file__).parent.parent.parent / "backend"
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
