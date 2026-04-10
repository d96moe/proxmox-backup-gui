"""Unit tests for proxmox-backup-gui backend.

Tests cover every change and corner case found during manual testing:

  DETECT_SELF  — _detect_self_vmid() cgroupv1 / cgroupv2 / bare-metal
  FMT          — _fmt_bytes helper
  PBS_IDEM     — delete_snapshot 400/404 treated as success, 500 re-raised
  CLOUD_ONLY   — cloud-only detection (dedup, multi-restic, correct timestamp)
  STEP_FORMAT  — every job type emits "Step N/M —" log lines
  ENDPOINT     — 400 / 404 / 409 for new and existing endpoints
  COVERAGE     — /restic/snapshots local-coverage annotation
  DELETE_CLOUD — step 4 forgets ALL restic IDs that carry the deleted pbs_time
"""
from __future__ import annotations

import json
import sys
import time
import threading
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, call

import pytest
import requests

# ── make backend importable without installing it ────────────────────────────
BACKEND = Path(__file__).parent.parent.parent / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

# ── import app (module-level HOSTS and SELF_VMID run here) ───────────────────
import app as _app

# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers / constants
# ─────────────────────────────────────────────────────────────────────────────

T1 = 1000   # "gone" PBS snapshot unix timestamp
T2 = 1100   # "current" local PBS snapshot unix timestamp
TS_R1 = 900   # restic snapshot 1 creation time  (covers T1 only)
TS_R2 = 1200  # restic snapshot 2 creation time  (covers T1 + T2)
TS_R3 = 1400  # restic snapshot 3 creation time  (covers T1 + T2, newest)
HOST_ID = "testhost"


def _pbs_snap(backup_time: int) -> dict:
    return {
        "backup_time": backup_time,
        "date": "2023-01-01 00:00",
        "size_bytes": 1024,
        "size": "1 KB",
        "incremental": True,
        "local": True,
        "cloud": False,
    }


def _pbs_group(vmid: int, btype: str, snaps: list) -> dict:
    return {"pve_id": vmid, "backup_type": btype, "snapshots": snaps}


def _storage() -> dict:
    return {"local_used": 1.0, "local_total": 10.0, "dedup_factor": None}


def _pve_meta(vmid: int = 301, vtype: str = "lxc") -> dict:
    return {vmid: {
        "name": f"ct-{vmid}", "type": vtype,
        "os": "linux", "status": "running", "template": False,
    }}


@pytest.fixture
def mock_host():
    from config import HostConfig
    return HostConfig(
        id=HOST_ID, label="Test",
        pve_url="https://1.2.3.4:8006", pve_user="root@pam", pve_password="x",
        pbs_url="https://1.2.3.4:8007", pbs_user="backup@pbs", pbs_password="x",
        pbs_datastore="test-store",
        pbs_storage_id="pbs-local",
        pbs_datastore_path="/mnt/pbs",
        restic_repo="rclone:gdrive:test",
        restic_password="x",
    )


@pytest.fixture
def flask_client():
    _app._cache.clear()
    with _app.app.test_client() as c:
        yield c
    _app._cache.clear()


@contextmanager
def _mock_clients(mock_host, pbs_snaps, pve_meta, restic_by_vm, untagged=None):
    """Patch all three backend clients for /api/host/<id>/items."""
    _app._cache.clear()
    with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
         patch("app.PBSClient") as pbs_cls, \
         patch("app.PVEClient") as pve_cls, \
         patch("app.ResticClient") as restic_cls:
        pbs_mock  = pbs_cls.return_value
        pve_mock  = pve_cls.return_value
        res_mock  = restic_cls.return_value
        pbs_mock.get_storage_info.return_value = _storage()
        pbs_mock.get_snapshots.return_value = pbs_snaps
        pve_mock.get_vms_and_lxcs.return_value = pve_meta
        res_mock.is_running.return_value = False
        res_mock.get_snapshots_by_vm.return_value = (restic_by_vm, untagged or [])
        yield pbs_mock, pve_mock, res_mock


def _poll(flask_client, job_id: str, timeout: float = 5.0) -> dict:
    """Poll job endpoint until done/error or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = flask_client.get(f"/api/job/{job_id}")
        job = json.loads(resp.data)
        if job["status"] in ("done", "error"):
            return job
        time.sleep(0.05)
    raise TimeoutError(f"job {job_id} did not finish within {timeout}s")


def _step_lines(job: dict) -> list[str]:
    return [l for l in job.get("logs", []) if l.startswith("Step ")]


# ─────────────────────────────────────────────────────────────────────────────
# DETECT_SELF — _detect_self_vmid()
# ─────────────────────────────────────────────────────────────────────────────

class TestDetectSelf:

    def test_cgroupv1_returns_vmid(self):
        """cgroupv1: /proc/1/cgroup line contains /lxc/<vmid>."""
        from app import _detect_self_vmid
        cgroup_data = "11:devices:/lxc/300\n0::/\n"
        with patch("builtins.open", side_effect=lambda p, *a, **kw:
                   __import__("io").StringIO(cgroup_data) if "cgroup" in str(p) else open(p, *a, **kw)):
            result = _detect_self_vmid()
        assert result == 300

    def test_cgroupv1_no_lxc_returns_none(self):
        """cgroupv1 without /lxc/ → not in a container → None."""
        from app import _detect_self_vmid
        cgroup_data = "0::/user.slice\n"
        with patch("builtins.open", side_effect=lambda p, *a, **kw:
                   __import__("io").StringIO(cgroup_data) if "cgroup" in str(p) else open(p, *a, **kw)):
            result = _detect_self_vmid()
        assert result is None

    def test_cgroupv2_matches_by_mac(self):
        """cgroupv2: cgroup=/.lxc, MAC of eth0 matches PVE LXC config → correct VMID."""
        from app import _detect_self_vmid
        from config import HostConfig

        test_mac = "BC:24:11:62:D6:5A"
        test_mac_no_colon = test_mac.replace(":", "")

        fake_host = HostConfig(
            id="x", label="x",
            pve_url="https://1.2.3.4:8006", pve_user="root@pam", pve_password="x",
            pbs_url="https://1.2.3.4:8007", pbs_user="b@pbs", pbs_password="x",
            pbs_datastore="ds",
        )

        mock_pve = MagicMock()
        # /nodes → [{"node": "pve"}]
        # /nodes/pve/lxc → [{"vmid": 42}]
        # /nodes/pve/lxc/42/config → {"net0": f"...,hwaddr={test_mac},..."}
        mock_pve._get.side_effect = [
            [{"node": "pve"}],
            [{"vmid": 42}],
            {"net0": f"name=eth0,bridge=vmbr0,hwaddr={test_mac},ip=10.0.0.1/24"},
        ]

        def _open_side(p, *a, **kw):
            import io
            if "cgroup" in str(p):
                return io.StringIO("0::/.lxc\n")
            if "environ" in str(p):
                return io.BytesIO(b"container=lxc\x00OTHER=val")
            raise FileNotFoundError(p)

        with patch("builtins.open", side_effect=_open_side), \
             patch("pathlib.Path.read_text", return_value=test_mac.lower() + "\n"), \
             patch("app.load_hosts", return_value=[fake_host]), \
             patch("app.PVEClient", return_value=mock_pve):
            result = _detect_self_vmid()

        assert result == 42

    def test_cgroupv2_no_match_returns_none(self):
        """cgroupv2 path taken but MAC doesn't match any LXC → None."""
        from app import _detect_self_vmid
        from config import HostConfig

        fake_host = HostConfig(
            id="x", label="x",
            pve_url="https://1.2.3.4:8006", pve_user="root@pam", pve_password="x",
            pbs_url="https://1.2.3.4:8007", pbs_user="b@pbs", pbs_password="x",
            pbs_datastore="ds",
        )

        mock_pve = MagicMock()
        mock_pve._get.side_effect = [
            [{"node": "pve"}],
            [{"vmid": 99}],
            {"net0": "name=eth0,bridge=vmbr0,hwaddr=AA:BB:CC:DD:EE:FF"},
        ]

        def _open_side(p, *a, **kw):
            import io
            if "cgroup" in str(p):
                return io.StringIO("0::/.lxc\n")
            if "environ" in str(p):
                return io.BytesIO(b"container=lxc\x00")
            raise FileNotFoundError(p)

        # MAC on eth0 is different from what PVE returns
        with patch("builtins.open", side_effect=_open_side), \
             patch("pathlib.Path.read_text", return_value="11:22:33:44:55:66\n"), \
             patch("app.load_hosts", return_value=[fake_host]), \
             patch("app.PVEClient", return_value=mock_pve):
            result = _detect_self_vmid()

        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# FMT — _fmt_bytes
# ─────────────────────────────────────────────────────────────────────────────

class TestFmtBytes:

    def _fmt(self, n):
        from app import _fmt_bytes
        return _fmt_bytes(n)

    def test_bytes(self):
        assert self._fmt(512) == "512 B"

    def test_kb(self):
        assert self._fmt(2048) == "2 KB"

    def test_mb(self):
        assert self._fmt(2 * 1024 ** 2) == "2.0 MB"

    def test_gb(self):
        assert self._fmt(3 * 1024 ** 3) == "3.0 GB"

    def test_boundary_exactly_1_gb(self):
        assert "GB" in self._fmt(1024 ** 3)

    def test_zero(self):
        assert self._fmt(0) == "0 B"


# ─────────────────────────────────────────────────────────────────────────────
# PBS_IDEM — delete_snapshot 400/404 idempotency
# ─────────────────────────────────────────────────────────────────────────────

class TestPBSDeleteIdempotency:
    """pbs_client.delete_snapshot must treat 400 and 404 as success (already gone)."""

    def _pbs(self):
        from config import HostConfig
        from pbs_client import PBSClient
        host = HostConfig(
            id="x", label="x",
            pve_url="https://1.2.3.4:8006", pve_user="root@pam", pve_password="x",
            pbs_url="https://1.2.3.4:8007", pbs_user="b@pbs", pbs_password="x",
            pbs_datastore="ds",
        )
        with patch("pbs_client.PBSClient._authenticate", return_value=("ticket", "csrf")):
            return PBSClient(host)

    def _http_error(self, status_code: int) -> requests.HTTPError:
        resp = MagicMock()
        resp.status_code = status_code
        return requests.HTTPError(response=resp)

    def test_400_treated_as_success(self):
        pbs = self._pbs()
        with patch.object(pbs, "_delete", side_effect=self._http_error(400)):
            pbs.delete_snapshot("ct", "301", 1000)  # must not raise

    def test_404_treated_as_success(self):
        pbs = self._pbs()
        with patch.object(pbs, "_delete", side_effect=self._http_error(404)):
            pbs.delete_snapshot("ct", "301", 1000)  # must not raise

    def test_500_is_re_raised(self):
        pbs = self._pbs()
        with patch.object(pbs, "_delete", side_effect=self._http_error(500)):
            with pytest.raises(requests.HTTPError):
                pbs.delete_snapshot("ct", "301", 1000)

    def test_success_makes_no_exception(self):
        pbs = self._pbs()
        with patch.object(pbs, "_delete", return_value={}):
            pbs.delete_snapshot("ct", "301", 1000)  # must not raise


# ─────────────────────────────────────────────────────────────────────────────
# CLOUD_ONLY — cloud-only detection edge cases
# ─────────────────────────────────────────────────────────────────────────────

class TestCloudOnlyDetection:
    """All tests use the Flask test client against the real get_items endpoint
    with all three external clients fully mocked."""

    # ── helpers ──────────────────────────────────────────────────────────────

    def _get_items(self, flask_client, mock_host, pbs_snaps, pve_meta, restic_by_vm, untagged=None):
        with _mock_clients(mock_host, pbs_snaps, pve_meta, restic_by_vm, untagged):
            resp = flask_client.get(f"/api/host/{HOST_ID}/items")
            assert resp.status_code == 200
            return json.loads(resp.data)

    def _find_lxc(self, data: dict, vmid: int) -> dict:
        return next(x for x in data["lxcs"] if x["id"] == vmid)

    def _snap(self, item: dict, backup_time: int) -> dict:
        return next(s for s in item["snapshots"] if s["backup_time"] == backup_time)

    # ── tests ─────────────────────────────────────────────────────────────────

    def test_basic_cloud_only_new_tag(self, flask_client, mock_host):
        """R2 covers T1 (gone) + T2 (local). T1 must appear as cloud-only."""
        restic_by_vm = {
            301: [
                {"ts": TS_R2, "pbs_time": T1, "id": "R2id", "short_id": "R2sh"},
                {"ts": TS_R2, "pbs_time": T2, "id": "R2id", "short_id": "R2sh"},
            ]
        }
        data = self._get_items(flask_client, mock_host,
                               pbs_snaps=[_pbs_group(301, "ct", [_pbs_snap(T2)])],
                               pve_meta=_pve_meta(),
                               restic_by_vm=restic_by_vm)
        ct = self._find_lxc(data, 301)
        assert len(ct["snapshots"]) == 2

        t2 = self._snap(ct, T2)
        assert t2["local"] is True
        assert t2["cloud"] is True

        t1 = self._snap(ct, T1)
        assert t1["local"] is False
        assert t1["cloud"] is True
        assert t1["restic_id"] == "R2id"

    def test_cloud_only_date_uses_pbs_time_not_restic_ts(self, flask_client, mock_host):
        """cloud-only row date must reflect pbs_time (T1=1000), not the restic snapshot ts (TS_R2=1200)."""
        from datetime import datetime, timezone
        expected_date = datetime.fromtimestamp(T1, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

        restic_by_vm = {
            301: [
                {"ts": TS_R2, "pbs_time": T1, "id": "R2id", "short_id": "R2sh"},
                {"ts": TS_R2, "pbs_time": T2, "id": "R2id", "short_id": "R2sh"},
            ]
        }
        data = self._get_items(flask_client, mock_host,
                               pbs_snaps=[_pbs_group(301, "ct", [_pbs_snap(T2)])],
                               pve_meta=_pve_meta(),
                               restic_by_vm=restic_by_vm)
        ct = self._find_lxc(data, 301)
        t1 = self._snap(ct, T1)

        assert t1["date"] == expected_date, (
            f"cloud-only date should be pbs_time ({expected_date}), "
            f"not restic snapshot ts (got: {t1['date']})"
        )
        # backup_time in the snap must also equal T1
        assert t1["backup_time"] == T1

    def test_multi_restic_dedup_single_cloud_only_row(self, flask_client, mock_host):
        """R1(T1), R2(T1+T2), R3(T1+T2): exactly 1 cloud-only row for T1, restic_id = R3 (newest)."""
        restic_by_vm = {
            301: [
                {"ts": TS_R1, "pbs_time": T1, "id": "R1id", "short_id": "R1sh"},
                {"ts": TS_R2, "pbs_time": T1, "id": "R2id", "short_id": "R2sh"},
                {"ts": TS_R2, "pbs_time": T2, "id": "R2id", "short_id": "R2sh"},
                {"ts": TS_R3, "pbs_time": T1, "id": "R3id", "short_id": "R3sh"},
                {"ts": TS_R3, "pbs_time": T2, "id": "R3id", "short_id": "R3sh"},
            ]
        }
        data = self._get_items(flask_client, mock_host,
                               pbs_snaps=[_pbs_group(301, "ct", [_pbs_snap(T2)])],
                               pve_meta=_pve_meta(),
                               restic_by_vm=restic_by_vm)
        ct = self._find_lxc(data, 301)

        cloud_only = [s for s in ct["snapshots"] if s["cloud"] and not s["local"]]
        assert len(cloud_only) == 1, (
            f"Expected exactly 1 cloud-only row for T1, got {len(cloud_only)}: {cloud_only}"
        )
        # newest restic snapshot covering T1 must be used as restic_id
        assert cloud_only[0]["restic_id"] == "R3id"

    def test_same_restic_snap_covers_local_and_gone(self, flask_client, mock_host):
        """R1 covers T1(gone)+T2(local). T2 = local+cloud, T1 = cloud-only. Both via R1."""
        restic_by_vm = {
            301: [
                {"ts": TS_R2, "pbs_time": T1, "id": "R1id", "short_id": "R1sh"},
                {"ts": TS_R2, "pbs_time": T2, "id": "R1id", "short_id": "R1sh"},
            ]
        }
        data = self._get_items(flask_client, mock_host,
                               pbs_snaps=[_pbs_group(301, "ct", [_pbs_snap(T2)])],
                               pve_meta=_pve_meta(),
                               restic_by_vm=restic_by_vm)
        ct = self._find_lxc(data, 301)

        t2 = self._snap(ct, T2)
        assert t2["local"] is True
        assert t2["cloud"] is True
        assert t2["restic_id"] == "R1id"

        t1 = self._snap(ct, T1)
        assert t1["local"] is False
        assert t1["cloud"] is True
        assert t1["restic_id"] == "R1id"

    def test_no_spurious_cloud_only_when_both_local_and_cloud(self, flask_client, mock_host):
        """When T2 is both local and in restic, no cloud-only entry must appear for T2."""
        restic_by_vm = {
            301: [
                {"ts": TS_R2, "pbs_time": T2, "id": "R2id", "short_id": "R2sh"},
            ]
        }
        data = self._get_items(flask_client, mock_host,
                               pbs_snaps=[_pbs_group(301, "ct", [_pbs_snap(T2)])],
                               pve_meta=_pve_meta(),
                               restic_by_vm=restic_by_vm)
        ct = self._find_lxc(data, 301)

        # Only T2 must appear — no extra cloud-only row
        assert len(ct["snapshots"]) == 1
        assert ct["snapshots"][0]["backup_time"] == T2
        assert ct["snapshots"][0]["local"] is True
        assert ct["snapshots"][0]["cloud"] is True

    def test_old_tag_style_cloud_only_older_than_oldest_local(self, flask_client, mock_host):
        """Old-style tag (no pbs_time) with restic ts older than oldest local PBS → cloud-only."""
        # R1 was taken at ts=500, before oldest local PBS snap at T2=1100
        # Old tag format: no "pbs_time" key
        restic_by_vm = {
            301: [
                {"ts": 500, "id": "R_old", "short_id": "R_ol"},  # no pbs_time → old tag
            ]
        }
        data = self._get_items(flask_client, mock_host,
                               pbs_snaps=[_pbs_group(301, "ct", [_pbs_snap(T2)])],
                               pve_meta=_pve_meta(),
                               restic_by_vm=restic_by_vm)
        ct = self._find_lxc(data, 301)

        cloud_only = [s for s in ct["snapshots"] if s["cloud"] and not s["local"]]
        assert len(cloud_only) == 1

    def test_old_tag_style_newer_than_oldest_local_not_cloud_only(self, flask_client, mock_host):
        """Old-style restic snapshot ts > oldest local PBS → not cloud-only (was covered)."""
        # R1 taken at ts=1200 (newer than T2=1100) → covered by existing local backup
        restic_by_vm = {
            301: [
                {"ts": 1200, "id": "R_new", "short_id": "R_ne"},  # no pbs_time → old tag
            ]
        }
        data = self._get_items(flask_client, mock_host,
                               pbs_snaps=[_pbs_group(301, "ct", [_pbs_snap(T2)])],
                               pve_meta=_pve_meta(),
                               restic_by_vm=restic_by_vm)
        ct = self._find_lxc(data, 301)

        cloud_only = [s for s in ct["snapshots"] if s["cloud"] and not s["local"]]
        assert len(cloud_only) == 0

    def test_multiple_vms_dedup_independent(self, flask_client, mock_host):
        """Cloud-only dedup is per-VM: deleting T1 from ct/301 doesn't affect ct/302."""
        restic_by_vm = {
            301: [
                {"ts": TS_R2, "pbs_time": T1, "id": "R2id", "short_id": "R2sh"},
                {"ts": TS_R2, "pbs_time": T2, "id": "R2id", "short_id": "R2sh"},
            ],
            302: [
                {"ts": TS_R2, "pbs_time": T2, "id": "R2id", "short_id": "R2sh"},
            ],
        }
        pbs_snaps = [
            _pbs_group(301, "ct", [_pbs_snap(T2)]),
            _pbs_group(302, "ct", [_pbs_snap(T2)]),
        ]
        pve_meta = {
            **_pve_meta(301),
            **_pve_meta(302),
        }
        data = self._get_items(flask_client, mock_host,
                               pbs_snaps=pbs_snaps,
                               pve_meta=pve_meta,
                               restic_by_vm=restic_by_vm)

        ct301 = self._find_lxc(data, 301)
        ct302 = self._find_lxc(data, 302)

        cloud_only_301 = [s for s in ct301["snapshots"] if s["cloud"] and not s["local"]]
        cloud_only_302 = [s for s in ct302["snapshots"] if s["cloud"] and not s["local"]]

        assert len(cloud_only_301) == 1, "ct/301 should have exactly 1 cloud-only entry"
        assert len(cloud_only_302) == 0, "ct/302 T2 is local+cloud, no cloud-only"

    def test_no_restic_no_cloud_snaps(self, flask_client, mock_host):
        """Without restic_repo, no cloud annotations appear."""
        from config import HostConfig
        host_no_restic = HostConfig(
            id=HOST_ID, label="Test",
            pve_url="https://1.2.3.4:8006", pve_user="root@pam", pve_password="x",
            pbs_url="https://1.2.3.4:8007", pbs_user="backup@pbs", pbs_password="x",
            pbs_datastore="test-store",
            restic_repo="",  # no restic
        )
        _app._cache.clear()
        with patch.dict(_app.HOSTS, {HOST_ID: host_no_restic}, clear=True), \
             patch("app.PBSClient") as pbs_cls, \
             patch("app.PVEClient") as pve_cls:
            pbs_cls.return_value.get_storage_info.return_value = _storage()
            pbs_cls.return_value.get_snapshots.return_value = [
                _pbs_group(301, "ct", [_pbs_snap(T2)])
            ]
            pve_cls.return_value.get_vms_and_lxcs.return_value = _pve_meta()
            resp = flask_client.get(f"/api/host/{HOST_ID}/items")

        data = json.loads(resp.data)
        ct = next(x for x in data["lxcs"] if x["id"] == 301)
        assert all(not s["cloud"] for s in ct["snapshots"])


# ─────────────────────────────────────────────────────────────────────────────
# STEP_FORMAT — every job type must emit "Step N/M —" log lines
# ─────────────────────────────────────────────────────────────────────────────

class TestStepFormat:
    """Verify progress log format so the frontend progress bar works."""

    def _start_job(self, flask_client, path: str, body: dict) -> dict:
        resp = flask_client.post(
            f"/api/host/{HOST_ID}{path}",
            data=json.dumps(body), content_type="application/json",
        )
        assert resp.status_code == 200, f"POST {path} failed: {resp.data}"
        return json.loads(resp.data)

    def _job_logs(self, flask_client, job_id: str) -> list[str]:
        job = _poll(flask_client, job_id)
        assert job["status"] == "done", (
            f"Job failed. Logs:\n" + "\n".join(job.get("logs", []))
        )
        return job["logs"]

    # ── backup/pbs ───────────────────────────────────────────────────────────

    def test_backup_pbs_step_format(self, flask_client, mock_host):
        _app._cache.clear()
        mock_pve = MagicMock()
        mock_pve.get_nodes.return_value = ["pve"]
        mock_pve.backup_vm.return_value = "UPID:pve:0:backup:vm/301"
        mock_pve.wait_for_task.return_value = True

        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.PVEClient", return_value=mock_pve):
            resp_body = self._start_job(flask_client, "/backup/pbs",
                                        {"vmid": 301, "type": "ct"})
            logs = self._job_logs(flask_client, resp_body["job_id"])

        steps = _step_lines({"logs": logs})
        assert steps, f"No 'Step N/M —' lines found in:\n{logs}"
        assert all("Step" in l and "/" in l and "—" in l for l in steps)

    def test_backup_restic_step_format(self, flask_client, mock_host):
        _app._cache.clear()
        mock_restic = MagicMock()
        mock_restic.backup_datastore.return_value = None

        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.ResticClient", return_value=mock_restic), \
             patch("app._pbs_vm_ids", return_value=[]):
            resp_body = self._start_job(flask_client, "/backup/restic", {})
            logs = self._job_logs(flask_client, resp_body["job_id"])

        steps = _step_lines({"logs": logs})
        assert steps, f"No 'Step N/M —' lines found in restic backup:\n{logs}"

    def test_restore_local_step_format(self, flask_client, mock_host):
        _app._cache.clear()
        mock_pve = MagicMock()
        mock_pve.get_nodes.return_value = ["pve"]
        mock_pve.stop_vm.return_value = None
        mock_pve.restore_vm.return_value = "UPID:pve:0:restore:ct/301"
        mock_pve.wait_for_task.return_value = True

        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.PVEClient", return_value=mock_pve):
            resp_body = self._start_job(flask_client, "/restore",
                                        {"vmid": 301, "type": "ct", "backup_time": T2,
                                         "source": "local"})
            logs = self._job_logs(flask_client, resp_body["job_id"])

        steps = _step_lines({"logs": logs})
        assert steps, f"No 'Step N/M —' lines in local restore:\n{logs}"
        # local restore has 2 steps (no cloud, no run_backup_after)
        assert any("2/" in l for l in steps)

    def test_restore_cloud_step_format(self, flask_client, mock_host):
        _app._cache.clear()
        mock_pve = MagicMock()
        mock_pve.get_nodes.return_value = ["pve"]
        mock_pve.stop_vm.return_value = None
        mock_pve.restore_vm.return_value = "UPID:pve:0:restore:ct/301"
        mock_pve.wait_for_task.return_value = True

        mock_restic = MagicMock()
        mock_restic._ssh_host = "1.2.3.4"
        mock_restic._ssh_run.return_value = ""
        mock_restic.restore_datastore.return_value = None

        mock_pbs = MagicMock()
        mock_pbs.get_snapshots.return_value = [
            _pbs_group(301, "ct", [_pbs_snap(T2)])
        ]

        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.PVEClient", return_value=mock_pve), \
             patch("app.ResticClient", return_value=mock_restic), \
             patch("app.PBSClient", return_value=mock_pbs):
            resp_body = self._start_job(flask_client, "/restore",
                                        {"vmid": 301, "type": "ct",
                                         "source": "cloud", "restic_id": "R2id",
                                         "run_backup_after": False})
            logs = self._job_logs(flask_client, resp_body["job_id"])

        steps = _step_lines({"logs": logs})
        assert steps, f"No 'Step N/M —' lines in cloud restore:\n{logs}"
        # cloud restore has 3 steps (no run_backup_after)
        assert any("3/" in l for l in steps)

    def test_delete_pbs_step_format(self, flask_client, mock_host):
        _app._cache.clear()
        mock_pbs = MagicMock()
        mock_pbs.delete_snapshot.return_value = None

        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.PBSClient", return_value=mock_pbs):
            resp_body = self._start_job(flask_client, "/delete/pbs",
                                        {"vmid": 301, "type": "ct", "backup_time": T2})
            logs = self._job_logs(flask_client, resp_body["job_id"])

        steps = _step_lines({"logs": logs})
        assert steps, f"No 'Step N/M —' in delete_pbs:\n{logs}"

    def test_delete_pbs_all_step_format(self, flask_client, mock_host):
        _app._cache.clear()
        mock_pbs = MagicMock()
        mock_pbs.delete_all_snapshots_for_vm.return_value = 2

        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.PBSClient", return_value=mock_pbs):
            resp_body = self._start_job(flask_client, "/delete/pbs/all",
                                        {"vmid": 301, "type": "ct"})
            logs = self._job_logs(flask_client, resp_body["job_id"])

        steps = _step_lines({"logs": logs})
        assert steps, f"No 'Step N/M —' in delete_pbs_all:\n{logs}"

    def test_delete_cloud_step_format(self, flask_client, mock_host):
        _app._cache.clear()
        mock_pbs = MagicMock()
        mock_pbs.get_snapshots.return_value = [_pbs_group(301, "ct", [_pbs_snap(T1)])]
        mock_pbs.delete_snapshot.return_value = None

        mock_restic = MagicMock()
        mock_restic._ssh_run.return_value = ""
        mock_restic.restore_datastore.return_value = None
        mock_restic.backup_datastore.return_value = None
        mock_restic.get_snapshots_by_vm.return_value = ({}, [])
        mock_restic.forget_snapshots.return_value = None

        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.PBSClient", return_value=mock_pbs), \
             patch("app.ResticClient", return_value=mock_restic), \
             patch("app._pbs_vm_ids", return_value=[]):
            resp_body = self._start_job(flask_client, "/delete/cloud",
                                        {"vmid": 301, "type": "ct",
                                         "backup_time": T1, "restic_id": "R1id"})
            logs = self._job_logs(flask_client, resp_body["job_id"])

        steps = _step_lines({"logs": logs})
        # Step 4 intentionally emits two "Step 4/4 —" lines (finding + removing),
        # so check that all four step numbers appear rather than counting lines.
        for n in range(1, 5):
            assert any(f"Step {n}/4" in l for l in steps), (
                f"delete_cloud missing 'Step {n}/4' in logs:\n{logs}"
            )

    def test_delete_restic_step_format(self, flask_client, mock_host):
        _app._cache.clear()
        mock_restic = MagicMock()
        mock_restic.forget_snapshots.return_value = None

        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.ResticClient", return_value=mock_restic):
            resp_body = self._start_job(flask_client, "/delete/restic",
                                        {"restic_id": "R1id"})
            logs = self._job_logs(flask_client, resp_body["job_id"])

        steps = _step_lines({"logs": logs})
        assert steps, f"No 'Step N/M —' in delete_restic:\n{logs}"


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT — input validation: 400 / 404 / 409
# ─────────────────────────────────────────────────────────────────────────────

class TestEndpointValidation:

    def _post(self, flask_client, path: str, body: dict):
        return flask_client.post(
            path, data=json.dumps(body), content_type="application/json",
        )

    def test_unknown_host_404(self, flask_client):
        resp = flask_client.get("/api/host/does-not-exist/items")
        assert resp.status_code == 404

    def test_delete_pbs_missing_vmid_400(self, flask_client, mock_host):
        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True):
            resp = self._post(flask_client, f"/api/host/{HOST_ID}/delete/pbs",
                              {"type": "ct", "backup_time": T2})
        assert resp.status_code == 400

    def test_delete_pbs_missing_backup_time_400(self, flask_client, mock_host):
        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True):
            resp = self._post(flask_client, f"/api/host/{HOST_ID}/delete/pbs",
                              {"vmid": 301, "type": "ct"})
        assert resp.status_code == 400

    def test_delete_cloud_missing_vmid_400(self, flask_client, mock_host):
        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True):
            resp = self._post(flask_client, f"/api/host/{HOST_ID}/delete/cloud",
                              {"type": "ct", "backup_time": T1, "restic_id": "R1id"})
        assert resp.status_code == 400

    def test_delete_cloud_missing_restic_id_400(self, flask_client, mock_host):
        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True):
            resp = self._post(flask_client, f"/api/host/{HOST_ID}/delete/cloud",
                              {"vmid": 301, "type": "ct", "backup_time": T1})
        assert resp.status_code == 400

    def test_backup_restic_without_restic_repo_400(self, flask_client):
        from config import HostConfig
        host_no_restic = HostConfig(
            id=HOST_ID, label="x",
            pve_url="https://1.2.3.4:8006", pve_user="root@pam", pve_password="x",
            pbs_url="https://1.2.3.4:8007", pbs_user="b@pbs", pbs_password="x",
            pbs_datastore="ds", restic_repo="",
        )
        with patch.dict(_app.HOSTS, {HOST_ID: host_no_restic}, clear=True):
            resp = self._post(flask_client, f"/api/host/{HOST_ID}/backup/restic", {})
        assert resp.status_code == 400

    def test_delete_restic_missing_restic_id_400(self, flask_client, mock_host):
        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True):
            resp = self._post(flask_client, f"/api/host/{HOST_ID}/delete/restic", {})
        assert resp.status_code == 400

    def test_restore_datastore_missing_restic_id_400(self, flask_client, mock_host):
        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True):
            resp = self._post(flask_client, f"/api/host/{HOST_ID}/restore/datastore", {})
        assert resp.status_code == 400

    def test_restic_snapshots_no_restic_repo_404(self, flask_client):
        from config import HostConfig
        host_no_restic = HostConfig(
            id=HOST_ID, label="x",
            pve_url="https://1.2.3.4:8006", pve_user="root@pam", pve_password="x",
            pbs_url="https://1.2.3.4:8007", pbs_user="b@pbs", pbs_password="x",
            pbs_datastore="ds", restic_repo="",
        )
        with patch.dict(_app.HOSTS, {HOST_ID: host_no_restic}, clear=True):
            resp = flask_client.get(f"/api/host/{HOST_ID}/restic/snapshots")
        assert resp.status_code == 404

    def test_concurrent_restic_operations_return_409(self, flask_client, mock_host):
        """Acquiring restic lock for a host and then POST again must return 409."""
        lock = _app._get_restic_lock(HOST_ID)
        lock.acquire()
        try:
            with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True):
                resp = self._post(flask_client, f"/api/host/{HOST_ID}/backup/restic", {})
            assert resp.status_code == 409
        finally:
            lock.release()

    def test_delete_restic_unknown_host_404(self, flask_client):
        resp = self._post(flask_client, "/api/host/no-such-host/delete/restic",
                          {"restic_id": "abc"})
        assert resp.status_code == 404


# ─────────────────────────────────────────────────────────────────────────────
# COVERAGE — /restic/snapshots local coverage annotation
# ─────────────────────────────────────────────────────────────────────────────

class TestResticSnapshotCoverage:

    def _get_restic_snaps(self, flask_client, mock_host, flat_snaps, pbs_groups):
        mock_restic = MagicMock()
        mock_restic.is_running.return_value = False
        mock_restic.get_snapshots_flat.return_value = flat_snaps
        mock_pbs = MagicMock()
        mock_pbs.get_snapshots.return_value = pbs_groups

        _app._cache.clear()
        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.ResticClient", return_value=mock_restic), \
             patch("app.PBSClient", return_value=mock_pbs):
            resp = flask_client.get(f"/api/host/{HOST_ID}/restic/snapshots")

        assert resp.status_code == 200
        body = json.loads(resp.data)
        return body.get("snaps", body) if isinstance(body, dict) else body

    def test_local_coverage_marked_true_when_in_pbs(self, flask_client, mock_host):
        """Cover entry for a snapshot that still exists in PBS → local=True."""
        flat_snaps = [{
            "id": "R1id", "short_id": "R1sh", "ts": TS_R2, "size_bytes": 1024,
            "covers": [{"type": "ct", "vmid": 301, "pbs_time": T2}],
        }]
        pbs_groups = [_pbs_group(301, "ct", [_pbs_snap(T2)])]

        snaps = self._get_restic_snaps(flask_client, mock_host, flat_snaps, pbs_groups)
        assert len(snaps) == 1
        cov = snaps[0]["covers"][0]
        assert cov["local"] is True

    def test_gone_coverage_marked_false_when_not_in_pbs(self, flask_client, mock_host):
        """Cover entry for a snapshot deleted from PBS → local=False."""
        flat_snaps = [{
            "id": "R1id", "short_id": "R1sh", "ts": TS_R2, "size_bytes": 1024,
            "covers": [{"type": "ct", "vmid": 301, "pbs_time": T1}],
        }]
        pbs_groups = [_pbs_group(301, "ct", [_pbs_snap(T2)])]  # T2 only, not T1

        snaps = self._get_restic_snaps(flask_client, mock_host, flat_snaps, pbs_groups)
        cov = snaps[0]["covers"][0]
        assert cov["local"] is False

    def test_old_tag_coverage_marked_unknown(self, flask_client, mock_host):
        """Cover entry with pbs_time=None (old-style tag) → local=None (unknown)."""
        flat_snaps = [{
            "id": "R_old", "short_id": "R_ol", "ts": TS_R1, "size_bytes": 512,
            "covers": [{"type": "ct", "vmid": 301, "pbs_time": None}],
        }]
        pbs_groups = [_pbs_group(301, "ct", [_pbs_snap(T2)])]

        snaps = self._get_restic_snaps(flask_client, mock_host, flat_snaps, pbs_groups)
        cov = snaps[0]["covers"][0]
        assert cov["local"] is None

    def test_pbs_date_populated_for_new_tags(self, flask_client, mock_host):
        """Cover entry with pbs_time → pbs_date populated as 'YYYY-MM-DD HH:MM'."""
        flat_snaps = [{
            "id": "R1id", "short_id": "R1sh", "ts": TS_R2, "size_bytes": 1024,
            "covers": [{"type": "ct", "vmid": 301, "pbs_time": T2}],
        }]
        snaps = self._get_restic_snaps(flask_client, mock_host, flat_snaps,
                                       [_pbs_group(301, "ct", [_pbs_snap(T2)])])
        cov = snaps[0]["covers"][0]
        assert cov["pbs_date"] is not None
        # Must be in YYYY-MM-DD HH:MM format
        import re
        assert re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", cov["pbs_date"])

    def test_pbs_date_none_for_old_tags(self, flask_client, mock_host):
        flat_snaps = [{
            "id": "R_old", "short_id": "R_ol", "ts": TS_R1, "size_bytes": 512,
            "covers": [{"type": "ct", "vmid": 301, "pbs_time": None}],
        }]
        snaps = self._get_restic_snaps(flask_client, mock_host, flat_snaps,
                                       [_pbs_group(301, "ct", [_pbs_snap(T2)])])
        cov = snaps[0]["covers"][0]
        assert cov["pbs_date"] is None

    def test_size_formatted_as_string(self, flask_client, mock_host):
        """size_bytes must be formatted to human-readable 'size' field."""
        flat_snaps = [{
            "id": "R1id", "short_id": "R1sh", "ts": TS_R2,
            "size_bytes": 5 * 1024 * 1024,  # 5 MB
            "covers": [],
        }]
        snaps = self._get_restic_snaps(flask_client, mock_host, flat_snaps, [])
        assert snaps[0]["size"] == "5.0 MB"

    def test_empty_repo_returns_empty_list(self, flask_client, mock_host):
        snaps = self._get_restic_snaps(flask_client, mock_host, [], [])
        assert snaps == []


# ─────────────────────────────────────────────────────────────────────────────
# DELETE_CLOUD — step 4 forgets ALL restic IDs that carry the deleted pbs_time
# ─────────────────────────────────────────────────────────────────────────────

class TestDeleteCloudStep4:
    """Step 4 must forget every restic snapshot that contains the deleted pbs_time tag.

    Scenario: T1 deleted from PBS. R1, R2, R3 were all taken while T1 existed
    and each carries the ct-301-T1 tag. After re-uploading (step 3), step 4 must
    forget R1, R2, and R3 — not just the single snapshot that was shown as cloud-only.
    """

    def _run_delete_cloud(self, flask_client, mock_host, restic_by_vm_post):
        """Run delete/cloud with mocked clients, return the completed job."""
        mock_pbs = MagicMock()
        # First call: PBS readiness poll (ready immediately)
        # Second call: get snapshot list to find closest pbs_time
        mock_pbs.get_snapshots.return_value = [
            _pbs_group(301, "ct", [_pbs_snap(T1)])
        ]
        mock_pbs.delete_snapshot.return_value = None

        mock_restic = MagicMock()
        mock_restic._ssh_run.return_value = ""
        mock_restic.restore_datastore.return_value = None
        mock_restic.backup_datastore.return_value = None
        mock_restic.get_snapshots_by_vm.return_value = (restic_by_vm_post, [])
        mock_restic.forget_snapshots.return_value = None

        _app._cache.clear()
        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.PBSClient", return_value=mock_pbs), \
             patch("app.ResticClient", return_value=mock_restic), \
             patch("app._pbs_vm_ids", return_value=[]):
            resp = flask_client.post(
                f"/api/host/{HOST_ID}/delete/cloud",
                data=json.dumps({"vmid": 301, "type": "ct",
                                 "backup_time": T1, "restic_id": "R1id"}),
                content_type="application/json",
            )
            assert resp.status_code == 200
            job_id = json.loads(resp.data)["job_id"]
            job = _poll(flask_client, job_id, timeout=10)
            return job, mock_restic

    def test_step4_forgets_all_restic_ids_with_matching_pbs_time(self, flask_client, mock_host):
        """Three restic snapshots all carry T1. Step 4 must forget all three."""
        restic_by_vm_post = {
            301: [
                {"ts": TS_R1, "pbs_time": T1, "id": "R1id", "short_id": "R1sh"},
                {"ts": TS_R2, "pbs_time": T1, "id": "R2id", "short_id": "R2sh"},
                {"ts": TS_R3, "pbs_time": T1, "id": "R3id", "short_id": "R3sh"},
                # R3 also covers T2 — must not affect step 4
                {"ts": TS_R3, "pbs_time": T2, "id": "R3id", "short_id": "R3sh"},
            ]
        }
        job, mock_restic = self._run_delete_cloud(flask_client, mock_host, restic_by_vm_post)
        assert job["status"] == "done", f"Job failed:\n" + "\n".join(job.get("logs", []))

        # Verify forget_snapshots was called with all three IDs (order may vary)
        call_args = mock_restic.forget_snapshots.call_args
        assert call_args is not None, "forget_snapshots was never called"
        ids_forgotten = set(call_args[0][0])
        assert ids_forgotten == {"R1id", "R2id", "R3id"}, (
            f"Expected all 3 restic IDs to be forgotten, got: {ids_forgotten}"
        )

    def test_step4_fallback_when_no_new_tags(self, flask_client, mock_host):
        """When get_snapshots_by_vm returns nothing for vmid, fall back to restic_id from request."""
        # Simulates old-style tags where pbs_time matching doesn't work
        restic_by_vm_post = {}  # empty — no new-tag entries

        job, mock_restic = self._run_delete_cloud(flask_client, mock_host, restic_by_vm_post)
        assert job["status"] == "done", f"Job failed:\n" + "\n".join(job.get("logs", []))

        call_args = mock_restic.forget_snapshots.call_args
        assert call_args is not None
        ids_forgotten = set(call_args[0][0])
        assert "R1id" in ids_forgotten, "Fallback must use the original restic_id from request"

    def test_step4_fallback_when_pbs_time_is_none_old_style_tag(self, flask_client, mock_host):
        """Old-style restic tags have no pbs_time (pbs_time=None). The condition
        e.get('pbs_time') == backup_time evaluates to None == T1 → False for every
        entry, so ids_to_forget is empty and the fallback [restic_id] is used.
        This must not raise and must forget exactly the one restic_id from the request.
        """
        # Simulate entries with old-style tags: pbs_time=None (no timestamp in tag)
        restic_by_vm_post = {
            301: [
                {"ts": TS_R1, "pbs_time": None, "id": "OldR1id", "short_id": "old1"},
                {"ts": TS_R2, "pbs_time": None, "id": "OldR2id", "short_id": "old2"},
            ]
        }

        job, mock_restic = self._run_delete_cloud(flask_client, mock_host, restic_by_vm_post)
        assert job["status"] == "done", f"Job failed:\n" + "\n".join(job.get("logs", []))

        call_args = mock_restic.forget_snapshots.call_args
        assert call_args is not None
        ids_forgotten = set(call_args[0][0])
        # fallback: none of old-style entries match T1, so forget original restic_id only
        assert ids_forgotten == {"R1id"}, (
            f"Old-style tag fallback must forget only the request's restic_id 'R1id', "
            f"got: {ids_forgotten}"
        )

    def test_step4_fallback_when_untagged_restic_snap(self, flask_client, mock_host):
        """Restic snapshots with no tags at all (empty covers list or no vmid entry).
        get_snapshots_by_vm returns an empty list for the vmid → fallback to restic_id.
        """
        # Simulate completely untagged restic snapshot: vmid=301 has no entries
        restic_by_vm_post = {
            999: [  # different vmid — 301 has no entries
                {"ts": TS_R1, "pbs_time": T1, "id": "OtherVMid", "short_id": "oth1"},
            ]
        }

        job, mock_restic = self._run_delete_cloud(flask_client, mock_host, restic_by_vm_post)
        assert job["status"] == "done", f"Job failed:\n" + "\n".join(job.get("logs", []))

        call_args = mock_restic.forget_snapshots.call_args
        assert call_args is not None
        ids_forgotten = set(call_args[0][0])
        assert ids_forgotten == {"R1id"}, (
            f"Untagged snapshot fallback must forget only 'R1id', got: {ids_forgotten}"
        )

    def test_step4_only_forgets_matching_pbs_time(self, flask_client, mock_host):
        """R2 covers T1 (to forget) + T2 (keep). Only R2's T1-tagged entry should be forgotten."""
        restic_by_vm_post = {
            301: [
                {"ts": TS_R2, "pbs_time": T1, "id": "R2id", "short_id": "R2sh"},
                {"ts": TS_R2, "pbs_time": T2, "id": "R2id", "short_id": "R2sh"},  # same snap, T2
                {"ts": TS_R3, "pbs_time": T2, "id": "R3id", "short_id": "R3sh"},  # different snap, T2 only
            ]
        }
        job, mock_restic = self._run_delete_cloud(flask_client, mock_host, restic_by_vm_post)
        assert job["status"] == "done", f"Job failed:\n" + "\n".join(job.get("logs", []))

        call_args = mock_restic.forget_snapshots.call_args
        ids_forgotten = set(call_args[0][0])
        # R2 covers T1 → must be forgotten
        assert "R2id" in ids_forgotten
        # R3 only covers T2 → must NOT be forgotten
        assert "R3id" not in ids_forgotten, (
            "R3 only covers T2 (not T1) — must not be included in forget list"
        )


# ─────────────────────────────────────────────────────────────────────────────
# API FIELDS — hosts endpoint includes self_vmid
# ─────────────────────────────────────────────────────────────────────────────

class TestApiFields:

    def test_hosts_endpoint_includes_self_vmid(self, flask_client, mock_host):
        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True):
            resp = flask_client.get("/api/hosts")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert len(data) == 1
        assert "self_vmid" in data[0], "self_vmid missing from /api/hosts response"

    def test_items_in_pve_true_for_pve_vm(self, flask_client, mock_host):
        with _mock_clients(mock_host,
                           pbs_snaps=[_pbs_group(301, "ct", [_pbs_snap(T2)])],
                           pve_meta=_pve_meta(),
                           restic_by_vm={}):
            resp = flask_client.get(f"/api/host/{HOST_ID}/items")
        data = json.loads(resp.data)
        ct = next(x for x in data["lxcs"] if x["id"] == 301)
        assert ct["in_pve"] is True

    def test_items_in_pve_false_for_orphaned_snapshot(self, flask_client, mock_host):
        """VM 999 has PBS snapshots but is not in PVE inventory → in_pve=False."""
        with _mock_clients(mock_host,
                           pbs_snaps=[_pbs_group(999, "vm", [_pbs_snap(T2)])],
                           pve_meta={},  # empty PVE — 999 not in PVE
                           restic_by_vm={}):
            resp = flask_client.get(f"/api/host/{HOST_ID}/items")
        data = json.loads(resp.data)
        vm = next(x for x in data["vms"] if x["id"] == 999)
        assert vm["in_pve"] is False

    def test_lxc_type_normalised_to_ct(self, flask_client, mock_host):
        """Endpoint accepts type='lxc' and normalises it to 'ct' before calling PBS."""
        mock_pbs = MagicMock()
        mock_pbs.delete_snapshot.return_value = None

        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.PBSClient", return_value=mock_pbs):
            resp = flask_client.post(
                f"/api/host/{HOST_ID}/delete/pbs",
                data=json.dumps({"vmid": 301, "type": "lxc", "backup_time": T2}),
                content_type="application/json",
            )
            assert resp.status_code == 200
            job_id = json.loads(resp.data)["job_id"]
            job = _poll(flask_client, job_id, timeout=5)

        assert job["status"] == "done"
        call_args = mock_pbs.delete_snapshot.call_args
        assert call_args[0][0] == "ct", "type='lxc' must be normalised to 'ct'"


# ─────────────────────────────────────────────────────────────────────────────
# ACTIVE_JOBS — /api/jobs/active endpoint
# ─────────────────────────────────────────────────────────────────────────────

class TestActiveJobs:

    def test_active_jobs_empty_when_no_jobs(self, flask_client):
        """No jobs running → empty list."""
        import jobs as _jobs
        _jobs._jobs.clear()
        resp = flask_client.get("/api/jobs/active")
        assert resp.status_code == 200
        assert json.loads(resp.data) == []

    def test_active_jobs_returns_running_job(self, flask_client):
        """A running job appears in /api/jobs/active."""
        import jobs as _jobs
        _jobs._jobs.clear()
        job_id = _jobs.create_job("Test running job")
        with _jobs._lock:
            _jobs._jobs[job_id]["status"] = "running"
        resp = flask_client.get("/api/jobs/active")
        data = json.loads(resp.data)
        assert any(j["id"] == job_id for j in data)
        assert data[0]["status"] == "running"

    def test_active_jobs_excludes_done(self, flask_client):
        """Done jobs do not appear in /api/jobs/active."""
        import jobs as _jobs
        _jobs._jobs.clear()
        job_id = _jobs.create_job("Done job")
        with _jobs._lock:
            _jobs._jobs[job_id]["status"] = "done"
        resp = flask_client.get("/api/jobs/active")
        data = json.loads(resp.data)
        assert not any(j["id"] == job_id for j in data)

    def test_active_jobs_excludes_error(self, flask_client):
        """Error jobs do not appear in /api/jobs/active."""
        import jobs as _jobs
        _jobs._jobs.clear()
        job_id = _jobs.create_job("Error job")
        with _jobs._lock:
            _jobs._jobs[job_id]["status"] = "error"
        resp = flask_client.get("/api/jobs/active")
        data = json.loads(resp.data)
        assert not any(j["id"] == job_id for j in data)

    def test_active_jobs_returns_pending(self, flask_client):
        """Pending jobs (created but not yet started) appear in /api/jobs/active."""
        import jobs as _jobs
        _jobs._jobs.clear()
        job_id = _jobs.create_job("Pending job")
        resp = flask_client.get("/api/jobs/active")
        data = json.loads(resp.data)
        assert any(j["id"] == job_id for j in data)


# ─────────────────────────────────────────────────────────────────────────────
# RESTIC_BUSY — items and restic/snapshots behaviour when restic is locked
# ─────────────────────────────────────────────────────────────────────────────

class TestResticBusy:

    def test_items_restic_busy_flag_false_when_idle(self, flask_client, mock_host):
        """restic_busy=False in items response when restic is not running."""
        with _mock_clients(mock_host,
                           pbs_snaps=[_pbs_group(301, "ct", [_pbs_snap(T2)])],
                           pve_meta=_pve_meta(),
                           restic_by_vm={}) as (_, _, res_mock):
            res_mock.is_running.return_value = False
            resp = flask_client.get(f"/api/host/{HOST_ID}/items")
        data = json.loads(resp.data)
        assert data.get("restic_busy") is False

    def test_items_restic_busy_flag_true_when_locked(self, flask_client, mock_host):
        """restic_busy=True in items response when restic is locked."""
        _app._restic_snap_cache.clear()
        with _mock_clients(mock_host,
                           pbs_snaps=[_pbs_group(301, "ct", [_pbs_snap(T2)])],
                           pve_meta=_pve_meta(),
                           restic_by_vm={}) as (_, _, res_mock):
            res_mock.is_running.return_value = True
            resp = flask_client.get(f"/api/host/{HOST_ID}/items")
        data = json.loads(resp.data)
        assert data.get("restic_busy") is True

    def test_items_uses_cached_by_vm_when_busy(self, flask_client, mock_host):
        """When restic is busy, items endpoint uses cached by_vm coverage."""
        cached_by_vm = {301: [{"ts": TS_R2, "id": "R2id", "short_id": "R2", "pbs_time": T2}]}
        _app._restic_snap_cache[f"by_vm:{HOST_ID}"] = (cached_by_vm, [])

        with _mock_clients(mock_host,
                           pbs_snaps=[_pbs_group(301, "ct", [_pbs_snap(T2)])],
                           pve_meta=_pve_meta(),
                           restic_by_vm={}) as (_, _, res_mock):
            res_mock.is_running.return_value = True
            resp = flask_client.get(f"/api/host/{HOST_ID}/items")
        data = json.loads(resp.data)
        ct = next(x for x in data["lxcs"] if x["id"] == 301)
        snap = ct["snapshots"][0]
        # Cloud coverage should come from cache, not empty restic_by_vm
        assert snap.get("cloud") is True

    def test_restic_snapshots_503_when_busy_no_cache(self, flask_client, mock_host):
        """503 when restic is busy and no cache exists yet."""
        _app._restic_snap_cache.clear()
        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.ResticClient") as restic_cls:
            restic_cls.return_value.is_running.return_value = True
            resp = flask_client.get(f"/api/host/{HOST_ID}/restic/snapshots")
        assert resp.status_code == 503
        data = json.loads(resp.data)
        assert data.get("busy") is True

    def test_restic_snapshots_returns_cache_when_busy(self, flask_client, mock_host):
        """Returns cached snaps + restic_busy=True when restic is locked."""
        cached = [{"id": "abc123", "short_id": "abc123", "ts": TS_R2,
                   "size_bytes": 1024, "covers": []}]
        _app._restic_snap_cache[f"flat:{HOST_ID}"] = cached

        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.ResticClient") as restic_cls, \
             patch("app.PBSClient") as pbs_cls:
            restic_cls.return_value.is_running.return_value = True
            pbs_cls.return_value.get_snapshots.return_value = []
            resp = flask_client.get(f"/api/host/{HOST_ID}/restic/snapshots")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["restic_busy"] is True
        assert len(data["snaps"]) == 1
        assert data["snaps"][0]["short_id"] == "abc123"

    def test_restic_snapshots_updates_cache_on_success(self, flask_client, mock_host):
        """Successful fetch updates _restic_snap_cache."""
        _app._restic_snap_cache.clear()
        fresh = [{"id": "fresh1", "short_id": "fresh1", "ts": TS_R2,
                  "size_bytes": 512, "covers": []}]

        with patch.dict(_app.HOSTS, {HOST_ID: mock_host}, clear=True), \
             patch("app.ResticClient") as restic_cls, \
             patch("app.PBSClient") as pbs_cls:
            restic_cls.return_value.is_running.return_value = False
            restic_cls.return_value.get_snapshots_flat.return_value = fresh
            pbs_cls.return_value.get_snapshots.return_value = []
            resp = flask_client.get(f"/api/host/{HOST_ID}/restic/snapshots")
        assert resp.status_code == 200
        assert f"flat:{HOST_ID}" in _app._restic_snap_cache
        assert _app._restic_snap_cache[f"flat:{HOST_ID}"][0]["short_id"] == "fresh1"


# ─────────────────────────────────────────────────────────────────────────────
# PROGRESS — _ssh_stream heartbeat and restic JSON parsing
# ─────────────────────────────────────────────────────────────────────────────

class TestProgress:

    def _make_stream(self, lines: list[str]):
        """Return a ResticClient with _ssh_stream mocked to emit lines."""
        from restic_client import ResticClient
        from config import HostConfig
        host = HostConfig(
            id="x", label="x",
            pve_url="https://1.2.3.4:8006", pve_user="u", pve_password="p",
            pbs_url="https://1.2.3.4:8007", pbs_user="u", pbs_password="p",
            pbs_datastore="d", pbs_storage_id="s", pbs_datastore_path="/mnt/d",
            restic_repo="rclone:gdrive:test", restic_password="pw",
        )
        client = ResticClient.__new__(ResticClient)
        client._repo = "rclone:gdrive:test"
        client._gdrive_remote = "gdrive"
        client._ssh_host = "1.2.3.4"
        client._env_prefix = "RESTIC_REPOSITORY=rclone:gdrive:test RESTIC_PASSWORD=pw"
        return client

    def test_heartbeat_emitted_when_silent(self):
        """Heartbeat '[Xs elapsed]' injected when no output for heartbeat_secs."""
        from restic_client import ResticClient

        client = self._make_stream([])
        logged = []

        # stdout blocks for 0.15s then yields nothing — heartbeat should fire
        def _slow_iter():
            time.sleep(0.15)
            return
            yield  # make it a generator

        class FakeProc:
            returncode = 0
            stdout = _slow_iter()
            def wait(self): pass

        with patch("restic_client.subprocess.Popen", return_value=FakeProc()):
            # heartbeat_secs=0.05, _check_interval=0.01 → fires at least once in 0.15s
            client._ssh_stream("cmd", logged.append, heartbeat_secs=0.05, _check_interval=0.01)

        assert any("[" in l and "elapsed]" in l for l in logged), \
            f"Expected heartbeat line in logs, got: {logged}"

    def test_parse_json_status_line_emits_progress(self):
        """restic --json status lines become '[X% — ...]' progress messages."""
        from restic_client import ResticClient

        client = self._make_stream([])
        logged = []

        status_line = json.dumps({
            "message_type": "status",
            "percent_done": 0.45,
            "seconds_elapsed": 23,
            "files_done": 180,
            "total_files": 400,
            "bytes_done": 245366784,
        })

        class FakeProc:
            returncode = 0
            stdout = iter([status_line + "\n"])
            def wait(self): pass

        with patch("restic_client.subprocess.Popen", return_value=FakeProc()):
            client._ssh_stream("cmd", logged.append, parse_json=True)

        assert len(logged) == 1
        assert logged[0].startswith("[45%")
        assert "180/400" in logged[0]

    def test_parse_json_summary_line_emits_uploaded(self):
        """restic --json summary line becomes 'Uploaded X MB' message."""
        from restic_client import ResticClient

        client = self._make_stream([])
        logged = []

        summary_line = json.dumps({
            "message_type": "summary",
            "data_added_packed": 10 * 1024 * 1024,  # 10 MB
        })

        class FakeProc:
            returncode = 0
            stdout = iter([summary_line + "\n"])
            def wait(self): pass

        with patch("restic_client.subprocess.Popen", return_value=FakeProc()):
            client._ssh_stream("cmd", logged.append, parse_json=True)

        assert len(logged) == 1
        assert "10 MB" in logged[0]

    def test_non_json_lines_passed_through(self):
        """Non-JSON lines are forwarded unchanged when parse_json=True."""
        from restic_client import ResticClient

        client = self._make_stream([])
        logged = []

        class FakeProc:
            returncode = 0
            stdout = iter(["repacking packs\n", "some other line\n"])
            def wait(self): pass

        with patch("restic_client.subprocess.Popen", return_value=FakeProc()):
            client._ssh_stream("cmd", logged.append, parse_json=True)

        assert "repacking packs" in logged
        assert "some other line" in logged

    def test_nonzero_exit_raises(self):
        """Non-zero exit code raises RuntimeError."""
        from restic_client import ResticClient

        client = self._make_stream([])

        class FakeProc:
            returncode = 1
            stdout = iter([])
            def wait(self): pass

        with patch("restic_client.subprocess.Popen", return_value=FakeProc()):
            with pytest.raises(RuntimeError):
                client._ssh_stream("cmd", lambda _: None)
