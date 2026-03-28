"""
Restore / backup integration tests — run against a REAL Flask backend.

BACKEND_URL=http://<ip>:5000 must be set (Jenkinsfile.integration sets it automatically).
All tests are skipped when BACKEND_URL is absent.

Prerequisites baked into CI template 9000:
  - PBS running with at least one VM/LXC snapshot in the datastore
  - LXC 300 has passwordless SSH access to the PVE host (required for cloud restore)
  - hosts.json configured: pve_url, pbs_url, pbs_storage_id, pbs_datastore_path
  - (optional) restic_repo + restic_password configured for cloud restore tests

Test categories:
  BACKUP  — trigger on-demand PBS backup via GUI
  LOCAL   — restore VM from a local PBS snapshot
  CLOUD   — restore PBS datastore from restic, then restore VM (SSH to PVE host)
  CLOUD-ONLY — same but using a snapshot that no longer exists in local PBS
"""
from __future__ import annotations

import json
import os
import time
import urllib.request

import pytest
from playwright.sync_api import sync_playwright

BACKEND_URL = os.environ.get("BACKEND_URL", "").rstrip("/")

pytestmark = pytest.mark.skipif(
    not BACKEND_URL,
    reason="BACKEND_URL not set — set BACKEND_URL=http://<ip>:5000 to run restore tests",
)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get(path: str):
    return json.loads(urllib.request.urlopen(f"{BACKEND_URL}{path}", timeout=30).read())


def _post(path: str, body: dict) -> dict:
    req = urllib.request.Request(
        f"{BACKEND_URL}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return json.loads(urllib.request.urlopen(req, timeout=30).read())


def _poll_job(job_id: str, timeout: int = 360) -> dict:
    """Poll /api/job/<id> until done or error. Returns final job dict."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = _get(f"/api/job/{job_id}")
        if job["status"] in ("done", "error"):
            return job
        time.sleep(4)
    raise TimeoutError(f"Job {job_id} did not finish within {timeout}s")


def _job_ok(job: dict) -> str:
    """Return empty string on success, or a formatted error summary."""
    if job["status"] == "done":
        return ""
    logs = "\n  ".join(job.get("logs", []))
    return f"status={job['status']}\n  {logs}"


# ─────────────────────────────────────────────────────────────────────────────
# Data helpers
# ─────────────────────────────────────────────────────────────────────────────

def _first_host() -> str:
    hosts = _get("/api/hosts")
    assert hosts, "No hosts configured on backend"
    return hosts[0]["id"]


def _items(host_id: str) -> dict:
    return _get(f"/api/host/{host_id}/items")


def _find_vm_with_local_snap(items: dict):
    """Return (vmid, vm_type, backup_time) — LXCs preferred (faster to restore)."""
    for key, vtype in (("lxcs", "ct"), ("vms", "vm")):
        for item in items.get(key, []):
            if item.get("template"):
                continue
            local = [s for s in item["snapshots"] if s.get("local")]
            if local:
                return item["id"], vtype, local[0]["backup_time"]
    return None, None, None


def _find_vm_with_any_cloud_snap(items: dict):
    """Return (vmid, vm_type, restic_id) — any cloud snapshot (may also be local)."""
    for key, vtype in (("lxcs", "ct"), ("vms", "vm")):
        for item in items.get(key, []):
            if item.get("template"):
                continue
            for snap in item["snapshots"]:
                if snap.get("cloud") and snap.get("restic_id"):
                    return item["id"], vtype, snap["restic_id"]
    return None, None, None


def _find_vm_with_cloud_only_snap(items: dict):
    """Return (vmid, vm_type, restic_id) — snapshot in restic but NOT in local PBS."""
    for key, vtype in (("lxcs", "ct"), ("vms", "vm")):
        for item in items.get(key, []):
            if item.get("template"):
                continue
            for snap in item["snapshots"]:
                if snap.get("cloud") and not snap.get("local") and snap.get("restic_id"):
                    return item["id"], vtype, snap["restic_id"]
    return None, None, None


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def host_id():
    return _first_host()


@pytest.fixture(scope="module")
def items(host_id):
    return _items(host_id)


@pytest.fixture(scope="module")
def browser_instance():
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        yield b
        b.close()


@pytest.fixture
def real_page(browser_instance):
    ctx = browser_instance.new_context(base_url=BACKEND_URL)
    pg = ctx.new_page()
    pg._js_errors = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    pg.goto("/")
    pg.wait_for_function(
        "() => document.getElementById('content').innerText !== 'Loading…'",
        timeout=15000,
    )
    yield pg
    ctx.close()


# ─────────────────────────────────────────────────────────────────────────────
# BACKUP — trigger on-demand PBS backup
# ─────────────────────────────────────────────────────────────────────────────

def test_backup_pbs_api(host_id, items):
    """POST /backup/pbs → job must complete with status=done."""
    vmid, vm_type, _ = _find_vm_with_local_snap(items)
    if vmid is None:
        pytest.skip("No suitable VM found for backup test")

    resp = _post(f"/api/host/{host_id}/backup/pbs", {"vmid": vmid, "type": vm_type})
    assert "job_id" in resp, f"No job_id in response: {resp}"

    job = _poll_job(resp["job_id"], timeout=180)
    err = _job_ok(job)
    assert not err, f"PBS backup job failed:\n{err}"


def test_backup_now_button_ui(real_page, host_id, items):
    """Clicking 'Backup now' on a VM card starts a job and the modal appears."""
    vmid, _, _ = _find_vm_with_local_snap(items)
    if vmid is None:
        pytest.skip("No suitable VM found for backup button test")

    # Navigate to correct host
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    # Wait for VM cards to render
    real_page.wait_for_function(
        f"() => document.getElementById('content').innerText.includes('{vmid}')",
        timeout=10000,
    )

    # Find and click the backup button for this VM
    backup_btn = real_page.locator(f".backup-btn[data-vmid='{vmid}']").first
    backup_btn.wait_for(timeout=5000)
    # Dismiss the confirm dialog
    real_page.on("dialog", lambda d: d.accept())
    backup_btn.click()

    # Job modal must open
    real_page.wait_for_selector("#job-modal.open", timeout=8000)

    # Wait for completion (backup can take a while)
    real_page.wait_for_function(
        "() => !document.getElementById('job-close-btn').disabled",
        timeout=240000,
    )
    badge = real_page.locator("#job-badge").inner_text()
    log_text = real_page.locator("#job-log").inner_text()
    assert badge == "done", f"Backup now failed. Badge: {badge}\nLog:\n{log_text}"
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"
    real_page.evaluate("closeJobModal()")


# ─────────────────────────────────────────────────────────────────────────────
# LOCAL — restore from PBS snapshot via UI
# ─────────────────────────────────────────────────────────────────────────────

def test_local_restore_ui(real_page, host_id, items):
    """UI: open restore modal, select local source, confirm — job must complete."""
    vmid, vm_type, _ = _find_vm_with_local_snap(items)
    if vmid is None:
        pytest.skip("No VM with local PBS snapshot — skipping local restore UI test")

    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        f"() => document.getElementById('content').innerText.includes('{vmid}')",
        timeout=10000,
    )

    # Click the first Restore button for this VM (latest snapshot row)
    restore_btn = real_page.locator(f".restore-btn[data-vmid='{vmid}']").first
    restore_btn.wait_for(timeout=5000)
    restore_btn.click()

    # Modal opens — select local source
    real_page.wait_for_selector("#modal.open", timeout=5000)
    real_page.locator(".source-opt[data-source='local']").click()

    # Confirm
    real_page.click("#modal-confirm-btn")

    # Job modal must appear
    real_page.wait_for_selector("#job-modal.open", timeout=5000)
    # PVE restore tasks take a few minutes
    real_page.wait_for_function(
        "() => !document.getElementById('job-close-btn').disabled",
        timeout=300000,
    )
    badge = real_page.locator("#job-badge").inner_text()
    log_text = real_page.locator("#job-log").inner_text()
    assert badge == "done", f"Local restore failed. Badge: {badge}\nLog:\n{log_text}"
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"
    real_page.evaluate("closeJobModal()")


# ─────────────────────────────────────────────────────────────────────────────
# CLOUD — restore PBS datastore from restic, then restore VM
# ─────────────────────────────────────────────────────────────────────────────

def test_cloud_restore_api(host_id, items):
    """POST /restore with source=cloud — restores PBS datastore via SSH then restores VM."""
    vmid, vm_type, restic_id = _find_vm_with_any_cloud_snap(items)
    if vmid is None:
        pytest.skip("No cloud snapshots found — configure restic_repo in hosts.json")

    resp = _post(f"/api/host/{host_id}/restore", {
        "vmid": vmid, "type": vm_type,
        "source": "cloud", "restic_id": restic_id,
        "run_backup_after": False,
    })
    assert "job_id" in resp

    # Cloud restore is slow (restic download + PBS restart + VM restore)
    job = _poll_job(resp["job_id"], timeout=600)
    err = _job_ok(job)
    assert not err, f"Cloud restore failed:\n{err}"


def test_cloud_restore_with_backup_after_api(host_id, items):
    """Cloud restore with run_backup_after=True — PBS backup runs after VM is restored."""
    vmid, vm_type, restic_id = _find_vm_with_any_cloud_snap(items)
    if vmid is None:
        pytest.skip("No cloud snapshots found — configure restic_repo in hosts.json")

    resp = _post(f"/api/host/{host_id}/restore", {
        "vmid": vmid, "type": vm_type,
        "source": "cloud", "restic_id": restic_id,
        "run_backup_after": True,
    })
    assert "job_id" in resp

    # Extra time budget for the post-restore backup
    job = _poll_job(resp["job_id"], timeout=900)
    err = _job_ok(job)
    assert not err, f"Cloud restore + post-backup failed:\n{err}"

    logs = "\n".join(job.get("logs", []))
    assert "Post-restore backup complete" in logs or "Backup complete" in logs, \
        f"Post-restore backup log line missing.\nLogs:\n{logs}"


def test_cloud_restore_ui(real_page, host_id, items):
    """UI: open restore modal, select cloud source, confirm — job must complete."""
    vmid, vm_type, _ = _find_vm_with_any_cloud_snap(items)
    if vmid is None:
        pytest.skip("No cloud snapshots — skipping cloud restore UI test")

    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        f"() => document.getElementById('content').innerText.includes('{vmid}')",
        timeout=10000,
    )

    # Click first Restore button for this VM
    restore_btn = real_page.locator(f".restore-btn[data-vmid='{vmid}']").first
    restore_btn.wait_for(timeout=5000)
    restore_btn.click()

    real_page.wait_for_selector("#modal.open", timeout=5000)
    real_page.locator(".source-opt[data-source='cloud']").click()

    # Confirm (leave run_backup_after unchecked — default)
    real_page.click("#modal-confirm-btn")

    real_page.wait_for_selector("#job-modal.open", timeout=5000)
    real_page.wait_for_function(
        "() => !document.getElementById('job-close-btn').disabled",
        timeout=600000,
    )
    badge = real_page.locator("#job-badge").inner_text()
    log_text = real_page.locator("#job-log").inner_text()
    assert badge == "done", f"Cloud restore failed. Badge: {badge}\nLog:\n{log_text}"
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"
    real_page.evaluate("closeJobModal()")


# ─────────────────────────────────────────────────────────────────────────────
# CLOUD-ONLY — restore from a snapshot that no longer exists in local PBS
# This is the primary disaster-recovery scenario.
# ─────────────────────────────────────────────────────────────────────────────

def test_cloud_only_restore_api(host_id, items):
    """Restore from a cloud-only restic snapshot (local PBS copy has been pruned).

    Skipped if no cloud-only snapshots are present — they appear when local PBS
    retention prunes old snapshots that have already been uploaded to restic.
    """
    vmid, vm_type, restic_id = _find_vm_with_cloud_only_snap(items)
    if vmid is None:
        pytest.skip(
            "No cloud-only snapshots found — these appear after local PBS retention "
            "prunes old snapshots that were already uploaded to restic. "
            "Run the nightly backup+prune cycle first."
        )

    resp = _post(f"/api/host/{host_id}/restore", {
        "vmid": vmid, "type": vm_type,
        "source": "cloud", "restic_id": restic_id,
        "run_backup_after": False,
    })
    assert "job_id" in resp

    job = _poll_job(resp["job_id"], timeout=600)
    err = _job_ok(job)
    assert not err, f"Cloud-only restore failed:\n{err}"
