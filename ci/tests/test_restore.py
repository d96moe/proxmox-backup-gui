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


@pytest.fixture
def real_page(browser):
    ctx = browser.new_context(base_url=BACKEND_URL)
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

    # Find and click the backup button for this VM — opens backup-type modal
    backup_btn = real_page.locator(f".backup-btn[data-vmid='{vmid}']").first
    backup_btn.wait_for(timeout=5000)
    backup_btn.click()

    # Backup-type modal opens — select PBS-only and confirm
    real_page.wait_for_selector("#backup-modal.open", timeout=5000)
    real_page.locator(".source-opt[data-btype='pbs']").click()
    real_page.click("#backup-confirm-btn")

    # Job modal must open
    real_page.wait_for_selector("#job-modal.open", timeout=8000)

    # Wait for completion (backup can take a while); close-btn is always enabled now
    real_page.wait_for_function(
        "() => ['done','error'].includes(document.getElementById('job-badge').innerText)",
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

    # Restore buttons live inside the collapsible .snapshots section (display:none
    # by default). Click the vm-header to expand it first.
    real_page.locator(f".vm-card:has(.backup-btn[data-vmid='{vmid}'])").locator(".vm-header").click()

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
    # PVE restore tasks take a few minutes; close-btn is always enabled now
    real_page.wait_for_function(
        "() => ['done','error'].includes(document.getElementById('job-badge').innerText)",
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

    # Expand the VM card so the restore buttons inside .snapshots become visible
    real_page.locator(f".vm-card:has(.backup-btn[data-vmid='{vmid}'])").locator(".vm-header").click()

    # Click first Restore button for this VM that has cloud coverage
    restore_btn = real_page.locator(f".restore-btn[data-vmid='{vmid}'][data-cloud='true']").first
    restore_btn.wait_for(timeout=5000)
    restore_btn.click()

    real_page.wait_for_selector("#modal.open", timeout=5000)
    real_page.locator(".source-opt[data-source='cloud']").click()

    # Confirm (leave run_backup_after unchecked — default)
    real_page.click("#modal-confirm-btn")

    real_page.wait_for_selector("#job-modal.open", timeout=5000)
    # close-btn is always enabled now — wait for badge instead
    real_page.wait_for_function(
        "() => ['done','error'].includes(document.getElementById('job-badge').innerText)",
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


# ─────────────────────────────────────────────────────────────────────────────
# ERROR HANDLING — bad inputs and failure paths must produce readable errors
# ─────────────────────────────────────────────────────────────────────────────

def test_cloud_restore_invalid_restic_id_shows_error(host_id):
    """Passing a non-existent restic snapshot ID must produce job status=error
    with a readable error message — not a 500 or a silent hang."""
    resp = _post(f"/api/host/{host_id}/restore", {
        "vmid": 101, "type": "vm",
        "source": "cloud", "restic_id": "0000000000000000deadbeef",
        "run_backup_after": False,
    })
    assert "job_id" in resp

    job = _poll_job(resp["job_id"], timeout=120)
    assert job["status"] == "error", \
        f"Expected error for invalid restic_id, got: {job['status']}"
    logs = "\n".join(job.get("logs", []))
    assert logs.strip(), "Error job must have log output — got empty logs"
    # The log must contain something human-readable (not just a traceback class name)
    assert any(word in logs.lower() for word in ("error", "failed", "not found", "exit")), \
        f"Error log does not contain a readable message:\n{logs}"


def test_local_restore_invalid_backup_time_shows_error(host_id):
    """Passing backup_time=1 (nonexistent timestamp) must produce job status=error."""
    resp = _post(f"/api/host/{host_id}/restore", {
        "vmid": 101, "type": "vm",
        "source": "local", "backup_time": 1,
    })
    assert "job_id" in resp

    job = _poll_job(resp["job_id"], timeout=120)
    assert job["status"] == "error", \
        f"Expected error for invalid backup_time, got: {job['status']}"
    logs = "\n".join(job.get("logs", []))
    assert logs.strip(), "Error job must have log output"


def test_restore_nonexistent_vmid_shows_error(host_id):
    """Requesting restore for a vmid that doesn't exist must produce job status=error."""
    resp = _post(f"/api/host/{host_id}/restore", {
        "vmid": 99999, "type": "vm",
        "source": "local", "backup_time": 1700000000,
    })
    assert "job_id" in resp

    job = _poll_job(resp["job_id"], timeout=120)
    assert job["status"] == "error", \
        f"Expected error for vmid=99999, got: {job['status']}"


def test_concurrent_backups_both_get_job_ids(host_id, items):
    """Triggering two backups simultaneously must return a job_id for each request.
    PVE may queue or reject the second, but the API must not crash."""
    vmid, vm_type, _ = _find_vm_with_local_snap(items)
    if vmid is None:
        pytest.skip("No suitable VM for concurrent backup test")

    import concurrent.futures

    def start_backup(_):
        return _post(f"/api/host/{host_id}/backup/pbs", {"vmid": vmid, "type": vm_type})

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        futures = [ex.submit(start_backup, i) for i in range(2)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    for resp in results:
        assert "job_id" in resp, f"Missing job_id in concurrent backup response: {resp}"

    # Wait for both to settle — PBS may reject the second concurrent vzdump for the
    # same CT (returns "job errors"). Both erroring is acceptable; the API must not
    # crash and both must settle without hanging.
    jobs = [_poll_job(r["job_id"], timeout=300) for r in results]
    statuses = [j["status"] for j in jobs]
    assert all(s in ("done", "error") for s in statuses), \
        f"Unexpected job statuses: {statuses}"


def test_backup_post_restore_data_appears_in_gui(real_page, host_id, items):
    """After a successful local restore, the GUI must refresh and show updated snapshot data."""
    vmid, vm_type, backup_time = _find_vm_with_local_snap(items)
    if vmid is None:
        pytest.skip("No VM with local PBS snapshot")

    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        f"() => document.getElementById('content').innerText.includes('{vmid}')",
        timeout=10000,
    )

    # Expand the VM card so the restore buttons inside .snapshots become visible
    real_page.locator(f".vm-card:has(.backup-btn[data-vmid='{vmid}'])").locator(".vm-header").click()

    restore_btn = real_page.locator(f".restore-btn[data-vmid='{vmid}']").first
    restore_btn.wait_for(timeout=5000)
    restore_btn.click()
    real_page.wait_for_selector("#modal.open", timeout=5000)
    real_page.locator(".source-opt[data-source='local']").click()
    real_page.click("#modal-confirm-btn")
    real_page.wait_for_selector("#job-modal.open", timeout=5000)
    # close-btn is always enabled now — wait for badge instead
    real_page.wait_for_function(
        "() => ['done','error'].includes(document.getElementById('job-badge').innerText)",
        timeout=300000,
    )

    badge = real_page.locator("#job-badge").inner_text()
    assert badge == "done", \
        f"Restore failed — cannot check GUI refresh.\nLog:\n{real_page.locator('#job-log').inner_text()}"

    real_page.evaluate("closeJobModal()")

    # After close, refreshData() is called — VM content must still be present
    real_page.wait_for_function(
        f"() => document.getElementById('content').innerText.includes('{vmid}')",
        timeout=15000,
    )
    assert real_page._js_errors == [], f"JS errors after restore + GUI refresh: {real_page._js_errors}"


# ─────────────────────────────────────────────────────────────────────────────
# API FIELDS — new backend fields (dedup_factor, in_pve) introduced for GUI features
# ─────────────────────────────────────────────────────────────────────────────

def test_storage_api_returns_dedup_factor(host_id):
    """GET /api/host/<id>/storage must include dedup_factor field (may be None if GC not run)."""
    storage = _get(f"/api/host/{host_id}/storage")
    assert "dedup_factor" in storage, \
        f"dedup_factor missing from storage response — PBS GC endpoint not wired up.\n{storage}"

def test_storage_dedup_factor_is_number_or_none(host_id):
    """dedup_factor must be a positive float or None — never a string or negative number."""
    storage = _get(f"/api/host/{host_id}/storage")
    dedup = storage.get("dedup_factor")
    if dedup is not None:
        assert isinstance(dedup, (int, float)), \
            f"dedup_factor must be numeric, got {type(dedup).__name__}: {dedup!r}"
        assert dedup > 0, f"dedup_factor must be positive, got: {dedup}"

def test_items_api_returns_in_pve_field(host_id, items):
    """Every VM and LXC in /api/host/<id>/items must have an in_pve boolean field."""
    for section in ("vms", "lxcs"):
        for item in items.get(section, []):
            assert "in_pve" in item, \
                f"in_pve missing from {section} item id={item.get('id')}"
            assert isinstance(item["in_pve"], bool), \
                f"in_pve must be bool for id={item.get('id')}, got {type(item['in_pve']).__name__}"

def test_items_in_pve_true_for_existing_vms(host_id, items):
    """VMs/LXCs that are in PVE's inventory must have in_pve=True."""
    for section in ("vms", "lxcs"):
        for item in items.get(section, []):
            # All items returned by PVE should be in PVE — in_pve=False only for
            # orphaned PBS snapshots whose source VM was deleted from PVE.
            # At minimum, items with local PBS snapshots must have in_pve=True
            # (otherwise the CI template is misconfigured).
            local_snaps = [s for s in item.get("snapshots", []) if s.get("local")]
            if local_snaps:
                assert item["in_pve"], \
                    f"VM {item['id']} has local PBS snapshots but in_pve=False — " \
                    f"check if VM was accidentally deleted from PVE"


# ─────────────────────────────────────────────────────────────────────────────
# GUI FEATURES — dedup display, backup button visibility based on in_pve
# ─────────────────────────────────────────────────────────────────────────────

def test_dedup_shown_in_sidebar(real_page, host_id):
    """GUI sidebar must show PBS dedup factor (pbs-dedup element must not be '—')."""
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    # Wait for storage data to load — dedup may need a moment
    try:
        real_page.wait_for_function(
            "() => document.getElementById('pbs-dedup').innerText !== '—'",
            timeout=10000,
        )
        dedup_text = real_page.locator("#pbs-dedup").inner_text()
        # Must contain a number (e.g. "4.2×")
        assert any(c.isdigit() for c in dedup_text), \
            f"pbs-dedup text does not contain a number: {dedup_text!r}"
    except Exception:
        # If GC hasn't run on CI PBS, dedup_factor may legitimately be None → '—'
        dedup_text = real_page.locator("#pbs-dedup").inner_text()
        if dedup_text == "—":
            import warnings
            warnings.warn(
                "pbs-dedup shows '—' — PBS GC may not have run on CI instance. "
                "Run 'proxmox-backup-manager garbage-collection start' to generate GC data."
            )
        else:
            raise

def test_backup_btn_absent_for_non_pve_items(real_page, host_id, items):
    """Backup now button must be absent for items with in_pve=False."""
    non_pve = [
        item for section in ("vms", "lxcs")
        for item in items.get(section, [])
        if not item.get("in_pve", True)
    ]
    if not non_pve:
        pytest.skip("No in_pve=False items in CI data — skipping backup button visibility test")

    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    for item in non_pve:
        vmid = item["id"]
        real_page.wait_for_function(
            f"() => document.getElementById('content').innerText.includes('{vmid}')",
            timeout=10000,
        )
        backup_btns = real_page.locator(
            f".backup-btn[data-vmid='{vmid}']"
        ).filter(has_text="Backup")
        assert backup_btns.count() == 0, \
            f"Backup now button shown for in_pve=False item vmid={vmid}"
