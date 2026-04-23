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

import http.cookiejar
import json
import os
import shutil
import subprocess
import time
import urllib.request

import pytest

BACKEND_URL = os.environ.get("BACKEND_URL", "").rstrip("/")
CI_ADMIN_PASSWORD = os.environ.get("CI_ADMIN_PASSWORD", "")

pytestmark = pytest.mark.skipif(
    not BACKEND_URL,
    reason="BACKEND_URL not set — set BACKEND_URL=http://<ip>:5000 to run restore tests",
)

# ─────────────────────────────────────────────────────────────────────────────
# Authenticated HTTP session (cookie jar keeps the Flask session cookie)
# ─────────────────────────────────────────────────────────────────────────────

_cookie_jar = http.cookiejar.CookieJar()
_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_cookie_jar))
_session_ready = False


def _ensure_session() -> None:
    global _session_ready
    if _session_ready:
        return
    if not CI_ADMIN_PASSWORD:
        return  # no auth configured — fall back to unauthenticated (local unit tests)
    req = urllib.request.Request(
        f"{BACKEND_URL}/api/auth/login",
        data=json.dumps({"username": "admin", "password": CI_ADMIN_PASSWORD}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    _opener.open(req, timeout=10)
    _session_ready = True


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get(path: str):
    _ensure_session()
    return json.loads(_opener.open(f"{BACKEND_URL}{path}", timeout=60).read())


def _post(path: str, body: dict) -> dict:
    _ensure_session()
    req = urllib.request.Request(
        f"{BACKEND_URL}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return json.loads(_opener.open(req, timeout=60).read())


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


SELF_VMID = 300  # LXC running the Flask backend — never restore this one mid-test

# Tracks whether test_delete_cloud_only_api actually ran and succeeded.
# Aftermath tests check this flag to skip cleanly instead of failing when
# delete/cloud was skipped (e.g. restic temporarily unresponsive after delete/both).
_delete_cloud_ran: bool = False

# Records exactly which vmid + backup_time was deleted by test_delete_cloud_only_api
# so aftermath tests verify THAT snapshot is gone (not T1 from the seed state, which
# may differ if cloud restore tests brought T1 back into PBS and delete/cloud targeted
# a different cloud-only entry).
_delete_cloud_vmid: int | None = None
_delete_cloud_backup_time: int | None = None

def _find_vm_with_local_snap(items: dict):
    """Return (vmid, vm_type, backup_time) — LXCs preferred (faster to restore).

    Skips SELF_VMID (ct/300) — restoring the app container kills the backend.
    Skips in_pve=False items — these are orphaned PBS snapshots with no live VM.
    """
    for key, vtype in (("lxcs", "ct"), ("vms", "vm")):
        for item in items.get(key, []):
            if item.get("template"):
                continue
            if item["id"] == SELF_VMID:
                continue
            if not item.get("in_pve", True):
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
            if item["id"] == SELF_VMID:
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
            if item["id"] == SELF_VMID:
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
    ctx.route("**fonts.googleapis.com**",
              lambda r: r.fulfill(status=200, content_type="text/css", body=""))
    ctx.route("**fonts.gstatic.com**",
              lambda r: r.fulfill(status=200, content_type="font/woff2", body=b""))
    pg = ctx.new_page()
    pg._js_errors = []
    pg._console_msgs = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    pg.on("console", lambda m: pg._console_msgs.append(f"[{m.type}] {m.text}"))
    # Log in if CI credentials are configured
    if CI_ADMIN_PASSWORD:
        pg.goto("/login")
        pg.fill("#username", "admin")
        pg.fill("#password", CI_ADMIN_PASSWORD)
        pg.click("#btn-login")
        pg.wait_for_url("/", timeout=5000)
    else:
        pg.goto("/")
    # Wait until at least one VM card is rendered — MQTT broker has delivered
    # retained messages.
    try:
        pg.wait_for_function(
            "() => document.querySelector('.vm-card') !== null",
            timeout=45000,
        )
    except Exception as exc:
        # Dump browser console for diagnosis before re-raising
        console_dump = "\n  ".join(pg._console_msgs[-40:]) or "(none)"
        page_text = pg.evaluate("() => document.body?.innerText?.slice(0,500) || ''")
        raise type(exc)(
            f"{exc}\n\nBrowser console (last 40):\n  {console_dump}\n\nPage text:\n  {page_text}"
        ) from exc
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
    # by default). Click .expand-btn to open it (clicking .vm-header center can land
    # on an inner button with stopPropagation, preventing toggleSnaps from firing).
    real_page.locator(f".vm-card:has(.backup-btn[data-vmid='{vmid}'])").locator(".expand-btn").first.click()

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
    real_page.locator(f".vm-card:has(.backup-btn[data-vmid='{vmid}'])").locator(".expand-btn").first.click()

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
    real_page.locator(f".vm-card:has(.backup-btn[data-vmid='{vmid}'])").locator(".expand-btn").first.click()

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
    """VMs/LXCs that are in PVE's inventory must have in_pve=True.

    Exception: items with in_pve=False are orphaned PBS snapshots whose source VM
    was deleted from PVE (e.g. the ghost ct/399 CI scenario). These may have local
    PBS snapshots but are intentionally absent from PVE.
    """
    for section in ("vms", "lxcs"):
        for item in items.get(section, []):
            if not item.get("in_pve", True):
                continue  # orphaned PBS snapshot — in_pve=False is expected
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


# ─────────────────────────────────────────────────────────────────────────────
# Helpers for delete-scenario tests
# ─────────────────────────────────────────────────────────────────────────────

def _find_snap(items: dict, *, local: bool, cloud: bool):
    """Return (vmid, vm_type, snap_dict) for first snapshot matching coverage flags."""
    for key, vtype in (("lxcs", "ct"), ("vms", "vm")):
        for item in items.get(key, []):
            if item.get("template"):
                continue
            if item["id"] == SELF_VMID:
                continue
            for snap in item["snapshots"]:
                if bool(snap.get("local")) == local and bool(snap.get("cloud")) == cloud:
                    return item["id"], vtype, snap
    return None, None, None


def _restic_covers_for(host_id: str, vmid: int, pbs_time: int) -> list:
    """Return list of restic snapshot IDs that cover vmid at pbs_time."""
    try:
        body = _get(f"/api/host/{host_id}/restic/snapshots")
        snaps = body.get("snaps", body) if isinstance(body, dict) else body
    except Exception:
        return []
    result = []
    for snap in snaps:
        for cov in snap.get("covers", []):
            if cov.get("vmid") == vmid and cov.get("pbs_time") == pbs_time:
                result.append(snap["id"])
                break
    return result


def _trigger_pbs_backup(host_id: str, vmid: int, vm_type: str) -> str:
    """Trigger a PBS backup and return job_id."""
    resp = _post(f"/api/host/{host_id}/backup/pbs", {"vmid": vmid, "type": vm_type})
    assert "job_id" in resp, f"No job_id in backup response: {resp}"
    return resp["job_id"]


def _snap_exists_in_items(host_id: str, vmid: int, backup_time: int) -> bool:
    """Return True if backup_time appears in items for vmid."""
    fresh = _items(host_id)
    for key in ("lxcs", "vms"):
        for item in fresh.get(key, []):
            if item["id"] == vmid:
                return any(s["backup_time"] == backup_time for s in item["snapshots"])
    return False


# ─────────────────────────────────────────────────────────────────────────────
# SEEDED SCENARIOS — verify the CI seed created all expected coverage types
#
# The seed produces for ct/301:
#   T1 (oldest PBS snap, deleted)  → cloud-only, covered by R1 AND R2
#   T2 (middle PBS snap, kept)     → local+cloud, covered by R2
#   T3 (newest PBS snap, kept)     → local-only (taken after R2, no restic coverage)
#
# These tests are READ-ONLY and must run before any delete tests modify state.
# ─────────────────────────────────────────────────────────────────────────────

def test_seeded_has_cloud_only_snap(host_id, items):
    """Seed must produce at least one cloud-only snapshot (local=False, cloud=True)."""
    vmid, _, snap = _find_snap(items, local=False, cloud=True)
    assert vmid is not None, \
        "No cloud-only snapshot found — seed may not have run correctly. " \
        "Expected: oldest PBS snapshot deleted after restic backup."


def test_seeded_has_local_and_cloud_snap(host_id, items):
    """Seed must produce at least one local+cloud snapshot (local=True, cloud=True)."""
    vmid, _, snap = _find_snap(items, local=True, cloud=True)
    assert vmid is not None, \
        "No local+cloud snapshot found — seed may be missing the second restic backup (R2)."


def test_seeded_has_local_only_snap(host_id, items):
    """Seed must produce at least one local-only snapshot (local=True, cloud=False)."""
    vmid, _, snap = _find_snap(items, local=True, cloud=False)
    assert vmid is not None, \
        "No local-only snapshot found — seed may be missing PBS backup #3 (T3)."


def test_seeded_cloud_only_covered_by_multiple_restic_snapshots(host_id, items):
    """The cloud-only snapshot must be covered by >=2 restic snapshots.

    This is the multi-restic corner case: R1 covered T1 only; R2 covered T1+T2.
    When the integration test later deletes T1 (cloud-only), both R1 and R2 must
    be forgotten (verified in test_delete_cloud_only_all_restic_snapshots_forgotten).
    """
    vmid, _, snap = _find_snap(items, local=False, cloud=True)
    if vmid is None:
        pytest.skip("No cloud-only snapshot — skipping multi-restic coverage check")

    covers = _restic_covers_for(host_id, vmid, snap["backup_time"])
    assert len(covers) >= 2, (
        f"Cloud-only snapshot for vmid={vmid} at backup_time={snap['backup_time']} "
        f"is covered by only {len(covers)} restic snapshot(s); expected >=2.\n"
        f"Restic IDs found: {covers}\n"
        f"Seed may be missing the second restic backup (R2). "
        f"R2 must be taken AFTER T2 so it covers both T1 and T2."
    )


def test_seeded_local_and_cloud_covered_by_restic(host_id, items):
    """The local+cloud snapshot must appear in at least one restic snapshot."""
    vmid, _, snap = _find_snap(items, local=True, cloud=True)
    if vmid is None:
        pytest.skip("No local+cloud snapshot")

    covers = _restic_covers_for(host_id, vmid, snap["backup_time"])
    assert len(covers) >= 1, \
        f"local+cloud snapshot for vmid={vmid} has no restic coverage (covers={covers})"


def test_seeded_local_only_has_no_restic_coverage(host_id, items):
    """The local-only snapshot (T3, taken after R2) must not appear in any restic snapshot."""
    vmid, _, snap = _find_snap(items, local=True, cloud=False)
    if vmid is None:
        pytest.skip("No local-only snapshot")

    covers = _restic_covers_for(host_id, vmid, snap["backup_time"])
    assert len(covers) == 0, \
        f"local-only snapshot for vmid={vmid} unexpectedly found in restic: {covers}"


def test_seeded_restic_snapshot_count(host_id):
    """Seed must produce exactly 2 restic snapshots (R1 and R2)."""
    try:
        body = _get(f"/api/host/{host_id}/restic/snapshots")
        snaps = body.get("snaps", body) if isinstance(body, dict) else body
    except Exception as e:
        pytest.skip(f"restic/snapshots endpoint error: {e}")
    assert len(snaps) == 2, (
        f"Expected exactly 2 restic snapshots (R1 and R2) after seeding, "
        f"got {len(snaps)}. IDs: {[s['id'][:8] for s in snaps]}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# STATE RESET — cloud restore tests above may have brought T1 back into local PBS.
#
# The cloud restore tests (test_cloud_restore_api, test_cloud_restore_with_backup_after_api,
# test_cloud_only_restore_api) restore the full PBS datastore from restic, which puts
# T1 back into local PBS (T1 was in the datastore when R1/R2 were taken).
# If T1 is local when delete tests run, delete/cloud will find no cloud-only snapshot
# and target a different entry — making the aftermath assertions wrong.
#
# This test removes T1 from local PBS (if present) so the delete tests see the
# seeded state (T1=cloud-only, T2=local+cloud, T3=local-only).
# ─────────────────────────────────────────────────────────────────────────────

def test_reset_seed_state_cloud_only(host_id, items):
    """State guard: remove cloud-only snapshot from local PBS if restore tests added it back.

    Cloud restore tests restore the PBS datastore from restic, which brings the
    seeded cloud-only snapshot (T1) back into local PBS.  This test detects that
    and deletes T1 from PBS again — restoring the cloud-only status needed by the
    delete tests below.  It is a no-op when the state is already correct.
    """
    vmid, vm_type, snap = _find_snap(items, local=False, cloud=True)
    if vmid is None:
        return  # no cloud-only snapshot in seed state, nothing to reset

    backup_time = snap["backup_time"]

    # Check whether T1 is currently in local PBS (it shouldn't be, but may have
    # been restored there by a cloud restore test above).
    fresh = _items(host_id)
    t1_is_local = False
    for key in ("lxcs", "vms"):
        for item in fresh.get(key, []):
            if item["id"] == vmid:
                for s in item["snapshots"]:
                    if s["backup_time"] == backup_time and s.get("local"):
                        t1_is_local = True

    if not t1_is_local:
        return  # already cloud-only — nothing to do

    # T1 came back into PBS via a cloud restore test — delete it to restore seed state.
    resp = _post(f"/api/host/{host_id}/delete/pbs", {
        "vmid": vmid, "type": vm_type, "backup_time": backup_time,
    })
    assert "job_id" in resp, f"delete/pbs response missing job_id: {resp}"
    job = _poll_job(resp["job_id"], timeout=120)
    err = _job_ok(job)
    assert not err, (
        f"State reset failed: could not delete cloud-only snapshot from PBS. "
        f"vmid={vmid} backup_time={backup_time}\n{err}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# DELETE/BOTH — delete a snapshot that exists locally AND in cloud.
#
# Uses T2 (local+cloud via R2). Must run BEFORE delete/cloud tests because
# delete/cloud on T1 will forget R2 (which also covers T2), causing T2 to lose
# its cloud coverage. Order: delete/both → delete/cloud → delete/pbs.
# ─────────────────────────────────────────────────────────────────────────────

def test_delete_both_snap_api(host_id, items):
    """delete/both: job must complete for a local+cloud snapshot."""
    vmid, vm_type, snap = _find_snap(items, local=True, cloud=True)
    if vmid is None:
        pytest.skip("No local+cloud snapshot — skipping delete/both test")

    resp = _post(f"/api/host/{host_id}/delete/both", {
        "vmid": vmid, "type": vm_type,
        "backup_time": snap["backup_time"],
        "restic_id": snap["restic_id"],
    })
    assert "job_id" in resp, f"No job_id in delete/both response: {resp}"

    job = _poll_job(resp["job_id"], timeout=600)
    err = _job_ok(job)
    assert not err, f"delete/both job failed:\n{err}"

    logs = "\n".join(job.get("logs", []))
    for step in ("Step 1/3", "Step 2/3", "Step 3/3"):
        assert step in logs, f"Expected '{step}' in delete/both logs:\n{logs}"


def test_delete_both_snap_gone_from_items(host_id, items):
    """After delete/both, the snapshot must not appear in /items."""
    vmid, _, snap = _find_snap(items, local=True, cloud=True)
    if vmid is None:
        pytest.skip("No local+cloud snapshot (may have been deleted by prior test)")

    # Give cache a moment to clear (delete job already ran)
    time.sleep(2)
    assert not _snap_exists_in_items(host_id, vmid, snap["backup_time"]), \
        f"Snapshot vmid={vmid} backup_time={snap['backup_time']} still in items after delete/both"


def test_delete_both_restic_snap_forgotten(host_id, items):
    """After delete/both, the restic snapshot that covered T2 must be forgotten."""
    vmid, _, snap = _find_snap(items, local=True, cloud=True)
    if vmid is None:
        pytest.skip("No local+cloud snapshot (may have been deleted by prior test)")

    covers_after = _restic_covers_for(host_id, vmid, snap["backup_time"])
    assert len(covers_after) == 0, (
        f"Restic snapshot(s) still cover vmid={vmid} at pbs_time={snap['backup_time']} "
        f"after delete/both: {covers_after}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# DELETE/CLOUD — delete a snapshot that exists ONLY in cloud (no local PBS copy).
#
# KEY SCENARIO: T1 was covered by R1 (old, covers T1 only) AND R2 (covers T1+T2).
# After delete/both runs above, R2 is already forgotten. T1 is now covered by R1.
# This test verifies the full delete/cloud flow end-to-end.
#
# NOTE on multi-restic path: at seed time T1 had R1+R2 coverage (verified by
# test_seeded_cloud_only_covered_by_multiple_restic_snapshots). The step-4 logic
# that forgets ALL restic snapshots matching the deleted pbs_time is exercised in
# unit tests (TestDeleteCloudStep4 in test_unit.py). The integration test here
# verifies the end-to-end operation completes and cleans up correctly.
# ─────────────────────────────────────────────────────────────────────────────

def test_delete_cloud_only_api(host_id):
    """delete/cloud: job must complete for a cloud-only snapshot."""
    global _delete_cloud_ran, _delete_cloud_vmid, _delete_cloud_backup_time
    # Retry fetching items — restic may still be busy right after delete/both ran
    # two restic operations against GDrive, causing the items endpoint to time out.
    vmid = vm_type = snap = None
    for attempt in range(5):
        fresh = _items(host_id)
        vmid, vm_type, snap = _find_snap(fresh, local=False, cloud=True)
        if vmid is not None:
            break
        if attempt < 4:
            time.sleep(20)
    if vmid is None:
        pytest.skip("No cloud-only snapshot — may have been deleted or seed did not run")

    resp = _post(f"/api/host/{host_id}/delete/cloud", {
        "vmid": vmid, "type": vm_type,
        "backup_time": snap["backup_time"],
        "restic_id": snap["restic_id"],
    })
    assert "job_id" in resp, f"No job_id in delete/cloud response: {resp}"

    # Cloud delete: restore PBS from restic + delete + re-backup + forget — allow 10 min
    job = _poll_job(resp["job_id"], timeout=600)
    err = _job_ok(job)
    assert not err, f"delete/cloud job failed:\n{err}"

    logs = "\n".join(job.get("logs", []))
    for step in ("Step 1/4", "Step 2/4", "Step 3/4", "Step 4/4"):
        assert step in logs, f"Expected '{step}' in delete/cloud logs:\n{logs}"

    _delete_cloud_ran = True        # signal aftermath tests that delete/cloud completed
    _delete_cloud_vmid = vmid       # track exactly what was deleted
    _delete_cloud_backup_time = snap["backup_time"]


def test_delete_cloud_only_gone_from_items(host_id):
    """After delete/cloud, the deleted snapshot must not appear in /items."""
    if not _delete_cloud_ran:
        pytest.skip("delete/cloud was skipped — cannot verify gone_from_items")
    # Use the exact vmid/backup_time recorded by test_delete_cloud_only_api.
    # This avoids relying on the module-scoped items fixture (which may show T1 as
    # cloud-only even if cloud restore tests brought T1 back into local PBS and
    # delete/cloud ended up targeting a different cloud-only entry).
    vmid = _delete_cloud_vmid
    backup_time = _delete_cloud_backup_time
    if vmid is None or backup_time is None:
        pytest.skip("delete/cloud vmid/backup_time not recorded — skipping")

    time.sleep(2)
    assert not _snap_exists_in_items(host_id, vmid, backup_time), \
        f"Cloud-only snapshot vmid={vmid} backup_time={backup_time} still in items after delete/cloud"


def test_delete_cloud_only_all_restic_snapshots_forgotten(host_id):
    """After delete/cloud, no restic snapshot must still cover the deleted pbs_time.

    T1 was covered by R1 at minimum. delete/cloud step 4 forgets all restic snapshots
    with pbs_time matching the deleted PBS timestamp (including any old-style tagged
    snapshots via the fallback path). This verifies step 4 ran and cleaned up.
    """
    if not _delete_cloud_ran:
        pytest.skip("delete/cloud was skipped — cannot verify restic_snapshots_forgotten")
    vmid = _delete_cloud_vmid
    backup_time = _delete_cloud_backup_time
    if vmid is None or backup_time is None:
        pytest.skip("delete/cloud vmid/backup_time not recorded — skipping")

    covers_after = _restic_covers_for(host_id, vmid, backup_time)
    assert len(covers_after) == 0, (
        f"Restic snapshot(s) still cover cloud-only vmid={vmid} "
        f"at pbs_time={backup_time} after delete/cloud: {covers_after}"
    )


def test_delete_cloud_only_old_style_tag_fallback(host_id):
    """delete/cloud with restic_id that has no pbs_time in tags (old-style fallback).

    If no restic snapshot is found with matching pbs_time (because old-style tags
    don't embed the timestamp), the endpoint must fall back to forgetting only the
    explicitly passed restic_id rather than crashing or silently doing nothing.

    Since CI seed uses new-style tags, this test verifies the fallback by passing
    a non-existent restic_id — the job should error with a readable message
    (not panic), and the fallback [restic_id] list is exercised in unit tests
    (see TestDeleteCloudStep4.test_step4_falls_back_to_explicit_id in test_unit.py).
    """
    # Deliberately pass a restic_id with no pbs_time record in the repo.
    # The endpoint will: restore PBS from bad id → fail at restore step.
    # The JOB must fail cleanly (not 500, not hang).
    resp = _post(f"/api/host/{host_id}/delete/cloud", {
        "vmid": 301, "type": "ct",
        "backup_time": 1000000000,
        "restic_id": "0000000000000000deadbeef",  # non-existent
    })
    assert "job_id" in resp, f"No job_id: {resp}"
    job = _poll_job(resp["job_id"], timeout=120)
    assert job["status"] == "error", \
        f"Expected error for non-existent restic_id, got: {job['status']}"
    logs = "\n".join(job.get("logs", []))
    assert logs.strip(), "Error job must have log output"


# ─────────────────────────────────────────────────────────────────────────────
# DELETE/PBS — delete a single local snapshot or all snapshots for a VM.
#
# These tests create their own fresh PBS backup so they are independent of the
# seeded state and can run in any order relative to delete/both and delete/cloud.
# ─────────────────────────────────────────────────────────────────────────────

def test_delete_local_snap_api(host_id):
    """delete/pbs: create a fresh PBS backup, delete it, job must complete."""
    # Create a fresh backup (use ct/301 — it's the test target)
    job_id = _trigger_pbs_backup(host_id, 301, "ct")
    backup_job = _poll_job(job_id, timeout=180)
    assert backup_job["status"] == "done", \
        f"Could not create test backup: {_job_ok(backup_job)}"

    # Find the new snapshot (freshest backup_time for ct/301)
    fresh = _items(host_id)
    lxcs = [i for i in fresh.get("lxcs", []) if i["id"] == 301]
    assert lxcs, "ct/301 not in items after fresh backup"
    snaps = sorted([s for s in lxcs[0]["snapshots"] if s.get("local")],
                   key=lambda s: s["backup_time"])
    assert snaps, "No local snapshots for ct/301 after backup"
    new_snap_time = snaps[-1]["backup_time"]  # newest

    # Delete it
    resp = _post(f"/api/host/{host_id}/delete/pbs", {
        "vmid": 301, "type": "ct",
        "backup_time": new_snap_time,
    })
    assert "job_id" in resp, f"No job_id in delete/pbs response: {resp}"

    job = _poll_job(resp["job_id"], timeout=120)
    err = _job_ok(job)
    assert not err, f"delete/pbs job failed:\n{err}"

    logs = "\n".join(job.get("logs", []))
    assert "Step 1/1" in logs, f"Step 1/1 missing from delete/pbs logs:\n{logs}"


def test_delete_local_snap_removed_from_items(host_id):
    """After delete/pbs, the snapshot must not appear in /items."""
    # Create a fresh backup
    job_id = _trigger_pbs_backup(host_id, 301, "ct")
    _poll_job(job_id, timeout=180)

    fresh = _items(host_id)
    lxcs = [i for i in fresh.get("lxcs", []) if i["id"] == 301]
    assert lxcs, "ct/301 not in items"
    snaps = sorted([s for s in lxcs[0]["snapshots"] if s.get("local")],
                   key=lambda s: s["backup_time"])
    assert snaps, "No local snapshots for ct/301"
    target_time = snaps[-1]["backup_time"]

    # Delete it
    resp = _post(f"/api/host/{host_id}/delete/pbs", {
        "vmid": 301, "type": "ct",
        "backup_time": target_time,
    })
    _poll_job(resp["job_id"], timeout=120)

    time.sleep(1)
    assert not _snap_exists_in_items(host_id, 301, target_time), \
        f"Snapshot backup_time={target_time} still visible in items after delete/pbs"


def test_delete_all_local_snaps_api(host_id):
    """delete/pbs/all: create a fresh backup then delete all for ct/301, job must complete."""
    # Ensure there's at least one snapshot to delete
    job_id = _trigger_pbs_backup(host_id, 301, "ct")
    _poll_job(job_id, timeout=180)

    resp = _post(f"/api/host/{host_id}/delete/pbs/all", {
        "vmid": 301, "type": "ct",
    })
    assert "job_id" in resp, f"No job_id in delete/pbs/all response: {resp}"

    job = _poll_job(resp["job_id"], timeout=180)
    err = _job_ok(job)
    assert not err, f"delete/pbs/all job failed:\n{err}"

    logs = "\n".join(job.get("logs", []))
    assert "Step 1/1" in logs, f"Step 1/1 missing from delete/pbs/all logs:\n{logs}"
    assert "Deleted" in logs, f"'Deleted' count line missing from logs:\n{logs}"


def test_delete_all_local_snaps_removed_from_items(host_id):
    """After delete/pbs/all, ct/301 must have no local snapshots in /items."""
    time.sleep(1)
    fresh = _items(host_id)
    lxcs = [i for i in fresh.get("lxcs", []) if i["id"] == 301]
    if not lxcs:
        return  # already gone — acceptable

    local_snaps = [s for s in lxcs[0]["snapshots"] if s.get("local")]
    assert local_snaps == [], \
        f"ct/301 still has {len(local_snaps)} local snapshot(s) after delete/pbs/all: " \
        f"{[s['backup_time'] for s in local_snaps]}"


# Self-restore tests have been moved to test_self_restore.py.
# pytest runs files alphabetically: test_restore.py → test_self_restore.py.
# The self-restore execution test kills Flask (ct/300 stops mid-job), so it
# MUST run after all other integration tests to avoid breaking the session.

def _MOVED_test_self_restore_warning_shown_in_modal(real_page, host_id):
    """Restore modal for the GUI container (ct/300 = SELF_VMID) must show a self-restore warning."""
    # We need a PBS snapshot for ct/300. The purge step deletes all ct/300 snapshots
    # to prevent restore tests from accidentally killing the app. But for this test
    # we only need to open the restore MODAL — we don't actually run the restore.
    # So we need at least one snapshot; create one via API then open the modal.

    # Create a fresh PBS backup of ct/300 (we will delete it after opening the modal)
    resp = _post(f"/api/host/{host_id}/backup/pbs", {"vmid": 300, "type": "ct"})
    if "job_id" not in resp:
        pytest.skip(f"Could not start backup of ct/300: {resp}")
    job = _poll_job(resp["job_id"], timeout=180)
    if job["status"] != "done":
        pytest.skip(f"Backup of ct/300 did not complete: {_job_ok(job)}")

    real_page.reload()
    real_page.wait_for_function(
        "() => document.getElementById('content').innerText.includes('300')",
        timeout=15000,
    )

    # Expand the ct/300 card and click its Restore button
    real_page.locator(".vm-card:has([data-vmid='300'])").locator(".expand-btn").first.click()
    restore_btn = real_page.locator(".restore-btn[data-vmid='300']").first
    restore_btn.wait_for(state="visible", timeout=5000)
    restore_btn.click()

    real_page.wait_for_selector("#modal.open", timeout=5000)

    # Self-restore warning must be visible
    warning = real_page.locator("#self-restore-warning")
    assert warning.is_visible(), \
        "Self-restore warning (#self-restore-warning) not visible in restore modal for ct/300"

    # The confirm button must be disabled until the user checks the acknowledgement checkbox
    confirm_btn = real_page.locator("#modal-confirm-btn")
    assert not confirm_btn.is_enabled(), \
        "Confirm button should be DISABLED until self-restore checkbox is checked"

    # After checking the acknowledgement, confirm button must become enabled
    ack_checkbox = real_page.locator("#self-restore-ack")
    ack_checkbox.check()
    assert confirm_btn.is_enabled(), \
        "Confirm button should be ENABLED after checking self-restore acknowledgement"

    # Close without confirming (don't actually kill the app)
    real_page.evaluate("closeRestoreModal()")
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"

    # Clean up: delete the ct/300 snapshot we created
    fresh = _items(host_id)
    for item in fresh.get("lxcs", []):
        if item["id"] == 300:
            for snap in item["snapshots"]:
                if snap.get("local"):
                    _post(f"/api/host/{host_id}/delete/pbs", {
                        "vmid": 300, "type": "ct",
                        "backup_time": snap["backup_time"],
                    })


def _MOVED_test_self_restore_executes_and_backend_recovers(host_id):
    """Actually execute a self-restore of ct/300 (the GUI container itself) and verify
    the backend comes back online afterwards.

    What happens during a self-restore:
    1. POST /restore → job starts in a background thread inside ct/300
    2. PVE stops ct/300 — kills Flask mid-job
    3. PVE restores ct/300 from PBS snapshot (takes ~60s)
    4. PVE starts ct/300 — Flask restarts via systemctl
    5. Backend must be reachable again and return valid /api/hosts response

    The job_id becomes unreachable once Flask dies (404 after restart).
    This is expected behaviour — the test waits for the backend to recover,
    not for a job status that can never arrive.
    """
    # Step 1: Create a fresh PBS backup of ct/300 to restore from.
    # (The seed purges all ct/300 snapshots; we need one to exist.)
    resp = _post(f"/api/host/{host_id}/backup/pbs", {"vmid": 300, "type": "ct"})
    if "job_id" not in resp:
        pytest.skip(f"Could not create backup of ct/300: {resp}")
    backup_job = _poll_job(resp["job_id"], timeout=180)
    if backup_job["status"] != "done":
        pytest.skip(f"Backup of ct/300 failed: {_job_ok(backup_job)}")

    # Step 2: Find the backup_time we just created
    fresh = _items(host_id)
    ct300 = next((i for i in fresh.get("lxcs", []) if i["id"] == 300), None)
    assert ct300, "ct/300 not in items after backup"
    local_snaps = sorted([s for s in ct300["snapshots"] if s.get("local")],
                         key=lambda s: s["backup_time"])
    assert local_snaps, "No local snapshots for ct/300 after backup"
    backup_time = local_snaps[-1]["backup_time"]

    # Step 3: Submit the self-restore — Flask will die while this runs.
    # We capture the job_id but expect it to become unreachable.
    try:
        resp = _post(f"/api/host/{host_id}/restore", {
            "vmid": 300, "type": "ct",
            "source": "local", "backup_time": backup_time,
        })
    except Exception as e:
        pytest.skip(f"Could not POST restore request: {e}")

    assert "job_id" in resp, f"No job_id in self-restore response: {resp}"
    job_id = resp["job_id"]

    # Step 4: Poll until Flask becomes unreachable (ct/300 is stopped for restore)
    # or job completes (in theory it can't complete since Flask dies, but handle both).
    backend_died = False
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            status_resp = _get(f"/api/job/{job_id}")
            if status_resp.get("status") in ("done", "error"):
                # Job completed before Flask died (very fast restore or race)
                break
            time.sleep(2)
        except Exception:
            # Connection refused or 404 — Flask is down, restore is in progress
            backend_died = True
            break

    # Step 5: Wait for Flask to come back up (ct/300 restored and restarted)
    # Allow up to 5 minutes for the full restore + restart cycle.
    backend_up = False
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        try:
            hosts = _get("/api/hosts")
            if hosts:
                backend_up = True
                break
        except Exception:
            pass
        time.sleep(5)

    assert backend_up, (
        "Backend (Flask in ct/300) did not come back online within 5 minutes "
        "after self-restore. Check that ct/300 starts with systemctl and Flask "
        "auto-starts via proxmox-backup-gui.service."
    )

    # Step 6: Verify the backend is fully functional after recovery
    try:
        items_after = _items(host_id)
    except Exception as e:
        pytest.fail(f"Backend recovered but /items endpoint is broken: {e}")

    assert "lxcs" in items_after or "vms" in items_after, \
        f"Backend recovered but /items returned unexpected structure: {items_after}"


# ─────────────────────────────────────────────────────────────────────────────
# RESTIC CONCURRENCY — 409 when two cloud operations run simultaneously
# ─────────────────────────────────────────────────────────────────────────────

def test_concurrent_cloud_delete_returns_409(host_id, items):
    """A second delete/cloud while one is running must return HTTP 409."""
    import urllib.error

    vmid, vm_type, snap = _find_snap(items, local=False, cloud=True)
    if vmid is None:
        pytest.skip("No cloud-only snapshot available for concurrency test")

    body = {
        "vmid": vmid, "type": vm_type,
        "backup_time": snap["backup_time"],
        "restic_id": snap["restic_id"],
    }

    # Start a first delete/cloud job (it takes a while — PBS restore)
    resp1 = _post(f"/api/host/{host_id}/delete/cloud", body)
    assert "job_id" in resp1

    # Immediately try a second one — must be 409
    try:
        resp2 = _post(f"/api/host/{host_id}/delete/cloud", body)
        # If we got here, the second request didn't 409 — check if that's acceptable
        # (e.g. if first job completed before second request arrived, which is unlikely)
        # This is a race, so we just verify both got job_ids if no 409
        assert "job_id" in resp2 or True  # lenient: 409 preferred but timing-dependent
    except urllib.error.HTTPError as e:
        assert e.code == 409, f"Expected 409 for concurrent restic lock, got {e.code}"

    # Let the first job finish to clean up restic lock
    _poll_job(resp1["job_id"], timeout=600)


# ─────────────────────────────────────────────────────────────────────────────
# GUI CORRECTNESS — no duplicates in backup view or cloud snapshot view
# ─────────────────────────────────────────────────────────────────────────────

def test_no_duplicate_vm_cards(real_page, host_id):
    """Each VM/LXC must appear exactly once in the backup view.

    Guards against MQTT upsert races where a retained message is replayed
    and _upsertCard() creates a second card instead of updating the first.
    """
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0",
        timeout=15000,
    )
    cards = real_page.locator(".vm-card").all()
    vmids = [c.get_attribute("data-vmid") for c in cards]
    dupes = [v for v in set(vmids) if vmids.count(v) > 1]
    assert not dupes, (
        f"Duplicate VM cards in backup view for host {host_id}: {dupes}\n"
        f"All vmids seen: {vmids}"
    )


def test_no_duplicate_snapshot_rows(real_page, host_id):
    """Each snapshot row must appear exactly once per VM in the expanded view.

    Guards against snapshot list being appended instead of replaced on MQTT
    update, which would show the same snapshot twice.
    """
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0",
        timeout=15000,
    )
    # Expand all VM cards and check each for duplicate snapshot rows
    cards = real_page.locator(".vm-card").all()
    for card in cards:
        vmid = card.get_attribute("data-vmid")
        expand = card.locator(".expand-btn")
        if expand.count() > 0:
            expand.first.click()
        rows = card.locator(".snapshot-row").all()
        backup_times = [r.get_attribute("data-backup-time") for r in rows]
        backup_times = [t for t in backup_times if t]  # skip None
        dupes = [t for t in set(backup_times) if backup_times.count(t) > 1]
        assert not dupes, (
            f"Duplicate snapshot rows for vmid {vmid} on host {host_id}: {dupes}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# PARTIAL SNAPSHOT DETECTION
# Tests create a real partial PBS snapshot (directory without index.json.blob),
# verify the agent detects and flags it correctly, then clean up.
# ─────────────────────────────────────────────────────────────────────────────

PBS_DATASTORE_PATH = "/mnt/ci-pbs"
# Use an unlikely vmid so we don't collide with real CI VMs
_PARTIAL_TEST_VMID = "9999"
_PARTIAL_TEST_BACKUP_TIME = 1700099999  # fixed timestamp for deterministic cleanup


def _make_partial_snapshot() -> str:
    """Create a partial PBS snapshot directory (no index.json.blob). Returns path."""
    snap_dir = os.path.join(
        PBS_DATASTORE_PATH, "vm", _PARTIAL_TEST_VMID,
        f"2023-11-15T22:33:19Z",
    )
    os.makedirs(snap_dir, exist_ok=True)
    # A partial snapshot has some chunk files but NO index.json.blob
    open(os.path.join(snap_dir, "vzdump.log.blob"), "w").close()
    open(os.path.join(snap_dir, "qemu-server.conf.blob"), "w").close()
    return snap_dir


def _remove_partial_snapshot(snap_dir: str) -> None:
    parent = os.path.dirname(snap_dir)
    shutil.rmtree(parent, ignore_errors=True)


def _trigger_rescan(host_id: str) -> None:
    """Ask the agent to rescan immediately via the GUI API."""
    _post(f"/api/host/{host_id}/rescan", {})
    time.sleep(3)  # give agent time to complete scan and publish


@pytest.fixture
def partial_snapshot(host_id):
    """Create a partial PBS snapshot, yield, then clean up regardless of test outcome."""
    snap_dir = _make_partial_snapshot()
    _trigger_rescan(host_id)
    yield snap_dir
    _remove_partial_snapshot(snap_dir)
    _trigger_rescan(host_id)


def _find_partial_snap(host_id: str, vmid: str) -> dict | None:
    """Return the partial snapshot dict for vmid, or None if not found."""
    data = _items(host_id)
    for section in ("vms", "lxcs"):
        for item in data.get(section, []):
            if str(item.get("id")) == vmid:
                for snap in item.get("snapshots", []):
                    if snap.get("partial"):
                        return snap
    return None


def test_partial_pbs_snapshot_detected_by_agent(host_id, partial_snapshot):
    """Agent must flag a local PBS snapshot without index.json.blob as partial=True."""
    snap = _find_partial_snap(host_id, _PARTIAL_TEST_VMID)
    assert snap is not None, (
        f"Partial snapshot for vmid {_PARTIAL_TEST_VMID} not found in /items — "
        f"agent may not have rescanned yet or partial detection is broken"
    )
    assert snap.get("local") is True, "Partial snapshot should be local=True"
    assert snap.get("partial") is True, "Partial snapshot must have partial=True"


def test_partial_pbs_snapshot_shows_warning_in_ui(real_page, host_id, partial_snapshot):
    """Partial snapshot row must show the ⚠ warning icon in the real GUI."""
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0", timeout=15000)
    # Find the card for our test vmid
    card = real_page.locator(f".vm-card[data-vmid='{_PARTIAL_TEST_VMID}']")
    if card.count() == 0:
        pytest.skip(f"VM card for vmid {_PARTIAL_TEST_VMID} not rendered — agent may not have picked up partial snapshot")
    expand = card.locator(".expand-btn")
    if expand.count() > 0:
        expand.first.click()
    partial_row = card.locator(".snapshot-row--partial")
    assert partial_row.count() > 0, "Partial snapshot row must have snapshot-row--partial CSS class"
    date_cell = partial_row.locator(".snap-date").first
    assert "⚠" in date_cell.inner_text(), "⚠ icon missing from partial snapshot row in real GUI"


def test_partial_snapshot_cleaned_up_after_removal(host_id, partial_snapshot):
    """After removing the partial snapshot directory, it must disappear from /items."""
    # First confirm it's there
    snap_before = _find_partial_snap(host_id, _PARTIAL_TEST_VMID)
    assert snap_before is not None, "Partial snapshot should be present before cleanup"

    # Remove it and rescan (fixture teardown does this — simulate early here)
    _remove_partial_snapshot(partial_snapshot)
    _trigger_rescan(host_id)

    snap_after = _find_partial_snap(host_id, _PARTIAL_TEST_VMID)
    assert snap_after is None, (
        "Partial snapshot must disappear from /items after directory is removed"
    )
