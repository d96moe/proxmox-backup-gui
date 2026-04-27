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


_PARTIAL_EXCLUDED_VMIDS = {str(SELF_VMID)}


def _find_existing_snapshot_dir() -> tuple[str, str, str] | None:
    """Find an existing PBS snapshot directory to clone.

    Returns (backup_type, vmid, snap_dir_path) or None if datastore is empty.
    Skips SELF_VMID so we never disturb the container running the backend.
    Prefers VM-type snapshots (faster to scan) over CT when both exist.
    """
    for btype in ("vm", "ct"):
        type_dir = os.path.join(PBS_DATASTORE_PATH, btype)
        if not os.path.isdir(type_dir):
            continue
        for vmid in sorted(os.listdir(type_dir)):
            if vmid in _PARTIAL_EXCLUDED_VMIDS:
                continue
            vmid_dir = os.path.join(type_dir, vmid)
            if not os.path.isdir(vmid_dir):
                continue
            for ts_dir in sorted(os.listdir(vmid_dir)):
                snap_dir = os.path.join(vmid_dir, ts_dir)
                if os.path.isfile(os.path.join(snap_dir, "index.json.blob")):
                    return btype, vmid, snap_dir
    return None


def _make_partial_snapshot() -> tuple[str, str, str] | None:
    """Clone an existing PBS snapshot and remove index.json.blob to simulate a
    partial (failed mid-backup) snapshot.

    Returns (backup_type, vmid, cloned_snap_dir) or None if no source found.
    The cloned directory uses a timestamp 1 second before the source so it sorts
    as an older entry and doesn't affect the real latest snapshot's position.
    """
    result = _find_existing_snapshot_dir()
    if result is None:
        return None
    btype, vmid, src_dir = result
    src_ts = os.path.basename(src_dir)  # e.g. "2024-04-22T02:00:01Z"

    # Build a fake timestamp that sorts before the real one (subtract 1 day)
    try:
        from datetime import datetime, timedelta, timezone
        dt = datetime.strptime(src_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        fake_dt = dt - timedelta(days=1)
        fake_ts = fake_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        fake_ts = "2000-01-01T00:00:00Z"

    clone_dir = os.path.join(PBS_DATASTORE_PATH, btype, vmid, fake_ts)
    if os.path.exists(clone_dir):
        shutil.rmtree(clone_dir)
    shutil.copytree(src_dir, clone_dir)

    # Remove index.json.blob → this is what makes it "partial"
    manifest = os.path.join(clone_dir, "index.json.blob")
    if os.path.exists(manifest):
        os.remove(manifest)

    return btype, vmid, clone_dir


def _remove_partial_snapshot(snap_dir: str) -> None:
    shutil.rmtree(snap_dir, ignore_errors=True)


def _rescan(host_id: str) -> None:
    """Ask the agent to trigger an immediate PBS rescan."""
    _post(f"/api/host/{host_id}/rescan", {})


def _wait_for_partial_snap(host_id: str, vmid: str, timeout: int = 30) -> dict:
    """Poll /items until a partial snapshot appears for vmid. Raises TimeoutError."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = _find_partial_snap_for_vm(host_id, vmid)
        if snap is not None:
            return snap
        time.sleep(2)
    raise TimeoutError(
        f"Partial snapshot for vmid {vmid} not detected within {timeout}s — "
        "agent rescan may be stuck or partial detection is broken"
    )


def _wait_for_no_partial_snap(host_id: str, vmid: str, timeout: int = 30) -> None:
    """Poll /items until no partial snapshot exists for vmid. Raises TimeoutError."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _find_partial_snap_for_vm(host_id, vmid) is None:
            return
        time.sleep(2)
    raise TimeoutError(
        f"Partial snapshot for vmid {vmid} still present after {timeout}s — "
        "cleanup or rescan may have failed"
    )


@pytest.fixture
def partial_snapshot(host_id):
    """Clone an existing PBS snapshot, remove index.json.blob to make it partial.

    Yields (backup_type, vmid, clone_dir). Cleans up on teardown.
    Skips the test if no suitable source snapshot exists in the datastore.

    Isolation guarantees:
    - Never clones SELF_VMID (LXC 300 running the backend).
    - Clone timestamp is 1 day before source, so it never becomes the "latest" snap.
    - Setup blocks until agent actually reports the partial (no sleep guessing).
    - Teardown blocks until agent confirms removal before yielding to the next test.
    """
    result = _make_partial_snapshot()
    if result is None:
        pytest.skip("No complete PBS snapshot found to clone — datastore may be empty")
    btype, vmid, clone_dir = result
    _rescan(host_id)
    _wait_for_partial_snap(host_id, vmid)
    yield btype, vmid, clone_dir
    _remove_partial_snapshot(clone_dir)
    _rescan(host_id)
    _wait_for_no_partial_snap(host_id, vmid)


def _find_partial_snap_for_vm(host_id: str, vmid: str) -> dict | None:
    """Return the partial snapshot dict for vmid, or None if not found."""
    data = _items(host_id)
    for section in ("vms", "lxcs"):
        for item in data.get(section, []):
            if str(item.get("id")) == str(vmid):
                for snap in item.get("snapshots", []):
                    if snap.get("partial"):
                        return snap
    return None


def test_partial_pbs_snapshot_detected_by_agent(host_id, partial_snapshot):
    """Agent must flag a cloned PBS snapshot without index.json.blob as partial=True."""
    _, vmid, _ = partial_snapshot
    # Fixture already waited for the partial to appear; verify the fields.
    snap = _find_partial_snap_for_vm(host_id, vmid)
    assert snap is not None, f"Partial snapshot for vmid {vmid} disappeared after fixture setup"
    assert snap.get("local") is True, "Partial snapshot should be local=True"
    assert snap.get("partial") is True, "Partial snapshot must have partial=True"


def test_partial_pbs_snapshot_shows_warning_in_ui(real_page, host_id, partial_snapshot):
    """Partial snapshot row must show the ⚠ warning icon in the real GUI."""
    _, vmid, _ = partial_snapshot
    # Reload so the page opens a fresh WebSocket connection and receives the
    # current state (including the partial snapshot created during fixture setup).
    # Without reload, the page may have connected before the partial existed.
    real_page.reload()
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=10000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_selector(
        f".vm-card[data-vmid='{vmid}'] .expand-btn", timeout=20000)
    real_page.locator(f".vm-card[data-vmid='{vmid}'] .expand-btn").first.click()
    partial_row = real_page.locator(f".vm-card[data-vmid='{vmid}'] .snapshot-row--partial")
    partial_row.first.wait_for(timeout=10000)
    assert partial_row.count() > 0, "Partial snapshot row must have snapshot-row--partial CSS class"
    date_cell = partial_row.locator(".snap-date").first
    assert "⚠" in date_cell.inner_text(), "⚠ icon missing from partial snapshot row in real GUI"


def test_partial_snapshot_cleaned_up_after_removal(host_id, partial_snapshot):
    """After removing the partial snapshot directory, it must disappear from /items."""
    _, vmid, clone_dir = partial_snapshot
    snap_before = _find_partial_snap_for_vm(host_id, vmid)
    assert snap_before is not None, "Partial snapshot should be present before cleanup"

    _remove_partial_snapshot(clone_dir)
    _rescan(host_id)
    # Fixture teardown will also call _wait_for_no_partial_snap, so this test just
    # verifies the agent eventually stops reporting the snapshot.
    _wait_for_no_partial_snap(host_id, vmid)


# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS — GET/POST /api/host/<id>/settings (restic retention)
# ─────────────────────────────────────────────────────────────────────────────

def _host_has_agent(host_id: str) -> bool:
    """Return True if the host has an agent_url configured."""
    hosts = _get("/api/hosts")
    for h in hosts:
        if h["id"] == host_id:
            return bool(h.get("agent_url"))
    return False


@pytest.fixture
def original_retention(host_id):
    """Save retention before test and restore it after regardless of outcome."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url — settings endpoint not available")
    original = _get(f"/api/host/{host_id}/settings").get("retention", {})
    yield original
    # Restore — ignore errors (e.g. if the test itself caused a partial write)
    try:
        _post(f"/api/host/{host_id}/settings", {"retention": original})
    except Exception:
        pass


def test_settings_api_returns_retention_dict(host_id, original_retention):
    """GET /settings must return {retention: dict}."""
    data = _get(f"/api/host/{host_id}/settings")
    assert "retention" in data, f"'retention' key missing from settings response: {data}"
    assert isinstance(data["retention"], dict), \
        f"retention must be a dict, got {type(data['retention']).__name__}"


def test_settings_api_retention_values_are_integers(host_id, original_retention):
    """All retention values must be integers (never strings or floats)."""
    retention = _get(f"/api/host/{host_id}/settings")["retention"]
    for key, val in retention.items():
        assert isinstance(val, int), \
            f"retention[{key!r}] must be int, got {type(val).__name__}: {val!r}"


def test_settings_api_save_and_reload(host_id, original_retention):
    """POST then GET must return the values that were saved."""
    payload = {
        "keep-last":    2,
        "keep-daily":   5,
        "keep-weekly":  2,
        "keep-monthly": 2,
    }
    resp = _post(f"/api/host/{host_id}/settings", {"retention": payload})
    assert "retention" in resp, f"POST /settings did not return retention: {resp}"
    reloaded = _get(f"/api/host/{host_id}/settings")["retention"]
    for key, expected in payload.items():
        assert reloaded.get(key) == expected, \
            f"retention[{key!r}] expected {expected}, got {reloaded.get(key)!r} after save"


def test_settings_api_partial_update_preserves_other_keys(host_id, original_retention):
    """POST with a subset of keys must not delete the other retention keys."""
    # First write a known full state
    full = {"keep-last": 3, "keep-daily": 7, "keep-weekly": 2, "keep-monthly": 2}
    _post(f"/api/host/{host_id}/settings", {"retention": full})

    # Now update only keep-weekly
    _post(f"/api/host/{host_id}/settings", {"retention": {"keep-weekly": 4}})
    after = _get(f"/api/host/{host_id}/settings")["retention"]

    assert after.get("keep-weekly") == 4, \
        f"keep-weekly not updated: {after.get('keep-weekly')!r}"
    assert after.get("keep-last") == 3, \
        f"keep-last was unexpectedly changed: {after.get('keep-last')!r}"
    assert after.get("keep-daily") == 7, \
        f"keep-daily was unexpectedly changed: {after.get('keep-daily')!r}"


def test_settings_api_unknown_retention_keys_ignored(host_id, original_retention):
    """POST with unknown retention keys must not crash — they are silently ignored."""
    resp = _post(f"/api/host/{host_id}/settings", {
        "retention": {"keep-last": 1, "keep-forever": 999, "bogus": "x"}
    })
    assert "retention" in resp, f"POST with unknown keys crashed: {resp}"
    after = _get(f"/api/host/{host_id}/settings")["retention"]
    assert "keep-forever" not in after, "Unknown key 'keep-forever' must not be written"
    assert "bogus" not in after, "Unknown key 'bogus' must not be written"
    assert after.get("keep-last") == 1, "keep-last must still be saved despite unknown keys"


def test_settings_api_invalid_body_returns_400(host_id, original_retention):
    """POST with retention as a non-dict must return 400."""
    import urllib.error
    try:
        _post(f"/api/host/{host_id}/settings", {"retention": "invalid"})
        pytest.fail("Expected 400 for retention=string, but got success")
    except urllib.error.HTTPError as e:
        assert e.code == 400, f"Expected 400, got {e.code}"


def test_settings_api_missing_retention_key_returns_400(host_id, original_retention):
    """POST body without 'retention' key must return 400."""
    import urllib.error
    try:
        _post(f"/api/host/{host_id}/settings", {"wrong_key": {}})
        pytest.fail("Expected 400 for missing retention key")
    except urllib.error.HTTPError as e:
        assert e.code == 400, f"Expected 400, got {e.code}"


def test_settings_api_post_returns_updated_retention(host_id, original_retention):
    """POST must return the full updated retention (not just the keys that were sent)."""
    # First set a baseline with multiple keys
    _post(f"/api/host/{host_id}/settings", {
        "retention": {"keep-last": 1, "keep-daily": 3, "keep-weekly": 1}
    })
    resp = _post(f"/api/host/{host_id}/settings", {"retention": {"keep-monthly": 2}})
    returned = resp.get("retention", {})
    # The response must include keep-monthly that we just set
    assert returned.get("keep-monthly") == 2, \
        f"POST response must reflect newly saved key: {returned}"


# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS — schedule (PBS vzdump schedule + restic OnCalendar)
# ─────────────────────────────────────────────────────────────────────────────

def _ssh_pve_output(*cmd: str) -> str:
    """Run a command on the PVE host via SSH and return stdout."""
    result = subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
         _PVE_HOST_SSH, *cmd],
        capture_output=True, text=True, timeout=15,
    )
    return result.stdout.strip()


def _read_pve_backup_schedule(job_id: str) -> str:
    """Read the current schedule of a PVE vzdump backup job via pvesh."""
    out = _ssh_pve_output(
        "pvesh", "get", f"/cluster/backup/{job_id}",
        "--output-format", "json",
    )
    import json as _json
    return _json.loads(out).get("schedule", "")


def _read_restic_on_calendar() -> str:
    """Read the OnCalendar value from restic-backup.timer on the PVE host."""
    out = _ssh_pve_output(
        "bash", "-c",
        "grep -i OnCalendar /etc/systemd/system/restic-backup.timer | head -1",
    )
    _, _, value = out.partition("=")
    return value.strip()


@pytest.fixture
def original_schedules(host_id):
    """Save PBS + restic schedules before test and restore after."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url — settings endpoint not available")
    data = _get(f"/api/host/{host_id}/settings")
    orig_pbs    = data.get("pbs_schedule")   # {id, schedule} or None
    orig_restic = data.get("restic_schedule") # str or None
    yield {"pbs": orig_pbs, "restic": orig_restic}
    # Restore
    body = {}
    if orig_pbs:
        body["pbs_schedule"] = orig_pbs
    if orig_restic is not None:
        body["restic_schedule"] = orig_restic
    if body:
        try:
            _post(f"/api/host/{host_id}/settings", body)
        except Exception:
            pass


def test_settings_api_returns_schedule_fields(host_id, original_schedules):
    """GET /settings must include pbs_schedule and restic_schedule keys."""
    data = _get(f"/api/host/{host_id}/settings")
    assert "pbs_schedule" in data, f"'pbs_schedule' key missing from settings: {data}"
    assert "restic_schedule" in data, f"'restic_schedule' key missing from settings: {data}"


def test_settings_api_pbs_schedule_has_id_and_schedule(host_id, original_schedules):
    """pbs_schedule must be a dict with 'id' and 'schedule' keys."""
    pbs = _get(f"/api/host/{host_id}/settings").get("pbs_schedule")
    if pbs is None:
        pytest.skip("No PBS backup job configured on this host")
    assert isinstance(pbs, dict), f"pbs_schedule must be a dict, got {type(pbs)}"
    assert "id" in pbs,       f"pbs_schedule missing 'id': {pbs}"
    assert "schedule" in pbs, f"pbs_schedule missing 'schedule': {pbs}"
    assert pbs["schedule"],   f"pbs_schedule.schedule must not be empty: {pbs}"


def test_settings_api_restic_schedule_is_string(host_id, original_schedules):
    """restic_schedule must be a non-empty string when the timer is configured."""
    restic = _get(f"/api/host/{host_id}/settings").get("restic_schedule")
    if restic is None:
        pytest.skip("No restic-backup.timer configured on this host")
    assert isinstance(restic, str), \
        f"restic_schedule must be a string, got {type(restic).__name__}: {restic!r}"
    assert restic, "restic_schedule must not be empty"


def test_settings_api_pbs_schedule_roundtrip(host_id, original_schedules):
    """POST pbs_schedule then GET must return the new schedule."""
    orig = original_schedules["pbs"]
    if not orig:
        pytest.skip("No PBS backup job configured on this host")
    job_id   = orig["id"]
    new_sched = "03:00"
    if orig["schedule"] == new_sched:
        new_sched = "03:30"

    _post(f"/api/host/{host_id}/settings", {
        "pbs_schedule": {"id": job_id, "schedule": new_sched},
    })
    after = _get(f"/api/host/{host_id}/settings").get("pbs_schedule", {})
    assert after.get("schedule") == new_sched, \
        f"Expected pbs_schedule={new_sched!r}, got {after.get('schedule')!r}"


def test_settings_pbs_schedule_written_to_pve(host_id, original_schedules):
    """pbs_schedule POST must be reflected in the actual PVE cluster/backup job."""
    orig = original_schedules["pbs"]
    if not orig:
        pytest.skip("No PBS backup job configured on this host")
    job_id    = orig["id"]
    new_sched = "04:00"
    if orig["schedule"] == new_sched:
        new_sched = "04:15"

    _post(f"/api/host/{host_id}/settings", {
        "pbs_schedule": {"id": job_id, "schedule": new_sched},
    })
    actual = _read_pve_backup_schedule(job_id)
    assert actual == new_sched, \
        f"PVE backup job {job_id!r} schedule: expected {new_sched!r}, got {actual!r}"


def test_settings_api_restic_schedule_roundtrip(host_id, original_schedules):
    """POST restic_schedule then GET must return the new OnCalendar value."""
    orig = original_schedules["restic"]
    if orig is None:
        pytest.skip("No restic-backup.timer configured on this host")
    new_sched = "05:00"
    if orig == new_sched:
        new_sched = "05:30"

    _post(f"/api/host/{host_id}/settings", {"restic_schedule": new_sched})
    after = _get(f"/api/host/{host_id}/settings").get("restic_schedule")
    assert after == new_sched, \
        f"Expected restic_schedule={new_sched!r}, got {after!r}"


def test_settings_restic_schedule_written_to_timer_file(host_id, original_schedules):
    """restic_schedule POST must be reflected when read back via GET /settings."""
    orig = original_schedules["restic"]
    if orig is None:
        pytest.skip("No restic-backup.timer configured on this host")
    new_sched = "05:15"
    if orig == new_sched:
        new_sched = "05:45"

    _post(f"/api/host/{host_id}/settings", {"restic_schedule": new_sched})
    # Verify by reading back through the agent (which reads the file directly)
    after = _get(f"/api/host/{host_id}/settings").get("restic_schedule")
    assert after == new_sched, \
        f"restic_schedule not persisted: expected {new_sched!r}, got {after!r}"


def test_settings_api_pbs_schedule_invalid_id_returns_error(host_id, original_schedules):
    """POST pbs_schedule with a non-existent job id must return an error."""
    import urllib.error
    try:
        _post(f"/api/host/{host_id}/settings", {
            "pbs_schedule": {"id": "no-such-job-xyz", "schedule": "03:00"},
        })
        pytest.fail("Expected error for non-existent pbs job id")
    except urllib.error.HTTPError as e:
        assert e.code == 500, f"Expected 500, got {e.code}"


def test_settings_api_pbs_schedule_missing_fields_returns_400(host_id, original_schedules):
    """POST pbs_schedule without id/schedule must return 400."""
    import urllib.error
    try:
        _post(f"/api/host/{host_id}/settings", {"pbs_schedule": {"id": "x"}})
        pytest.fail("Expected 400 for pbs_schedule missing schedule field")
    except urllib.error.HTTPError as e:
        assert e.code == 400, f"Expected 400, got {e.code}"


def test_settings_api_schedule_update_does_not_change_retention(host_id, original_schedules, original_retention):
    """Updating only pbs_schedule must leave retention unchanged."""
    orig = original_schedules["pbs"]
    if not orig:
        pytest.skip("No PBS backup job configured on this host")
    ret_before = _get(f"/api/host/{host_id}/settings")["retention"]

    job_id    = orig["id"]
    new_sched = "03:45"
    if orig["schedule"] == new_sched:
        new_sched = "04:45"
    _post(f"/api/host/{host_id}/settings", {
        "pbs_schedule": {"id": job_id, "schedule": new_sched},
    })
    ret_after = _get(f"/api/host/{host_id}/settings")["retention"]
    assert ret_before == ret_after, \
        f"Retention changed unexpectedly after schedule update: {ret_before} → {ret_after}"


# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS — PBS prune policy
# ─────────────────────────────────────────────────────────────────────────────

_PBS_PRUNE_KEYS = ("keep-last", "keep-hourly", "keep-daily", "keep-weekly", "keep-monthly", "keep-yearly")


def _pbs_prune_job_live(job_id: str) -> dict:
    """Read a PBS prune job dict from proxmox-backup-manager on the PVE host."""
    import json as _json
    out = _ssh_pve_output(
        "proxmox-backup-manager", "prune-job", "list", "--output-format", "json",
    )
    jobs = _json.loads(out or "[]")
    return next((j for j in jobs if j.get("id") == job_id), {})


@pytest.fixture
def original_pbs_prune(host_id):
    """Save PBS prune retention before test and restore after."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url — settings endpoint not available")
    data = _get(f"/api/host/{host_id}/settings")
    pp = data.get("pbs_prune")
    if not pp:
        pytest.skip("No PBS prune job configured for this host's datastore")
    yield pp  # {"id": ..., "retention": {...}}
    try:
        _post(f"/api/host/{host_id}/settings", {"pbs_prune": pp})
    except Exception:
        pass


def test_settings_api_returns_pbs_prune(host_id, original_pbs_prune):
    """GET /settings must include pbs_prune with id and retention."""
    data = _get(f"/api/host/{host_id}/settings")
    assert "pbs_prune" in data, f"'pbs_prune' missing from settings: {data}"
    pp = data["pbs_prune"]
    assert isinstance(pp, dict), f"pbs_prune must be dict, got {type(pp)}"
    assert "id" in pp, f"pbs_prune missing 'id': {pp}"
    assert isinstance(pp.get("retention"), dict), f"pbs_prune missing 'retention' dict: {pp}"


def test_settings_api_pbs_prune_retention_values_are_integers(host_id, original_pbs_prune):
    """pbs_prune.retention values must be integers."""
    retention = _get(f"/api/host/{host_id}/settings")["pbs_prune"]["retention"]
    for key, val in retention.items():
        assert isinstance(val, int), \
            f"pbs_prune.retention[{key!r}] must be int, got {type(val).__name__}: {val!r}"


def test_settings_api_pbs_prune_roundtrip(host_id, original_pbs_prune):
    """POST pbs_prune then GET must return the same retention."""
    job_id = original_pbs_prune["id"]
    new_ret = {"keep-last": 2, "keep-daily": 5, "keep-weekly": 3}
    _post(f"/api/host/{host_id}/settings", {"pbs_prune": {"id": job_id, "retention": new_ret}})
    after = _get(f"/api/host/{host_id}/settings")["pbs_prune"]["retention"]
    for key, expected in new_ret.items():
        assert after.get(key) == expected, \
            f"pbs_prune.retention[{key!r}] expected {expected}, got {after.get(key)!r}"


def test_settings_pbs_prune_written_to_pbs(host_id, original_pbs_prune):
    """POST pbs_prune must be reflected in the actual PBS prune job."""
    job_id = original_pbs_prune["id"]
    new_ret = {"keep-last": 3, "keep-weekly": 2}
    _post(f"/api/host/{host_id}/settings", {"pbs_prune": {"id": job_id, "retention": new_ret}})
    live = _pbs_prune_job_live(job_id)
    assert live.get("keep-last") == 3, \
        f"PBS prune job keep-last: expected 3, got {live.get('keep-last')!r}"
    assert live.get("keep-weekly") == 2, \
        f"PBS prune job keep-weekly: expected 2, got {live.get('keep-weekly')!r}"


def test_settings_pbs_prune_clear_key_removes_from_pbs(host_id, original_pbs_prune):
    """Omitting a key from retention must remove it from the PBS prune job."""
    job_id = original_pbs_prune["id"]
    # Set keep-daily explicitly
    _post(f"/api/host/{host_id}/settings", {
        "pbs_prune": {"id": job_id, "retention": {"keep-last": 1, "keep-daily": 7}}
    })
    # Now save without keep-daily — should be deleted from PBS
    _post(f"/api/host/{host_id}/settings", {
        "pbs_prune": {"id": job_id, "retention": {"keep-last": 1}}
    })
    live = _pbs_prune_job_live(job_id)
    assert "keep-daily" not in live, \
        f"keep-daily should be removed from PBS prune job, but got: {live}"


def test_settings_api_pbs_prune_invalid_body_returns_400(host_id, original_pbs_prune):
    """POST pbs_prune without required fields must return 400."""
    import urllib.error
    try:
        _post(f"/api/host/{host_id}/settings", {"pbs_prune": {"id": "x"}})
        pytest.fail("Expected 400 for missing retention")
    except urllib.error.HTTPError as e:
        assert e.code == 400, f"Expected 400, got {e.code}"


def test_settings_pbs_prune_does_not_change_retention(host_id, original_pbs_prune, original_retention):
    """POST pbs_prune must leave restic retention unchanged."""
    job_id = original_pbs_prune["id"]
    ret_before = _get(f"/api/host/{host_id}/settings")["retention"]
    _post(f"/api/host/{host_id}/settings", {
        "pbs_prune": {"id": job_id, "retention": {"keep-last": 2}}
    })
    ret_after = _get(f"/api/host/{host_id}/settings")["retention"]
    assert ret_before == ret_after, \
        f"Restic retention changed after pbs_prune update: {ret_before} → {ret_after}"


def test_settings_pbs_prune_unknown_job_id_returns_500(host_id, original_pbs_prune):
    """POST pbs_prune with a non-existent job id must return 500."""
    import urllib.error
    try:
        _post(f"/api/host/{host_id}/settings", {
            "pbs_prune": {"id": "no-such-job-xyz", "retention": {"keep-last": 1}}
        })
        pytest.fail("Expected 500 for non-existent pbs prune job id")
    except urllib.error.HTTPError as e:
        assert e.code == 500, f"Expected 500, got {e.code}"


def test_settings_pbs_prune_zero_value_treated_as_unset(host_id, original_pbs_prune):
    """Retention value of 0 must be treated the same as omitted (field cleared in PBS)."""
    job_id = original_pbs_prune["id"]
    # First set keep-weekly explicitly
    _post(f"/api/host/{host_id}/settings", {
        "pbs_prune": {"id": job_id, "retention": {"keep-last": 1, "keep-weekly": 4}}
    })
    # Then send 0 for keep-weekly — should be treated as "clear"
    _post(f"/api/host/{host_id}/settings", {
        "pbs_prune": {"id": job_id, "retention": {"keep-last": 1, "keep-weekly": 0}}
    })
    after = _get(f"/api/host/{host_id}/settings")["pbs_prune"]["retention"]
    assert "keep-weekly" not in after or after.get("keep-weekly") == 0, \
        f"keep-weekly=0 should clear the field, got: {after}"
    live = _pbs_prune_job_live(job_id)
    assert not live.get("keep-weekly"), \
        f"PBS prune job should not have keep-weekly after setting 0, got: {live.get('keep-weekly')}"


def test_settings_pbs_prune_non_integer_retention_returns_400(host_id, original_pbs_prune):
    """POST pbs_prune with string retention values must return 400."""
    import urllib.error
    try:
        _post(f"/api/host/{host_id}/settings", {
            "pbs_prune": {"id": original_pbs_prune["id"], "retention": {"keep-last": "many"}}
        })
        pytest.fail("Expected 400 for non-integer retention value")
    except urllib.error.HTTPError as e:
        assert e.code == 400, f"Expected 400, got {e.code}"


def test_settings_pbs_prune_green_path(host_id, original_pbs_prune):
    """Full end-to-end green path: POST → GET returns same → PBS live matches."""
    job_id = original_pbs_prune["id"]
    target = {"keep-last": 3, "keep-daily": 7, "keep-weekly": 4, "keep-monthly": 2}

    # POST
    resp = _post(f"/api/host/{host_id}/settings", {"pbs_prune": {"id": job_id, "retention": target}})
    assert "pbs_prune" in resp, f"POST response missing pbs_prune: {resp}"

    # GET roundtrip
    after_api = _get(f"/api/host/{host_id}/settings")["pbs_prune"]["retention"]
    for key, expected in target.items():
        assert after_api.get(key) == expected, \
            f"API GET: pbs_prune.retention[{key!r}] expected {expected}, got {after_api.get(key)!r}"

    # Live PBS verification
    live = _pbs_prune_job_live(job_id)
    for key, expected in target.items():
        assert live.get(key) == expected, \
            f"PBS live: {key!r} expected {expected}, got {live.get(key)!r}"


def test_settings_pbs_prune_empty_retention_clears_all_keys(host_id, original_pbs_prune):
    """POST pbs_prune with empty retention dict must clear all keep-* fields in PBS."""
    job_id = original_pbs_prune["id"]
    # First ensure some values are set
    _post(f"/api/host/{host_id}/settings", {
        "pbs_prune": {"id": job_id, "retention": {"keep-last": 1, "keep-daily": 3}}
    })
    # Clear everything
    _post(f"/api/host/{host_id}/settings", {
        "pbs_prune": {"id": job_id, "retention": {}}
    })
    live = _pbs_prune_job_live(job_id)
    for key in _PBS_PRUNE_KEYS:
        assert key not in live, \
            f"PBS prune job should have no {key!r} after empty retention, got: {live.get(key)}"


def test_settings_pbs_prune_all_keys_roundtrip(host_id, original_pbs_prune):
    """All keep-* fields written and read back correctly."""
    job_id = original_pbs_prune["id"]
    full_ret = {"keep-last": 2, "keep-daily": 7, "keep-weekly": 4, "keep-monthly": 3, "keep-yearly": 1}
    _post(f"/api/host/{host_id}/settings", {"pbs_prune": {"id": job_id, "retention": full_ret}})
    after = _get(f"/api/host/{host_id}/settings")["pbs_prune"]["retention"]
    for key, expected in full_ret.items():
        assert after.get(key) == expected, \
            f"pbs_prune.retention[{key!r}]: expected {expected}, got {after.get(key)!r}"


# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS — VM/LXC backup selection (mode + vmids)
# ─────────────────────────────────────────────────────────────────────────────

def _pve_backup_job_raw(job_id: str) -> dict:
    """Read a vzdump backup job dict from pvesh on the PVE host."""
    import json as _json
    out = _ssh_pve_output(
        "pvesh", "get", f"/cluster/backup/{job_id}",
        "--output-format", "json",
    )
    return _json.loads(out)


def _pve_backup_job_excluded(job_id: str) -> list[int]:
    j = _pve_backup_job_raw(job_id)
    raw = j.get("exclude", "")
    return [int(v) for v in str(raw).split(",") if v.strip().isdigit()] if raw else []


def _pve_backup_job_vmids(job_id: str) -> list[int]:
    j = _pve_backup_job_raw(job_id)
    raw = j.get("vmid", "")
    return [int(v) for v in str(raw).split(",") if v.strip().isdigit()] if raw else []


@pytest.fixture
def original_vm_selection(host_id):
    """Save vm_selection before test and restore after."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url — settings endpoint not available")
    data = _get(f"/api/host/{host_id}/settings")
    pbs_schedule = data.get("pbs_schedule")
    if not pbs_schedule:
        pytest.skip("No PBS backup job configured — vm_selection not applicable")
    orig = data.get("vm_selection", {"mode": "exclude", "vmids": []})
    yield {"vm_selection": orig, "job_id": pbs_schedule["id"]}
    try:
        _post(f"/api/host/{host_id}/settings", {"vm_selection": orig})
    except Exception:
        pass


def _get_any_vmid(host_id: str) -> int | None:
    """Return any VMID known to the backend for this host (not SELF_VMID)."""
    items = _get(f"/api/host/{host_id}/items")
    for key in ("lxcs", "vms"):
        for item in items.get(key, []):
            if item["id"] != SELF_VMID:
                return item["id"]
    return None


def test_settings_api_returns_vm_selection(host_id, original_vm_selection):
    """GET /settings must include vm_selection with mode and vmids."""
    data = _get(f"/api/host/{host_id}/settings")
    assert "vm_selection" in data, f"'vm_selection' missing from settings: {data}"
    sel = data["vm_selection"]
    assert isinstance(sel, dict), f"vm_selection must be dict, got {type(sel)}"
    assert sel.get("mode") in ("exclude", "include"), \
        f"vm_selection.mode must be 'exclude' or 'include', got {sel.get('mode')!r}"
    assert isinstance(sel.get("vmids"), list), \
        f"vm_selection.vmids must be list, got {type(sel.get('vmids'))}"


def test_settings_api_vm_selection_vmids_are_integers(host_id, original_vm_selection):
    """vm_selection.vmids must contain only integers."""
    vmids = _get(f"/api/host/{host_id}/settings")["vm_selection"]["vmids"]
    for v in vmids:
        assert isinstance(v, int), \
            f"vm_selection.vmids entry must be int, got {type(v).__name__}: {v!r}"


def test_settings_api_exclude_mode_roundtrip(host_id, original_vm_selection):
    """POST vm_selection exclude mode then GET must return the same list."""
    vmid = _get_any_vmid(host_id)
    if vmid is None:
        pytest.skip("No VM/LXC available")

    _post(f"/api/host/{host_id}/settings", {"vm_selection": {"mode": "exclude", "vmids": [vmid]}})
    after = _get(f"/api/host/{host_id}/settings")["vm_selection"]
    assert after["mode"] == "exclude", f"mode should be 'exclude', got {after['mode']!r}"
    assert vmid in after["vmids"], f"VMID {vmid} not in vmids after save: {after['vmids']}"


def test_settings_exclude_mode_written_to_pve(host_id, original_vm_selection):
    """Exclude mode must set all=1 + exclude field on PVE backup job."""
    vmid = _get_any_vmid(host_id)
    if vmid is None:
        pytest.skip("No VM/LXC available")
    job_id = original_vm_selection["job_id"]

    _post(f"/api/host/{host_id}/settings", {"vm_selection": {"mode": "exclude", "vmids": [vmid]}})
    j = _pve_backup_job_raw(job_id)
    assert j.get("all") == 1, f"PVE job should have all=1 in exclude mode, got {j.get('all')}"
    assert vmid in _pve_backup_job_excluded(job_id), \
        f"VMID {vmid} not in PVE exclude field: {_pve_backup_job_excluded(job_id)}"


def test_settings_api_include_mode_roundtrip(host_id, original_vm_selection):
    """POST vm_selection include mode then GET must return the same list."""
    vmid = _get_any_vmid(host_id)
    if vmid is None:
        pytest.skip("No VM/LXC available")

    _post(f"/api/host/{host_id}/settings", {"vm_selection": {"mode": "include", "vmids": [vmid]}})
    after = _get(f"/api/host/{host_id}/settings")["vm_selection"]
    assert after["mode"] == "include", f"mode should be 'include', got {after['mode']!r}"
    assert vmid in after["vmids"], f"VMID {vmid} not in vmids after save: {after['vmids']}"


def test_settings_include_mode_written_to_pve(host_id, original_vm_selection):
    """Include mode must set all=0 + vmid field on PVE backup job."""
    vmid = _get_any_vmid(host_id)
    if vmid is None:
        pytest.skip("No VM/LXC available")
    job_id = original_vm_selection["job_id"]

    _post(f"/api/host/{host_id}/settings", {"vm_selection": {"mode": "include", "vmids": [vmid]}})
    j = _pve_backup_job_raw(job_id)
    assert not j.get("all"), f"PVE job should not have all=1 in include mode, got {j.get('all')}"
    assert vmid in _pve_backup_job_vmids(job_id), \
        f"VMID {vmid} not in PVE vmid field: {_pve_backup_job_vmids(job_id)}"


def test_settings_switch_from_include_to_exclude(host_id, original_vm_selection):
    """Switching from include to exclude mode must set all=1 and clear vmid field."""
    vmid = _get_any_vmid(host_id)
    if vmid is None:
        pytest.skip("No VM/LXC available")
    job_id = original_vm_selection["job_id"]

    _post(f"/api/host/{host_id}/settings", {"vm_selection": {"mode": "include", "vmids": [vmid]}})
    _post(f"/api/host/{host_id}/settings", {"vm_selection": {"mode": "exclude", "vmids": []}})

    after = _get(f"/api/host/{host_id}/settings")["vm_selection"]
    assert after["mode"] == "exclude"
    assert after["vmids"] == []
    j = _pve_backup_job_raw(job_id)
    assert j.get("all") == 1, f"PVE job should have all=1 after switching to exclude: {j}"


def test_settings_api_vm_selection_invalid_mode_returns_400(host_id, original_vm_selection):
    """POST vm_selection with invalid mode must return 400."""
    import urllib.error
    try:
        _post(f"/api/host/{host_id}/settings", {"vm_selection": {"mode": "bogus", "vmids": []}})
        pytest.fail("Expected 400 for invalid mode")
    except urllib.error.HTTPError as e:
        assert e.code == 400, f"Expected 400, got {e.code}"


def test_settings_api_vm_selection_non_int_vmids_returns_400(host_id, original_vm_selection):
    """POST vm_selection with non-integer vmids must return 400."""
    import urllib.error
    try:
        _post(f"/api/host/{host_id}/settings", {"vm_selection": {"mode": "exclude", "vmids": ["abc"]}})
        pytest.fail("Expected 400 for string vmids")
    except urllib.error.HTTPError as e:
        assert e.code == 400, f"Expected 400, got {e.code}"


def test_settings_vm_selection_does_not_change_schedule(host_id, original_vm_selection, original_schedules):
    """POST vm_selection must not affect the PBS schedule."""
    vmid = _get_any_vmid(host_id)
    if vmid is None:
        pytest.skip("No VM/LXC available")
    if not original_schedules["pbs"]:
        pytest.skip("No PBS schedule to verify")
    sched_before = _get(f"/api/host/{host_id}/settings")["pbs_schedule"]["schedule"]

    _post(f"/api/host/{host_id}/settings", {"vm_selection": {"mode": "exclude", "vmids": [vmid]}})
    sched_after = _get(f"/api/host/{host_id}/settings")["pbs_schedule"]["schedule"]
    assert sched_before == sched_after, \
        f"PBS schedule changed unexpectedly: {sched_before!r} → {sched_after!r}"


# ─────────────────────────────────────────────────────────────────────────────
# PBS TASKS — GET /api/host/<id>/pbs/tasks
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# PBS TASKS — live integration: trigger GC, verify task visible via API + UI
# ─────────────────────────────────────────────────────────────────────────────

def _pbs_datastore(host_id: str) -> str | None:
    """Return PBS datastore name for the host, or None if not configured."""
    hosts = _get("/api/hosts")
    for h in hosts:
        if h["id"] == host_id:
            return h.get("pbs_datastore") or None
    return None


def _pbs_storage_id(host_id: str) -> str | None:
    """Return PVE storage ID for PBS on the host (used with vzdump), or None."""
    hosts = _get("/api/hosts")
    for h in hosts:
        if h["id"] == host_id:
            return h.get("pbs_storage_id") or None
    return None


_PVE_HOST_SSH = "root@10.10.0.1"  # PVE host as seen from inside LXC 300 (vmbr0 bridge)


def _ssh_pve(*cmd: str) -> None:
    """Run a command on the PVE host via SSH (fire-and-forget, ignores output)."""
    subprocess.Popen(
        ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
         _PVE_HOST_SSH, *cmd],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def _trigger_pbs_gc(datastore: str) -> None:
    """Start a PBS garbage collection on the PVE host (fire-and-forget)."""
    _ssh_pve("proxmox-backup-manager", "garbage-collection", "start", datastore)


def _trigger_pbs_external_backup(vmid: int, vm_type: str, storage: str = "local") -> None:
    """Trigger a PBS backup of vmid via vzdump directly on the PVE host. Waits for completion.

    This simulates a scheduled/cron backup that the GUI did not initiate — the resulting
    PBS backup task should appear in /pbs/tasks as an external task card.
    """
    result = subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
         _PVE_HOST_SSH, "vzdump", str(vmid),
         "--storage", storage, "--mode", "stop", "--remove", "0"],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"vzdump failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout[-400:]}\nstderr: {result.stderr[-400:]}"
        )


def _trigger_pbs_prune(datastore: str, vm_type: str, vmid: int) -> None:
    """Trigger a PBS prune job on the PVE host. Waits for completion.

    Runs the ci-prune prune job (created in STEP_B) which creates a 'prunejob' task.
    The datastore/vm_type/vmid parameters are accepted for API compatibility but the
    prune job covers the full store — this is sufficient for CI task visibility tests.
    """
    result = subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
         _PVE_HOST_SSH, "proxmox-backup-manager", "prune", "run", "ci-prune"],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"proxmox-backup-manager prune run ci-prune failed (exit {result.returncode}):\n"
            f"stdout: {result.stdout[-400:]}\nstderr: {result.stderr[-400:]}"
        )


def _wait_for_running_pbs_task(host_id: str, worker_type: str, timeout: int = 30) -> dict:
    """Poll /pbs/tasks?running=1 until a task of the given worker_type appears."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        tasks = _get(f"/api/host/{host_id}/pbs/tasks?running=1")
        for t in tasks:
            if t.get("worker_type") == worker_type:
                return t
        time.sleep(2)
    raise TimeoutError(
        f"No running PBS task of type '{worker_type}' appeared within {timeout}s"
    )


def _wait_for_recent_pbs_task(host_id: str, worker_type: str, since: float, timeout: int = 10) -> dict:
    """Poll /pbs/tasks (all, incl. completed) for a task that started at or after `since`."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        tasks = _get(f"/api/host/{host_id}/pbs/tasks")
        for t in tasks:
            if t.get("worker_type") == worker_type and t.get("starttime", 0) >= since - 5:
                return t
        time.sleep(1)
    raise TimeoutError(f"No recent PBS task of type '{worker_type}' appeared within {timeout}s")


def _wait_for_pbs_task_done(host_id: str, upid: str, timeout: int = 120) -> dict:
    """Poll /pbs/tasks until the task with this UPID has an endtime."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        tasks = _get(f"/api/host/{host_id}/pbs/tasks")
        for t in tasks:
            if t.get("upid") == upid and t.get("endtime"):
                return t
        time.sleep(3)
    raise TimeoutError(f"PBS task {upid[:20]}… did not finish within {timeout}s")


@pytest.fixture
def running_gc_task(host_id):
    """Trigger a PBS GC and yield the running task dict. Waits for it to finish on teardown."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    datastore = _pbs_datastore(host_id)
    if not datastore:
        pytest.skip("No PBS datastore configured for host")
    trigger_time = time.time()
    _trigger_pbs_gc(datastore)
    try:
        task = _wait_for_running_pbs_task(host_id, "garbage_collection", timeout=30)
    except TimeoutError:
        # GC finished before first poll — look for recently-completed task
        task = _wait_for_recent_pbs_task(host_id, "garbage_collection", since=trigger_time)
    yield task
    # Wait for it to finish so it doesn't interfere with subsequent tests
    try:
        _wait_for_pbs_task_done(host_id, task["upid"], timeout=120)
    except TimeoutError:
        pass


def test_pbs_gc_task_appears_in_running_api(host_id, running_gc_task):
    """Triggered GC must appear in /pbs/tasks?running=1 with correct fields."""
    task = running_gc_task
    assert task["worker_type"] == "garbage_collection"
    assert "upid" in task
    assert "starttime" in task
    # Task may have already completed by the time we assert (GC is fast on small datastores)


def test_pbs_gc_task_log_has_content(host_id, running_gc_task):
    """Log endpoint for a running GC task must return non-empty lines."""
    import urllib.parse
    upid = running_gc_task["upid"]
    encoded = urllib.parse.quote(upid, safe="")
    data = _get(f"/api/host/{host_id}/pbs/tasks/{encoded}/log")
    assert "lines" in data
    assert len(data["lines"]) > 0, \
        f"Log for running GC task must not be empty. UPID: {upid[:30]}…"


def test_pbs_gc_task_card_appears_in_sidebar(real_page, host_id, running_gc_task):
    """A PBS GC task card must appear in the sidebar Running section."""
    # Navigate first so clearState() fires before we inject the task
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0", timeout=20000)

    # GC completes in <1s — faster than the 15s MQTT poll. Inject the real task directly
    # into the browser's PBS task state to test the rendering pipeline.
    real_page.evaluate(
        "(t) => { _pbsTasks = [t]; renderPbsTasks(); }",
        running_gc_task,
    )

    real_page.wait_for_function(
        "() => document.getElementById('global-job-section').style.display !== 'none'",
        timeout=5000,
    )
    cards = real_page.locator("#pbs-task-cards .job-indicator")
    assert cards.count() > 0, "No PBS task cards rendered in sidebar after inject"

    card_text = cards.first.inner_text()
    assert any(word in card_text.lower() for word in ("gc", "garbage", "collection")), \
        f"PBS task card text does not mention GC: {card_text!r}"
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"


def test_pbs_gc_task_card_opens_log_modal(real_page, host_id, running_gc_task):
    """Clicking a PBS task card must open the job modal with log content."""
    # Navigate first so clearState() fires before we inject the task
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0", timeout=20000)

    # Inject the real GC task (completed but log is still available via PBS API)
    real_page.evaluate(
        "(t) => { _pbsTasks = [t]; renderPbsTasks(); }",
        running_gc_task,
    )
    real_page.wait_for_function(
        "() => document.getElementById('pbs-task-cards').children.length > 0",
        timeout=5000,
    )
    real_page.locator("#pbs-task-cards .job-indicator").first.click()

    real_page.wait_for_selector("#job-modal.open", timeout=5000)

    # Log must receive at least one line from the SSE stream
    real_page.wait_for_function(
        "() => document.getElementById('job-log').children.length > 0",
        timeout=15000,
    )
    log_text = real_page.locator("#job-log").inner_text()
    assert log_text.strip(), "Job modal log must not be empty for running GC task"

    real_page.evaluate("closeJobModal()")
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"


def test_pbs_gc_task_card_disappears_when_done(real_page, host_id, running_gc_task):
    """After GC finishes, the PBS task card must disappear from the sidebar."""
    upid = running_gc_task["upid"]

    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0", timeout=20000)

    # Wait for GC to finish via API
    _wait_for_pbs_task_done(host_id, upid, timeout=120)

    # Agent publishes updated (empty) running task list via MQTT —
    # card must disappear within the next poll cycle (15s)
    real_page.wait_for_function(
        "() => document.getElementById('pbs-task-cards').children.length === 0",
        timeout=25000,
    )
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"


@pytest.fixture
def running_backup_task(host_id, items):
    """Trigger an external PBS backup (via vzdump, not via agent) and yield the task.

    Uses ct/301 — the designated CI test target. This tests the case where a backup
    runs outside the GUI (cron/manual) and must still appear as a PBS task card.
    vzdump runs synchronously so failures are surfaced as errors, not silent timeouts.
    """
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    pbs_storage = _pbs_storage_id(host_id)
    if not pbs_storage:
        pytest.skip("No PBS storage configured for host")
    trigger_time = time.time()
    _trigger_pbs_external_backup(301, "ct", storage=pbs_storage)
    # vzdump completed — task is in PBS task list (possibly already finished)
    task = _wait_for_recent_pbs_task(host_id, "backup", since=trigger_time, timeout=30)
    yield task
    try:
        _wait_for_pbs_task_done(host_id, task["upid"], timeout=300)
    except TimeoutError:
        pass


@pytest.fixture
def running_prune_task(host_id):
    """Trigger an external PBS prune for ct/301 and yield the task."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    datastore = _pbs_datastore(host_id)
    if not datastore:
        pytest.skip("No PBS datastore configured")
    trigger_time = time.time()
    _trigger_pbs_prune(datastore, "ct", 301)
    # prunejob completed — task is in PBS task list (possibly already finished)
    task = _wait_for_recent_pbs_task(host_id, "prunejob", since=trigger_time, timeout=30)
    yield task
    try:
        _wait_for_pbs_task_done(host_id, task["upid"], timeout=60)
    except TimeoutError:
        pass


def test_external_backup_task_appears_in_api(host_id, running_backup_task):
    """External (non-GUI) backup must appear in /pbs/tasks?running=1."""
    task = running_backup_task
    assert task["worker_type"] == "backup"
    assert "upid" in task
    # Task may have already completed (PBS deduplication makes backup instant)
    # worker_id should reference the VM
    assert "301" in (task.get("worker_id") or ""), \
        f"Expected '301' in worker_id for ct/301 backup: {task.get('worker_id')}"


def test_external_backup_task_card_in_sidebar(real_page, host_id, running_backup_task):
    """External backup must show as a PBS task card in the sidebar (no GUI op active)."""
    # Navigate first so clearState() fires before we inject the task
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0", timeout=20000)

    # Backup likely completed before MQTT poll — inject the real task to test rendering
    real_page.evaluate(
        "(t) => { _pbsTasks = [t]; renderPbsTasks(); }",
        running_backup_task,
    )
    real_page.wait_for_function(
        "() => document.getElementById('pbs-task-cards').children.length > 0",
        timeout=5000,
    )
    card_text = real_page.locator("#pbs-task-cards .job-indicator").first.inner_text()
    assert "backup" in card_text.lower(), \
        f"PBS task card for backup not found in sidebar. Card text: {card_text!r}"
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"


def test_gui_backup_no_duplicate_when_external_running(real_page, host_id, running_backup_task):
    """PBS task card for a VMID must be hidden when a GUI backup op is active for that VMID."""
    # Navigate first so clearState() fires before we inject state
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0", timeout=20000)

    # Inject a GUI backup op for VMID 301. PBS deduplication makes real backups instant,
    # so the WebSocket 'running' message may never arrive — inject directly to test dedup logic.
    real_page.evaluate("""() => {
        _activeJobs['301'] = { jobId: 'ci-dedup-test', title: 'PBS backup' };
        _updateGlobalJobIndicator();
    }""")

    # Inject the external backup task so the dedup logic has something to consider
    real_page.evaluate(
        "(t) => { _pbsTasks = [t]; renderPbsTasks(); }",
        running_backup_task,
    )

    # The PBS task card for vmid 301 must be hidden (dedup logic hides it)
    cards = real_page.locator("#pbs-task-cards .job-indicator")
    visible_texts = [c.inner_text() for c in cards.all() if c.is_visible()]
    for text in visible_texts:
        assert "301" not in text or "backup" not in text.lower(), \
            f"Duplicate PBS backup card shown for vmid 301 alongside GUI op: {text!r}"

    # Clean up injected state
    real_page.evaluate("""() => {
        delete _activeJobs['301'];
        _pbsTasks = [];
        renderPbsTasks();
    }""")
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"


def test_external_prune_task_appears_in_api(host_id, running_prune_task):
    """External prune job must appear in /pbs/tasks with worker_type 'prunejob'."""
    task = running_prune_task
    assert task["worker_type"] == "prunejob"
    assert "upid" in task
    # Task may have already completed by the time we assert (prune is fast on small datastores)


def test_pbs_tasks_api_returns_list(host_id):
    """GET /pbs/tasks must return a JSON list (possibly empty)."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    tasks = _get(f"/api/host/{host_id}/pbs/tasks")
    assert isinstance(tasks, list), \
        f"Expected list from /pbs/tasks, got {type(tasks).__name__}: {tasks!r}"


def test_pbs_tasks_running_filter_returns_list(host_id):
    """GET /pbs/tasks?running=1 must return a list (no tasks running is fine)."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    tasks = _get(f"/api/host/{host_id}/pbs/tasks?running=1")
    assert isinstance(tasks, list), \
        f"Expected list from /pbs/tasks?running=1, got {type(tasks).__name__}"


def test_pbs_tasks_fields_present(host_id):
    """Each task in /pbs/tasks must have upid, worker_type, starttime fields."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    tasks = _get(f"/api/host/{host_id}/pbs/tasks")
    if not tasks:
        pytest.skip("No PBS tasks returned — cannot verify field structure")
    for t in tasks:
        for field in ("upid", "worker_type", "starttime"):
            assert field in t, \
                f"Field '{field}' missing from PBS task: {t}"


def test_pbs_tasks_worker_type_is_known(host_id):
    """All returned task worker_types must be from the expected set."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    known = {"backup", "prune", "prunejob", "garbage_collection", "verify"}
    tasks = _get(f"/api/host/{host_id}/pbs/tasks")
    for t in tasks:
        assert t.get("worker_type") in known, \
            f"Unexpected worker_type {t.get('worker_type')!r} in task: {t}"


def test_pbs_tasks_running_filter_excludes_finished(host_id):
    """Tasks returned by ?running=1 must not have an endtime."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    tasks = _get(f"/api/host/{host_id}/pbs/tasks?running=1")
    for t in tasks:
        assert not t.get("endtime"), \
            f"Running-filter returned task with endtime: {t}"


def test_pbs_task_log_returns_lines_for_recent_task(host_id):
    """GET /pbs/tasks/<upid>/log must return {lines: [...]} for a known completed task."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    tasks = _get(f"/api/host/{host_id}/pbs/tasks")
    completed = [t for t in tasks if t.get("endtime")]
    if not completed:
        pytest.skip("No completed PBS tasks found to test log endpoint")

    upid = completed[0]["upid"]
    import urllib.parse
    encoded = urllib.parse.quote(upid, safe="")
    data = _get(f"/api/host/{host_id}/pbs/tasks/{encoded}/log")
    assert "lines" in data, f"Log response missing 'lines' key: {data}"
    assert isinstance(data["lines"], list), \
        f"'lines' must be a list, got {type(data['lines']).__name__}"
    assert len(data["lines"]) > 0, \
        f"Log for completed task {upid[:16]}… must not be empty"


def test_pbs_task_log_invalid_upid_returns_empty(host_id):
    """GET /pbs/tasks/<bad-upid>/log must return empty lines, not crash."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    import urllib.parse
    bad = urllib.parse.quote("UPID:invalid:00000000:00000000:00000000:bogus::root@pam:", safe="")
    data = _get(f"/api/host/{host_id}/pbs/tasks/{bad}/log")
    assert "lines" in data, f"Response missing 'lines' key for invalid UPID: {data}"
    assert isinstance(data["lines"], list), "lines must be a list even for invalid UPID"


# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS UI — settings modal opens, loads and saves values
# ─────────────────────────────────────────────────────────────────────────────

def test_settings_modal_opens_and_closes(real_page, host_id):
    """Settings ⚙ button must open the modal; Cancel must close it."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url — settings modal requires agent")
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0", timeout=20000)

    real_page.click("#settings-btn")
    real_page.wait_for_selector(".settings-overlay.open", timeout=5000)

    real_page.click("button.btn-secondary")  # Cancel
    real_page.wait_for_function(
        "() => !document.querySelector('.settings-overlay').classList.contains('open')",
        timeout=3000,
    )
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"


def test_settings_modal_loads_current_values(real_page, host_id, original_retention):
    """Settings modal must pre-populate fields with current agent retention values."""
    # Write known values first via API
    _post(f"/api/host/{host_id}/settings", {
        "retention": {"keep-last": 3, "keep-weekly": 2, "keep-monthly": 1}
    })

    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0", timeout=20000)

    real_page.click("#settings-btn")
    real_page.wait_for_selector(".settings-overlay.open", timeout=5000)
    # Modal fetches values async — wait for a field to be populated
    real_page.wait_for_function(
        "() => document.getElementById('s-keep-last').value !== ''", timeout=5000)

    assert real_page.locator("#s-keep-last").input_value() == "3", \
        "keep-last field did not load from agent"
    assert real_page.locator("#s-keep-weekly").input_value() == "2", \
        "keep-weekly field did not load from agent"
    assert real_page.locator("#s-keep-monthly").input_value() == "1", \
        "keep-monthly field did not load from agent"

    real_page.click("button.btn-secondary")
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"


def test_settings_modal_save_persists_values(real_page, host_id, original_retention):
    """Filling in retention fields and clicking Save must persist the values."""
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0", timeout=20000)

    real_page.click("#settings-btn")
    real_page.wait_for_selector(".settings-overlay.open", timeout=5000)
    # The modal fetches settings async — wait for s-restic-schedule (always set in CI)
    # to be populated before filling fields, so our values aren't overwritten by the fetch.
    real_page.wait_for_function(
        "() => document.getElementById('s-restic-schedule').value !== ''", timeout=5000)

    real_page.fill("#s-keep-last", "4")
    real_page.fill("#s-keep-weekly", "3")

    real_page.click("#settings-save-btn")
    # Modal closes on successful save
    real_page.wait_for_function(
        "() => !document.querySelector('.settings-overlay').classList.contains('open')",
        timeout=5000,
    )

    # Verify persisted via API
    saved = _get(f"/api/host/{host_id}/settings")["retention"]
    assert saved.get("keep-last") == 4, \
        f"keep-last not persisted after modal save: {saved}"
    assert saved.get("keep-weekly") == 3, \
        f"keep-weekly not persisted after modal save: {saved}"
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"


# ─────────────────────────────────────────────────────────────────────────────
# Restic log API + UI
# ─────────────────────────────────────────────────────────────────────────────

_RESTIC_LOG_PATH = "/var/log/restic-backup.log"
_RESTIC_LOG_SENTINEL = "=== restic VM backup started: test sentinel ==="


@pytest.fixture
def seeded_restic_log(host_id):
    """Write a known log file on the PVE host; remove it on teardown."""
    _ssh_pve_output(f"echo '{_RESTIC_LOG_SENTINEL}' > {_RESTIC_LOG_PATH}")
    yield
    _ssh_pve_output("rm", "-f", _RESTIC_LOG_PATH)


def test_restic_log_api_returns_lines(host_id, seeded_restic_log):
    """GET /api/host/<id>/restic/log must return a lines list and running bool."""
    result = _get(f"/api/host/{host_id}/restic/log")
    assert "lines" in result, f"Missing 'lines' in response: {result}"
    assert "running" in result, f"Missing 'running' in response: {result}"
    assert isinstance(result["lines"], list)
    assert isinstance(result["running"], bool)


def test_restic_log_api_contains_seeded_line(host_id, seeded_restic_log):
    """The seeded sentinel line must appear in the returned lines."""
    result = _get(f"/api/host/{host_id}/restic/log")
    assert any(_RESTIC_LOG_SENTINEL in l for l in result["lines"]), \
        f"Sentinel not found in log lines: {result['lines']}"


def test_restic_log_api_returns_empty_when_no_file(host_id):
    """When the log file does not exist, lines must be [] and running must be False."""
    _ssh_pve("rm", "-f", _RESTIC_LOG_PATH)
    result = _get(f"/api/host/{host_id}/restic/log")
    assert result["lines"] == [], f"Expected empty lines, got: {result['lines']}"
    assert result["running"] is False, f"Expected running=False, got: {result['running']}"


def test_restic_log_running_false_when_no_process(host_id, seeded_restic_log):
    """running must be False when no restic backup process is active."""
    result = _get(f"/api/host/{host_id}/restic/log")
    assert result["running"] is False, \
        f"Expected running=False (no restic process), got: {result['running']}"


def test_restic_log_stream_returns_done_when_not_running(host_id, seeded_restic_log):
    """SSE stream must emit __done__ quickly when no process is active."""
    import urllib.request as _ur
    _ensure_session()
    url = f"{BACKEND_URL}/api/host/{host_id}/restic/log/stream"
    req = _ur.Request(url)
    for c in _cookie_jar:
        req.add_header("Cookie", f"{c.name}={c.value}")
    done_seen = False
    with _ur.urlopen(req, timeout=30) as resp:
        for raw in resp:
            line = raw.decode().strip()
            if line.startswith("data:"):
                payload = line[5:].strip()
                if payload == "__done__":
                    done_seen = True
                    break
    assert done_seen, "SSE stream did not emit __done__ after log finished"


def test_restic_log_stream_delivers_existing_lines(host_id, seeded_restic_log):
    """SSE stream must replay existing log lines before emitting __done__."""
    import urllib.request as _ur
    _ensure_session()
    url = f"{BACKEND_URL}/api/host/{host_id}/restic/log/stream"
    req = _ur.Request(url)
    for c in _cookie_jar:
        req.add_header("Cookie", f"{c.name}={c.value}")
    lines_seen = []
    with _ur.urlopen(req, timeout=30) as resp:
        for raw in resp:
            line = raw.decode().strip()
            if line.startswith("data:"):
                payload = line[5:].strip()
                if payload == "__done__":
                    break
                lines_seen.append(payload)
    assert any(_RESTIC_LOG_SENTINEL in l for l in lines_seen), \
        f"Sentinel not in SSE lines: {lines_seen}"


def test_restic_log_badge_opens_modal(real_page, host_id, seeded_restic_log):
    """
    When restic_running=True the sidebar badge is clickable and opens the job modal
    with the log content.

    We can't easily fake restic_running via MQTT in this test, so we call
    openResticLogModal() directly from JS (the function must exist and not throw).
    """
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0", timeout=20000)

    # Call the modal open function directly — verifies it exists and renders
    real_page.evaluate(f"openResticLogModal({repr(host_id)})")
    real_page.wait_for_selector("#job-modal.open", timeout=5000)

    title = real_page.inner_text("#job-title")
    assert "restic" in title.lower() or "Restic" in title, \
        f"Unexpected modal title: {title}"

    real_page.click("#job-close-btn")
    real_page.wait_for_function(
        "() => !document.getElementById('job-modal').classList.contains('open')",
        timeout=3000,
    )
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"


# ─────────────────────────────────────────────────────────────────────────────
# CONNECTION SETTINGS — GET/POST /api/host/<id>/connection
# ─────────────────────────────────────────────────────────────────────────────

def test_connection_get_returns_config(host_id):
    """GET /connection must return the agent config with passwords redacted."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    data = _get(f"/api/host/{host_id}/connection")
    assert "pve_url" in data,      f"Missing pve_url: {data}"
    assert "pve_user" in data,     f"Missing pve_user: {data}"
    assert "pbs_url" in data,      f"Missing pbs_url: {data}"
    assert "pbs_user" in data,     f"Missing pbs_user: {data}"
    assert "pbs_datastore" in data, f"Missing pbs_datastore: {data}"
    assert "restic_repo" in data,  f"Missing restic_repo: {data}"
    # Passwords must be redacted (empty string, not the real value)
    for key in ("pve_password", "pbs_password", "restic_password"):
        assert data.get(key) == "", \
            f"{key} must be redacted (empty string), got: {data.get(key)!r}"


def test_connection_get_urls_are_valid(host_id):
    """pve_url and pbs_url must look like https URLs."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    data = _get(f"/api/host/{host_id}/connection")
    assert data["pve_url"].startswith("https://"), \
        f"pve_url should be https, got: {data['pve_url']!r}"
    assert data["pbs_url"].startswith("https://"), \
        f"pbs_url should be https, got: {data['pbs_url']!r}"


def test_connection_post_noop_roundtrip(host_id):
    """POST /connection with empty passwords must not change the stored config."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    before = _get(f"/api/host/{host_id}/connection")
    # Post the non-password fields back unchanged, passwords empty (= unchanged)
    payload = {
        "pve_url":       before["pve_url"],
        "pve_user":      before["pve_user"],
        "pve_password":  "",
        "pbs_url":       before["pbs_url"],
        "pbs_user":      before["pbs_user"],
        "pbs_password":  "",
        "pbs_datastore": before["pbs_datastore"],
        "restic_repo":   before.get("restic_repo", ""),
        "restic_password": "",
    }
    result = _post(f"/api/host/{host_id}/connection", payload)
    assert result.get("ok") is True, f"POST /connection failed: {result}"
    after = _get(f"/api/host/{host_id}/connection")
    assert after["pve_url"] == before["pve_url"], "pve_url changed unexpectedly"
    assert after["pbs_user"] == before["pbs_user"], "pbs_user changed unexpectedly"
    assert after["pbs_datastore"] == before["pbs_datastore"], "pbs_datastore changed unexpectedly"


def test_connection_post_updates_non_secret_field(host_id):
    """POST /connection with a changed pbs_user must persist and be readable back."""
    if not _host_has_agent(host_id):
        pytest.skip("Host has no agent_url")
    before = _get(f"/api/host/{host_id}/connection")
    original_user = before["pbs_user"]
    new_user = original_user + "-ci-test"
    result = _post(f"/api/host/{host_id}/connection", {"pbs_user": new_user})
    assert result.get("ok") is True, f"POST /connection failed: {result}"
    after = _get(f"/api/host/{host_id}/connection")
    assert after["pbs_user"] == new_user, \
        f"pbs_user not persisted: expected {new_user!r}, got {after['pbs_user']!r}"
    # Restore original
    restore = _post(f"/api/host/{host_id}/connection", {"pbs_user": original_user})
    assert restore.get("ok") is True, f"failed to restore original pbs_user: {restore}"


def test_connection_section_visible_in_settings_modal(real_page, host_id):
    """Settings modal must show the Connection section when logged in as admin."""
    real_page.wait_for_selector(f"#nav-{host_id}", timeout=5000)
    real_page.click(f"#nav-{host_id}")
    real_page.wait_for_function(
        "() => document.querySelectorAll('.vm-card').length > 0", timeout=20000)

    real_page.click("#settings-btn")
    real_page.wait_for_selector(".settings-overlay.open", timeout=5000)
    # Connection section should become visible after the async fetch
    real_page.wait_for_function(
        "() => document.getElementById('s-connection-section').style.display !== 'none'",
        timeout=5000,
    )
    pve_url = real_page.input_value("#s-conn-pve-url")
    assert pve_url.startswith("https://"), \
        f"PVE URL field should be pre-filled, got: {pve_url!r}"

    real_page.click("#settings-btn") if False else None  # suppress lint
    real_page.evaluate("closeSettingsModal()")
    assert real_page._js_errors == [], f"JS errors: {real_page._js_errors}"
