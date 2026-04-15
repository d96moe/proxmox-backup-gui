"""Self-restore integration tests — run LAST (after test_restore.py).

pytest collects files alphabetically: test_restore.py → test_self_restore.py.
The execution test kills Flask (PVE stops ct/300 mid-job), so it must run
after all other integration tests to avoid breaking the session.

BACKEND_URL=http://<ip>:5000 must be set. All tests skipped otherwise.
"""
from __future__ import annotations

import http.cookiejar
import json
import os
import time
import urllib.request

import pytest

BACKEND_URL = os.environ.get("BACKEND_URL", "").rstrip("/")
CI_ADMIN_PASSWORD = os.environ.get("CI_ADMIN_PASSWORD", "")

pytestmark = pytest.mark.skipif(
    not BACKEND_URL,
    reason="BACKEND_URL not set",
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
# HTTP helpers (duplicated from test_restore.py — no shared module to keep
# files self-contained and avoid import-order issues)
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
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = _get(f"/api/job/{job_id}")
        if job["status"] in ("done", "error"):
            return job
        time.sleep(4)
    raise TimeoutError(f"Job {job_id} did not finish within {timeout}s")


def _job_ok(job: dict) -> str:
    if job["status"] == "done":
        return ""
    logs = "\n  ".join(job.get("logs", []))
    return f"status={job['status']}\n  {logs}"


def _items(host_id: str) -> dict:
    return _get(f"/api/host/{host_id}/items")


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def host_id():
    hosts = _get("/api/hosts")
    assert hosts, "No hosts configured"
    return hosts[0]["id"]


@pytest.fixture
def real_page(browser):
    ctx = browser.new_context(base_url=BACKEND_URL)
    ctx.route("**fonts.googleapis.com**",
              lambda r: r.fulfill(status=200, content_type="text/css", body=""))
    ctx.route("**fonts.gstatic.com**",
              lambda r: r.fulfill(status=200, content_type="font/woff2", body=b""))
    pg = ctx.new_page()
    pg._js_errors = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    if CI_ADMIN_PASSWORD:
        pg.goto("/login")
        pg.fill("#username", "admin")
        pg.fill("#password", CI_ADMIN_PASSWORD)
        pg.click("#btn-login")
        pg.wait_for_url("/", timeout=5000)
    else:
        pg.goto("/")
    pg.wait_for_function(
        "() => document.querySelector('.vm-card') !== null",
        timeout=45000,
    )
    yield pg
    ctx.close()


# ─────────────────────────────────────────────────────────────────────────────
# SELF-RESTORE WARNING — UI shows warning + disabled confirm until ack checked.
# Does NOT execute a real restore — safe to run before the execution test.
# ─────────────────────────────────────────────────────────────────────────────

def test_self_restore_warning_shown_in_modal(real_page, host_id):
    """Restore modal for ct/300 (SELF_VMID) must show a self-restore warning
    and the confirm button must be disabled until the ack checkbox is checked."""
    # Create a fresh PBS backup of ct/300 so a restore button exists
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

    real_page.locator(".vm-card:has([data-vmid='300'])").locator(".expand-btn").first.click()
    restore_btn = real_page.locator(".restore-btn[data-vmid='300']").first
    restore_btn.wait_for(state="visible", timeout=5000)
    restore_btn.click()

    real_page.wait_for_selector("#modal.open", timeout=5000)

    warning = real_page.locator("#modal-self-warning")
    assert warning.is_visible(), \
        "Self-restore warning (#modal-self-warning) not visible for ct/300"

    confirm_btn = real_page.locator("#modal-confirm-btn")
    assert not confirm_btn.is_enabled(), \
        "Confirm button must be DISABLED before self-restore ack checkbox is checked"

    real_page.locator("#modal-self-confirm").check()
    assert confirm_btn.is_enabled(), \
        "Confirm button must be ENABLED after checking self-restore ack checkbox"

    real_page.evaluate("closeModal()")
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


# ─────────────────────────────────────────────────────────────────────────────
# SELF-RESTORE EXECUTION — actually restore ct/300, verify Flask recovers.
#
# This test KILLS Flask (PVE stops ct/300 mid-job). It MUST be last.
# All tests in test_restore.py finish before this file is collected.
# ─────────────────────────────────────────────────────────────────────────────

def test_self_restore_executes_and_backend_recovers(host_id):
    """Execute a real self-restore of ct/300, then verify Flask comes back online.

    Timeline:
    1. Create PBS backup of ct/300
    2. POST /restore → background thread starts in ct/300
    3. PVE stops ct/300 (Flask dies), restores from PBS snapshot (~60s)
    4. PVE starts ct/300 → Flask restarts via systemctl
    5. Verify backend reachable and /items works
    """
    # Create a backup to restore from
    resp = _post(f"/api/host/{host_id}/backup/pbs", {"vmid": 300, "type": "ct"})
    if "job_id" not in resp:
        pytest.skip(f"Could not create backup of ct/300: {resp}")
    backup_job = _poll_job(resp["job_id"], timeout=180)
    if backup_job["status"] != "done":
        pytest.skip(f"Backup of ct/300 failed: {_job_ok(backup_job)}")

    fresh = _items(host_id)
    ct300 = next((i for i in fresh.get("lxcs", []) if i["id"] == 300), None)
    assert ct300, "ct/300 not in items after backup"
    local_snaps = sorted([s for s in ct300["snapshots"] if s.get("local")],
                         key=lambda s: s["backup_time"])
    assert local_snaps, "No local snapshots for ct/300 after backup"
    backup_time = local_snaps[-1]["backup_time"]

    # Submit self-restore — Flask will die mid-job
    try:
        resp = _post(f"/api/host/{host_id}/restore", {
            "vmid": 300, "type": "ct",
            "source": "local", "backup_time": backup_time,
        })
    except Exception as e:
        pytest.skip(f"Could not POST restore: {e}")

    assert "job_id" in resp, f"No job_id in self-restore response: {resp}"
    job_id = resp["job_id"]

    # Poll until Flask becomes unreachable or job completes
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            status = _get(f"/api/job/{job_id}")
            if status.get("status") in ("done", "error"):
                break  # completed before ct/300 stopped (race)
            time.sleep(2)
        except Exception:
            break  # Flask is down — restore in progress

    # Flask has died — old session cookie is invalid. Force re-login on next call.
    global _session_ready
    _session_ready = False

    # Wait for Flask to recover (up to 5 min)
    backend_up = False
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        try:
            if _get("/api/hosts"):
                backend_up = True
                break
        except Exception:
            _session_ready = False  # retry login each attempt until it works
        time.sleep(5)

    assert backend_up, (
        "Flask in ct/300 did not recover within 5 minutes after self-restore. "
        "Check proxmox-backup-gui.service is enabled and auto-starts."
    )

    # Verify full functionality
    items_after = _items(host_id)
    assert "lxcs" in items_after or "vms" in items_after, \
        f"Backend recovered but /items structure unexpected: {items_after}"
