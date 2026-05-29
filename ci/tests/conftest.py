"""Shared pytest fixtures for all test modules."""
import json
import sys
from pathlib import Path

import pytest
from werkzeug.security import generate_password_hash

try:
    from playwright.sync_api import sync_playwright
    _playwright_available = True
except ImportError:
    _playwright_available = False

# ── make backend importable ──────────────────────────────────────────────────
# In dev: conftest lives at ci/tests/conftest.py → parent.parent.parent = repo root
# In CI deploy: conftest lives at /opt/ci/proxmox-backup-gui/tests/conftest.py
#   → parent.parent.parent = /opt/ci, but backend is at /opt/proxmox-backup-gui/backend/
# Fall back to known CI path if calculated path doesn't exist.
BACKEND = Path(__file__).parent.parent.parent / "backend"
if not BACKEND.is_dir():
    BACKEND = Path("/opt/proxmox-backup-gui/backend")
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

# Check whether auth (and its deps: flask-login etc.) are actually importable.
# In test_frontend.py (mock server), the backend is not needed at all.
# In integration CI the backend runs in LXC 300 and the test process on the
# PVE host can't import it — auth fixtures become no-ops in that case.
_BACKEND_IMPORTABLE = False
try:
    import auth as _auth_probe  # noqa: F401
    _BACKEND_IMPORTABLE = True
except ImportError:
    pass

# ── auth test user constants ─────────────────────────────────────────────────
ADMIN_USER = "admin"
ADMIN_PW   = "testpass123"
VIEWER_USER = "viewer"
VIEWER_PW   = "viewpass123"

_BASE_USERS = [
    {"username": ADMIN_USER,  "password_hash": generate_password_hash(ADMIN_PW),  "role": "admin"},
    {"username": VIEWER_USER, "password_hash": generate_password_hash(VIEWER_PW), "role": "viewer"},
]


@pytest.fixture(scope="session")
def _auth_files(tmp_path_factory):
    """Session-scoped: patch auth module to use temp users.json + .secret_key.

    No-op in integration CI (backend not importable from test process).
    Runs once per test session in unit/mock mode, before any test. Because
    auth.USERS_FILE is read on every _load_users() call, patching the
    module-level variable here is sufficient — no restart of the app needed.
    """
    if not _BACKEND_IMPORTABLE:
        yield {}
        return

    import auth
    tmp = tmp_path_factory.mktemp("auth_files")

    users_file  = tmp / "users.json"
    secret_file = tmp / ".secret_key"
    secret_file.write_text("pytest-secret-key-not-for-production")

    users_file.write_text(json.dumps(_BASE_USERS))

    orig_uf = auth.USERS_FILE
    orig_sk = auth.SECRET_KEY_FILE
    auth.USERS_FILE    = users_file
    auth.SECRET_KEY_FILE = secret_file

    yield {"users_file": users_file, "secret_file": secret_file}

    auth.USERS_FILE    = orig_uf
    auth.SECRET_KEY_FILE = orig_sk


@pytest.fixture(autouse=True)
def _reset_users(_auth_files):
    """Reset users.json to base state before every test.

    No-op in integration CI (backend not importable from test process).
    """
    if _BACKEND_IMPORTABLE and _auth_files:
        _auth_files["users_file"].write_text(json.dumps(_BASE_USERS))
    yield


@pytest.fixture(autouse=True)
def _reset_mqtt_cache():
    """Clear MQTT_CACHE before every test to ensure complete test isolation."""
    if _BACKEND_IMPORTABLE:
        from mqtt_manager import MQTT_CACHE, MQTT_CACHE_LOCK
        with MQTT_CACHE_LOCK:
            MQTT_CACHE.clear()
    yield



@pytest.fixture(autouse=True)
def _mock_mqtt_publish():
    """Mock publish_cmd dynamically in unit tests to simulate a running agent over MQTT."""
    if not _BACKEND_IMPORTABLE:
        yield
        return

    from unittest.mock import patch
    import json
    import uuid

    def mock_publish_cmd(topic: str, payload: str | dict):
        from mqtt_manager import MQTT_CACHE, MQTT_CACHE_LOCK
        
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                pass
                
        if not isinstance(payload, dict):
            payload = {}
            
        corr_id = payload.get("corr_id")
        if not corr_id:
            return
            
        op_id = f"op_{corr_id[:8]}"
        base_topic = "/".join(topic.split("/")[:2]) # "proxmox/testhost"
        action = topic.split("/")[-1] # "backup", "restore", etc.
        
        # Generate appropriate logs based on action
        logs = []
        if action == "backup":
            logs = [
                "Step 1/2 — Triggering PBS backup...",
                "Step 2/2 — waiting for completion..."
            ]
        elif action == "backup-restic":
            logs = [
                "Step 1/2 — Triggering cloud backup...",
                "Step 2/2 — waiting for completion..."
            ]
        elif action == "restore":
            source = payload.get("source", "local")
            if source == "cloud":
                logs = [
                    "Step 1/3 — Downloading from cloud...",
                    "Step 2/3 — Restoring VM/LXC from PBS...",
                    "Step 3/3 — Cleanup..."
                ]
            else:
                logs = [
                    "Step 1/2 — Restoring VM/LXC from local PBS...",
                    "Step 2/2 — waiting for completion..."
                ]
        elif action == "delete":
            scope = payload.get("scope", "pbs")
            if scope == "cloud":
                logs = [
                    "Step 1/4 — Finding snapshots...",
                    "Step 2/4 — Forgetting restic snapshots...",
                    "Step 3/4 — Running restic prune...",
                    "Step 4/4 — Removing deleted pbs_time tags..."
                ]
            else:
                logs = [
                    "Step 1/2 — Deleting local PBS snapshot...",
                    "Step 2/2 — waiting for completion..."
                ]
        elif action == "delete-all":
            logs = [
                "Step 1/2 — Deleting all local PBS snapshots...",
                "Step 2/2 — waiting for completion..."
            ]
        elif action == "delete-restic":
            logs = [
                "Step 1/2 — Deleting restic snapshot...",
                "Step 2/2 — waiting for completion..."
            ]
        elif action == "restore-datastore":
            logs = [
                "Step 1/2 — Restoring full datastore...",
                "Step 2/2 — waiting for completion..."
            ]
        elif action == "restic-prune":
            logs = [
                "Step 1/2 — Pruning restic repository...",
                "Step 2/2 — waiting for completion..."
            ]
            
        with MQTT_CACHE_LOCK:
            MQTT_CACHE[f"{base_topic}/job/{corr_id}/ack"] = {"op_id": op_id}
            MQTT_CACHE[f"{base_topic}/ops/{op_id}/status"] = "ok"
            MQTT_CACHE[f"{base_topic}/ops/{op_id}/log"] = logs

    with patch("mqtt_manager.publish_cmd", side_effect=mock_publish_cmd), \
         patch("agent_client.publish_cmd", side_effect=mock_publish_cmd):
        yield


@pytest.fixture
def flask_client(_reset_users):
    """Authenticated Flask test client (logged in as admin).

    All existing tests keep working because this client has a valid session.
    Auth-specific tests that need an unauthenticated client create their own.
    Skipped in integration CI (backend not importable from test process).
    """
    if not _BACKEND_IMPORTABLE:
        pytest.skip("backend not importable — integration CI uses real backend")
    import app as _app
    _app._cache.clear()
    with _app.app.test_client() as c:
        res = c.post("/api/auth/login",
                     json={"username": ADMIN_USER, "password": ADMIN_PW},
                     content_type="application/json")
        assert res.status_code == 200, f"Test login failed: {res.data}"
        yield c
    _app._cache.clear()


@pytest.fixture(scope="session")
def browser():
    """Session-scoped Chromium browser — shared by test_frontend and test_restore.
    A single sync_playwright() instance avoids the 'Sync API inside asyncio loop'
    error that occurs when a second instance is created while the first is active."""
    if not _playwright_available:
        pytest.skip("playwright not installed")
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        yield b
        b.close()
