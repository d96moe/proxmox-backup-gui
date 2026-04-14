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
BACKEND = Path(__file__).parent.parent.parent / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

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

    Runs once per test session, before any test. Because auth.USERS_FILE is
    read on every _load_users() call, patching the module-level variable here
    is sufficient — no restart of the app needed.
    """
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
    """Reset users.json to base state before every test."""
    _auth_files["users_file"].write_text(json.dumps(_BASE_USERS))
    yield


@pytest.fixture
def flask_client(_reset_users):
    """Authenticated Flask test client (logged in as admin).

    All existing tests keep working because this client has a valid session.
    Auth-specific tests that need an unauthenticated client create their own.
    """
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
