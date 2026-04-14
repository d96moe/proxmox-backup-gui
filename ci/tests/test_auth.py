"""Unit tests for authentication — auth.py module + Flask auth endpoints.

AUTH_MODULE    — users.json CRUD, password hashing, secret key lifecycle
AUTH_ENDPOINTS — /api/auth/login, logout, me, change-password
USER_MGMT      — admin: list/add/delete users; viewer: 403
ROLE_GUARD     — @admin_required blocks viewer on every mutating endpoint
UNAUTH_GUARD   — @login_required returns 401 JSON for /api/* routes
PAGE_REDIRECT  — unauthenticated browser requests → redirect to /login
HA_UNAUTH      — /api/host/<id>/ha/sensors stays open (HA integration)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── backend on sys.path (conftest already adds it, but be explicit) ──────────
BACKEND = Path(__file__).parent.parent.parent / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import app as _app
import auth as _auth

from conftest import ADMIN_USER, ADMIN_PW, VIEWER_USER, VIEWER_PW


# ─────────────────────────────────────────────────────────────────────────────
# Local fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def unauthed_client(_reset_users):
    """Test client with no session — simulates a browser that hasn't logged in."""
    _app._cache.clear()
    with _app.app.test_client() as c:
        yield c
    _app._cache.clear()


@pytest.fixture
def viewer_client(_reset_users):
    """Test client logged in as the viewer role user."""
    _app._cache.clear()
    with _app.app.test_client() as c:
        res = c.post("/api/auth/login",
                     json={"username": VIEWER_USER, "password": VIEWER_PW},
                     content_type="application/json")
        assert res.status_code == 200, f"Viewer login failed: {res.data}"
        yield c
    _app._cache.clear()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _json(resp) -> dict:
    return json.loads(resp.data)


# ─────────────────────────────────────────────────────────────────────────────
# AUTH_MODULE — auth.py functions directly
# ─────────────────────────────────────────────────────────────────────────────

class TestAuthModule:

    def test_add_and_get_user(self):
        _auth.add_user("newuser", "password123", "viewer")
        u = _auth.get_user("newuser")
        assert u is not None
        assert u.username == "newuser"
        assert u.role == "viewer"

    def test_add_user_default_role_is_viewer(self):
        _auth.add_user("newuser2", "password123")
        u = _auth.get_user("newuser2")
        assert u.role == "viewer"

    def test_add_duplicate_raises(self):
        with pytest.raises(ValueError, match="already exists"):
            _auth.add_user(ADMIN_USER, "anything", "admin")

    def test_check_password_correct(self):
        u = _auth.get_user(ADMIN_USER)
        assert u.check_password(ADMIN_PW) is True

    def test_check_password_wrong(self):
        u = _auth.get_user(ADMIN_USER)
        assert u.check_password("wrongpassword") is False

    def test_is_admin_true_for_admin(self):
        u = _auth.get_user(ADMIN_USER)
        assert u.is_admin is True

    def test_is_admin_false_for_viewer(self):
        u = _auth.get_user(VIEWER_USER)
        assert u.is_admin is False

    def test_get_user_nonexistent_returns_none(self):
        assert _auth.get_user("nobody") is None

    def test_get_all_users(self):
        users = _auth.get_all_users()
        usernames = {u.username for u in users}
        assert ADMIN_USER in usernames
        assert VIEWER_USER in usernames

    def test_remove_user_returns_true(self):
        assert _auth.remove_user(VIEWER_USER) is True
        assert _auth.get_user(VIEWER_USER) is None

    def test_remove_nonexistent_returns_false(self):
        assert _auth.remove_user("nobody") is False

    def test_update_password(self):
        _auth.update_password(ADMIN_USER, "newpass456")
        u = _auth.get_user(ADMIN_USER)
        assert u.check_password("newpass456") is True
        assert u.check_password(ADMIN_PW) is False

    def test_update_password_nonexistent_raises(self):
        with pytest.raises(KeyError):
            _auth.update_password("nobody", "anything")

    def test_secret_key_created_on_first_call(self, tmp_path):
        sk_file = tmp_path / ".secret_key"
        orig = _auth.SECRET_KEY_FILE
        _auth.SECRET_KEY_FILE = sk_file
        try:
            key = _auth.get_or_create_secret_key()
            assert sk_file.exists()
            assert len(key) == 64       # 32 bytes hex = 64 chars
            assert key == sk_file.read_text().strip()
        finally:
            _auth.SECRET_KEY_FILE = orig

    def test_secret_key_persists_across_calls(self, tmp_path):
        sk_file = tmp_path / ".secret_key"
        orig = _auth.SECRET_KEY_FILE
        _auth.SECRET_KEY_FILE = sk_file
        try:
            key1 = _auth.get_or_create_secret_key()
            key2 = _auth.get_or_create_secret_key()
            assert key1 == key2
        finally:
            _auth.SECRET_KEY_FILE = orig

    def test_user_id_equals_username(self):
        """Flask-Login requires .id; it must equal username for user_loader."""
        u = _auth.get_user(ADMIN_USER)
        assert u.id == u.username


# ─────────────────────────────────────────────────────────────────────────────
# AUTH_ENDPOINTS — login / logout / me / change-password
# ─────────────────────────────────────────────────────────────────────────────

class TestAuthEndpoints:

    def test_login_success_returns_user_info(self, unauthed_client):
        res = unauthed_client.post("/api/auth/login",
                                   json={"username": ADMIN_USER, "password": ADMIN_PW})
        assert res.status_code == 200
        data = _json(res)
        assert data["username"] == ADMIN_USER
        assert data["role"] == "admin"

    def test_login_wrong_password_returns_401(self, unauthed_client):
        res = unauthed_client.post("/api/auth/login",
                                   json={"username": ADMIN_USER, "password": "wrongpass"})
        assert res.status_code == 401
        assert "error" in _json(res)

    def test_login_unknown_user_returns_401(self, unauthed_client):
        res = unauthed_client.post("/api/auth/login",
                                   json={"username": "nobody", "password": "anything"})
        assert res.status_code == 401

    def test_login_empty_body_returns_401(self, unauthed_client):
        res = unauthed_client.post("/api/auth/login", json={})
        assert res.status_code == 401

    def test_me_returns_current_user(self, flask_client):
        res = flask_client.get("/api/auth/me")
        assert res.status_code == 200
        data = _json(res)
        assert data["username"] == ADMIN_USER
        assert data["role"] == "admin"

    def test_me_unauthenticated_returns_401(self, unauthed_client):
        res = unauthed_client.get("/api/auth/me")
        assert res.status_code == 401

    def test_logout_clears_session(self, flask_client):
        # Verify we're logged in
        assert flask_client.get("/api/auth/me").status_code == 200
        # Logout
        res = flask_client.post("/api/auth/logout")
        assert res.status_code == 200
        assert _json(res)["ok"] is True
        # Now /me should return 401
        assert flask_client.get("/api/auth/me").status_code == 401

    def test_logout_requires_login(self, unauthed_client):
        res = unauthed_client.post("/api/auth/logout")
        assert res.status_code == 401

    def test_change_password_success(self, flask_client):
        res = flask_client.post("/api/auth/change-password",
                                json={"current_password": ADMIN_PW,
                                      "new_password": "newpassword99"})
        assert res.status_code == 200
        assert _json(res)["ok"] is True
        # Verify new password works
        u = _auth.get_user(ADMIN_USER)
        assert u.check_password("newpassword99") is True

    def test_change_password_wrong_current(self, flask_client):
        res = flask_client.post("/api/auth/change-password",
                                json={"current_password": "wrongcurrent",
                                      "new_password": "newpassword99"})
        assert res.status_code == 400
        assert "error" in _json(res)

    def test_change_password_too_short(self, flask_client):
        res = flask_client.post("/api/auth/change-password",
                                json={"current_password": ADMIN_PW,
                                      "new_password": "short"})
        assert res.status_code == 400
        assert "8 characters" in _json(res)["error"]

    def test_change_password_requires_login(self, unauthed_client):
        res = unauthed_client.post("/api/auth/change-password",
                                   json={"current_password": "x", "new_password": "newpassword99"})
        assert res.status_code == 401

    def test_viewer_can_change_own_password(self, viewer_client):
        res = viewer_client.post("/api/auth/change-password",
                                 json={"current_password": VIEWER_PW,
                                       "new_password": "newviewerpass"})
        assert res.status_code == 200


# ─────────────────────────────────────────────────────────────────────────────
# USER_MGMT — /api/auth/users (admin CRUD)
# ─────────────────────────────────────────────────────────────────────────────

class TestUserManagement:

    def test_list_users_as_admin(self, flask_client):
        res = flask_client.get("/api/auth/users")
        assert res.status_code == 200
        users = _json(res)
        assert isinstance(users, list)
        usernames = {u["username"] for u in users}
        assert ADMIN_USER in usernames
        assert VIEWER_USER in usernames

    def test_list_users_as_viewer_returns_403(self, viewer_client):
        res = viewer_client.get("/api/auth/users")
        assert res.status_code == 403

    def test_list_users_unauthenticated_returns_401(self, unauthed_client):
        res = unauthed_client.get("/api/auth/users")
        assert res.status_code == 401

    def test_add_user_as_admin(self, flask_client):
        res = flask_client.post("/api/auth/users",
                                json={"username": "newuser", "password": "securepass1",
                                      "role": "viewer"})
        assert res.status_code == 201
        assert _auth.get_user("newuser") is not None

    def test_add_admin_user(self, flask_client):
        res = flask_client.post("/api/auth/users",
                                json={"username": "newadmin", "password": "securepass1",
                                      "role": "admin"})
        assert res.status_code == 201
        u = _auth.get_user("newadmin")
        assert u.is_admin is True

    def test_add_user_duplicate_returns_409(self, flask_client):
        res = flask_client.post("/api/auth/users",
                                json={"username": ADMIN_USER, "password": "anything",
                                      "role": "viewer"})
        assert res.status_code == 409

    def test_add_user_invalid_role_returns_400(self, flask_client):
        res = flask_client.post("/api/auth/users",
                                json={"username": "x", "password": "anything",
                                      "role": "superuser"})
        assert res.status_code == 400

    def test_add_user_missing_password_returns_400(self, flask_client):
        res = flask_client.post("/api/auth/users",
                                json={"username": "x", "role": "viewer"})
        assert res.status_code == 400

    def test_add_user_missing_username_returns_400(self, flask_client):
        res = flask_client.post("/api/auth/users",
                                json={"password": "securepass1", "role": "viewer"})
        assert res.status_code == 400

    def test_add_user_as_viewer_returns_403(self, viewer_client):
        res = viewer_client.post("/api/auth/users",
                                 json={"username": "x", "password": "x", "role": "viewer"})
        assert res.status_code == 403

    def test_delete_user_as_admin(self, flask_client):
        res = flask_client.delete(f"/api/auth/users/{VIEWER_USER}")
        assert res.status_code == 200
        assert _auth.get_user(VIEWER_USER) is None

    def test_delete_self_returns_400(self, flask_client):
        res = flask_client.delete(f"/api/auth/users/{ADMIN_USER}")
        assert res.status_code == 400
        assert "own account" in _json(res)["error"]

    def test_delete_nonexistent_returns_404(self, flask_client):
        res = flask_client.delete("/api/auth/users/nobody")
        assert res.status_code == 404

    def test_delete_user_as_viewer_returns_403(self, viewer_client):
        res = viewer_client.delete(f"/api/auth/users/{ADMIN_USER}")
        assert res.status_code == 403


# ─────────────────────────────────────────────────────────────────────────────
# ROLE_GUARD — @admin_required blocks viewer on mutating endpoints
# ─────────────────────────────────────────────────────────────────────────────

HOST_ID = "testhost"

def _mock_host():
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


class TestRoleGuards:
    """Viewer gets 403 on every endpoint decorated with @admin_required."""

    def _with_host(self):
        return patch.dict(_app.HOSTS, {HOST_ID: _mock_host()}, clear=True)

    def test_backup_pbs_viewer_forbidden(self, viewer_client):
        with self._with_host():
            res = viewer_client.post(f"/api/host/{HOST_ID}/backup/pbs",
                                     json={"vmid": 100, "type": "ct"})
        assert res.status_code == 403

    def test_backup_restic_viewer_forbidden(self, viewer_client):
        with self._with_host():
            res = viewer_client.post(f"/api/host/{HOST_ID}/backup/restic")
        assert res.status_code == 403

    def test_restore_viewer_forbidden(self, viewer_client):
        with self._with_host():
            res = viewer_client.post(f"/api/host/{HOST_ID}/restore",
                                     json={"vmid": 100, "type": "ct",
                                           "source": "local", "backup_time": 1000})
        assert res.status_code == 403

    def test_delete_pbs_viewer_forbidden(self, viewer_client):
        with self._with_host():
            res = viewer_client.post(f"/api/host/{HOST_ID}/delete/pbs",
                                     json={"vmid": 100, "type": "ct", "backup_time": 1000})
        assert res.status_code == 403

    def test_delete_pbs_all_viewer_forbidden(self, viewer_client):
        with self._with_host():
            res = viewer_client.post(f"/api/host/{HOST_ID}/delete/pbs/all",
                                     json={"vmid": 100, "type": "ct"})
        assert res.status_code == 403

    def test_delete_cloud_viewer_forbidden(self, viewer_client):
        with self._with_host():
            res = viewer_client.post(f"/api/host/{HOST_ID}/delete/cloud",
                                     json={"vmid": 100, "type": "ct",
                                           "backup_time": 1000, "restic_id": "abc"})
        assert res.status_code == 403

    def test_delete_both_viewer_forbidden(self, viewer_client):
        with self._with_host():
            res = viewer_client.post(f"/api/host/{HOST_ID}/delete/both",
                                     json={"vmid": 100, "type": "ct",
                                           "backup_time": 1000, "restic_id": "abc"})
        assert res.status_code == 403

    def test_delete_restic_viewer_forbidden(self, viewer_client):
        with self._with_host():
            res = viewer_client.post(f"/api/host/{HOST_ID}/delete/restic",
                                     json={"restic_id": "abc123"})
        assert res.status_code == 403

    def test_restore_datastore_viewer_forbidden(self, viewer_client):
        with self._with_host():
            res = viewer_client.post(f"/api/host/{HOST_ID}/restore/datastore",
                                     json={"restic_id": "abc123"})
        assert res.status_code == 403

    def test_read_endpoints_allowed_for_viewer(self, viewer_client):
        """Viewer can reach GET endpoints (read-only access)."""
        with self._with_host(), \
             patch("app.PBSClient") as pbs_cls, \
             patch("app.PVEClient") as pve_cls, \
             patch("app.ResticClient") as res_cls:
            pbs_cls.return_value.get_snapshots.return_value = []
            pbs_cls.return_value.get_storage_info.return_value = {
                "local_used": 0, "local_total": 10, "dedup_factor": None}
            pve_cls.return_value.get_vms_and_lxcs.return_value = {}
            res_cls.return_value.is_running.return_value = False
            res_cls.return_value.get_snapshots_by_vm.return_value = ({}, [])
            _app._cache.clear()
            res = viewer_client.get(f"/api/host/{HOST_ID}/items")
        assert res.status_code == 200


# ─────────────────────────────────────────────────────────────────────────────
# UNAUTH_GUARD — @login_required returns 401 JSON for /api/* routes
# ─────────────────────────────────────────────────────────────────────────────

class TestUnauthGuard:
    """Unauthenticated requests to /api/* get 401 JSON, not a redirect."""

    def _with_host(self):
        return patch.dict(_app.HOSTS, {HOST_ID: _mock_host()}, clear=True)

    def test_api_hosts_returns_401_json(self, unauthed_client):
        res = unauthed_client.get("/api/hosts")
        assert res.status_code == 401
        assert b"Unauthorized" in res.data

    def test_api_items_returns_401_json(self, unauthed_client):
        with self._with_host():
            res = unauthed_client.get(f"/api/host/{HOST_ID}/items")
        assert res.status_code == 401
        assert b"Unauthorized" in res.data

    def test_api_storage_returns_401_json(self, unauthed_client):
        with self._with_host():
            res = unauthed_client.get(f"/api/host/{HOST_ID}/storage")
        assert res.status_code == 401

    def test_api_jobs_active_returns_401_json(self, unauthed_client):
        res = unauthed_client.get("/api/jobs/active")
        assert res.status_code == 401

    def test_api_schedules_returns_401_json(self, unauthed_client):
        with self._with_host():
            res = unauthed_client.get(f"/api/host/{HOST_ID}/schedules")
        assert res.status_code == 401

    def test_root_page_redirects_to_login(self, unauthed_client):
        """Browser requests to / should redirect to /login, not get 401 JSON."""
        res = unauthed_client.get("/")
        assert res.status_code == 302
        assert "/login" in res.headers["Location"]

    def test_login_page_accessible_without_auth(self, unauthed_client):
        """GET /login must not require authentication."""
        with patch("app.send_from_directory", return_value=("login", 200)):
            res = unauthed_client.get("/login")
        assert res.status_code == 200

    def test_login_page_redirects_if_already_authed(self, flask_client):
        """If already logged in, GET /login should redirect to /."""
        res = flask_client.get("/login")
        assert res.status_code == 302
        assert res.headers["Location"] in ("/", "http://localhost/")


# ─────────────────────────────────────────────────────────────────────────────
# HA_UNAUTH — /api/host/<id>/ha/sensors must work without login (Home Assistant)
# ─────────────────────────────────────────────────────────────────────────────

class TestHASensorsUnauthenticated:

    def test_ha_sensors_accessible_without_login(self, unauthed_client):
        host = _mock_host()
        mock_pbs  = MagicMock()
        mock_pve  = MagicMock()
        mock_pbs.get_snapshots.return_value = []
        mock_pbs.get_storage_info.return_value = {
            "local_used": 1.0, "local_total": 10.0, "dedup_factor": None}
        mock_pve.get_vms_and_lxcs.return_value = {}
        _app._cache.clear()
        with patch.dict(_app.HOSTS, {HOST_ID: host}, clear=True), \
             patch("app.PBSClient", return_value=mock_pbs), \
             patch("app.PVEClient", return_value=mock_pve):
            res = unauthed_client.get(f"/api/host/{HOST_ID}/ha/sensors")
        assert res.status_code == 200
        data = _json(res)
        assert "storage_local_used_pct" in data

    def test_ha_sensors_no_session_cookie_needed(self, _reset_users):
        """No cookies at all — fresh client with no prior session still works."""
        host = _mock_host()
        host.restic_repo = None  # skip restic to keep mock simple
        mock_pbs = MagicMock()
        mock_pve = MagicMock()
        mock_pbs.get_snapshots.return_value = []
        mock_pbs.get_storage_info.return_value = {
            "local_used": 0.5, "local_total": 5.0, "dedup_factor": None}
        mock_pve.get_vms_and_lxcs.return_value = {}
        _app._cache.clear()
        # Deliberately use a brand-new test client so there's no session cookie
        with patch.dict(_app.HOSTS, {HOST_ID: host}, clear=True), \
             patch("app.PBSClient", return_value=mock_pbs), \
             patch("app.PVEClient", return_value=mock_pve), \
             _app.app.test_client() as fresh:
            res = fresh.get(f"/api/host/{HOST_ID}/ha/sensors")
        assert res.status_code == 200
