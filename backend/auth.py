"""Authentication module — Flask-Login + werkzeug password hashing.

Users are stored in users.json next to hosts.json:
  [{"username": "admin", "password_hash": "...", "role": "admin"}]

Roles:
  "admin"  — full access (all GET + POST/DELETE operations)
  "viewer" — read-only (GET endpoints only)
"""
from __future__ import annotations

import json
import secrets
from functools import wraps
from pathlib import Path

from flask import abort, jsonify, redirect, request
from flask_login import LoginManager, UserMixin, current_user
from werkzeug.security import check_password_hash, generate_password_hash

USERS_FILE = Path(__file__).parent / "users.json"
SECRET_KEY_FILE = Path(__file__).parent / ".secret_key"


class User(UserMixin):
    def __init__(self, username: str, password_hash: str, role: str) -> None:
        self.id = username
        self.username = username
        self.password_hash = password_hash
        self.role = role

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


# ── users.json CRUD ───────────────────────────────────────────────────────────

def _load_users() -> dict[str, User]:
    if not USERS_FILE.exists():
        return {}
    data = json.loads(USERS_FILE.read_text())
    return {u["username"]: User(**u) for u in data}


def _save_users(users: dict[str, User]) -> None:
    data = [
        {"username": u.username, "password_hash": u.password_hash, "role": u.role}
        for u in users.values()
    ]
    USERS_FILE.write_text(json.dumps(data, indent=2))


def get_user(username: str) -> User | None:
    return _load_users().get(username)


def get_all_users() -> list[User]:
    return list(_load_users().values())


def add_user(username: str, password: str, role: str = "viewer") -> None:
    users = _load_users()
    if username in users:
        raise ValueError(f"User '{username}' already exists")
    users[username] = User(
        username=username,
        password_hash=generate_password_hash(password),
        role=role,
    )
    _save_users(users)


def remove_user(username: str) -> bool:
    users = _load_users()
    if username not in users:
        return False
    del users[username]
    _save_users(users)
    return True


def update_password(username: str, new_password: str) -> None:
    users = _load_users()
    if username not in users:
        raise KeyError(username)
    users[username].password_hash = generate_password_hash(new_password)
    _save_users(users)


# ── Secret key ────────────────────────────────────────────────────────────────

def get_or_create_secret_key() -> str:
    """Return persisted secret key, generating one on first run."""
    if SECRET_KEY_FILE.exists():
        return SECRET_KEY_FILE.read_text().strip()
    key = secrets.token_hex(32)
    SECRET_KEY_FILE.write_text(key)
    SECRET_KEY_FILE.chmod(0o600)
    return key


# ── Flask-Login setup ─────────────────────────────────────────────────────────

def init_login_manager(app) -> LoginManager:
    lm = LoginManager(app)
    lm.login_view = "login_page"
    lm.login_message = ""

    @lm.user_loader
    def load_user(user_id: str) -> User | None:
        return get_user(user_id)

    @lm.unauthorized_handler
    def unauthorized():
        if request.path.startswith("/api/"):
            return jsonify({"error": "Unauthorized"}), 401
        next_path = request.path
        if request.query_string:
            next_path += "?" + request.query_string.decode()
        return redirect(f"/login?next={next_path}")

    return lm


# ── Decorators ────────────────────────────────────────────────────────────────

def admin_required(f):
    """Require role=='admin'. Must be applied after @login_required."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_admin:
            if request.path.startswith("/api/"):
                return jsonify({"error": "Forbidden — admin required"}), 403
            abort(403)
        return f(*args, **kwargs)
    return decorated
