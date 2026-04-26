#!/bin/bash
# =============================================================================
# add-user.sh
# Adds a user to the proxmox-backup-gui.
#
# Run wherever the GUI is installed (LXC, VM, bare-metal — anywhere).
#
# Usage:
#   bash add-user.sh <username> [admin|viewer]
#
# Examples:
#   bash add-user.sh alice           # viewer (read-only)
#   bash add-user.sh bob admin       # admin (full access)
#
# Override install directory (default: /opt/proxmox-backup-gui):
#   APP_DIR=/srv/proxmox-backup-gui bash add-user.sh alice
# =============================================================================

set -euo pipefail

APP_DIR="${APP_DIR:-/opt/proxmox-backup-gui}"
USERNAME="${1:-}"
ROLE="${2:-viewer}"

# ── Validate args ──────────────────────────────────────────────────────────────
if [ -z "${USERNAME}" ]; then
    echo "Usage: bash add-user.sh <username> [admin|viewer]"
    exit 1
fi

if [ "${ROLE}" != "admin" ] && [ "${ROLE}" != "viewer" ]; then
    echo "Error: role must be 'admin' or 'viewer', got '${ROLE}'"
    exit 1
fi

if [ ! -d "${APP_DIR}/backend" ]; then
    echo "Error: GUI not found at ${APP_DIR} — set APP_DIR to the install directory"
    exit 1
fi

# ── Read password securely ─────────────────────────────────────────────────────
if [ -z "${GUI_PASSWORD:-}" ]; then
    read -rsp "Password for ${USERNAME}: " GUI_PASSWORD
    echo
    read -rsp "Confirm password: " GUI_PASSWORD2
    echo
    if [ "${GUI_PASSWORD}" != "${GUI_PASSWORD2}" ]; then
        echo "Error: passwords do not match"
        exit 1
    fi
fi

# ── Add user ───────────────────────────────────────────────────────────────────
cd "${APP_DIR}/backend"
_USERNAME="${USERNAME}" _PASSWORD="${GUI_PASSWORD}" _ROLE="${ROLE}" \
"${APP_DIR}/.venv/bin/python" - <<'PYEOF'
import sys, os
sys.path.insert(0, '.')
from auth import add_user, get_user
username = os.environ['_USERNAME']
password = os.environ['_PASSWORD']
role     = os.environ['_ROLE']
if get_user(username):
    print(f"Error: user '{username}' already exists")
    sys.exit(1)
add_user(username, password, role)
print(f"Created {role} user: {username}")
PYEOF
