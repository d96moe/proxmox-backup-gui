#!/bin/bash
# =============================================================================
# add-user.sh
# Adds a user to the proxmox-backup-gui.
#
# Run on the PVE host as root (uses pct exec to run inside the GUI LXC).
#
# Usage:
#   bash add-user.sh <username> [admin|viewer]
#
# Examples:
#   bash add-user.sh alice           # viewer (read-only)
#   bash add-user.sh bob admin       # admin (full access)
#
# Override LXC ID (default 199):
#   LXC_ID=200 bash add-user.sh alice
# =============================================================================

set -euo pipefail

LXC_ID="${LXC_ID:-199}"
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

# ── Add user inside the LXC ───────────────────────────────────────────────────
pct exec "${LXC_ID}" -- bash -c "
    cd /opt/proxmox-backup-gui/backend
    /opt/proxmox-backup-gui/.venv/bin/python - <<'PYEOF'
import sys, os
sys.path.insert(0, '.')
from auth import add_user, get_user
username = os.environ['_USERNAME']
password = os.environ['_PASSWORD']
role     = os.environ['_ROLE']
if get_user(username):
    print(f\"Error: user '{username}' already exists\")
    sys.exit(1)
add_user(username, password, role)
print(f\"Created {role} user: {username}\")
PYEOF
" _USERNAME="${USERNAME}" _PASSWORD="${GUI_PASSWORD}" _ROLE="${ROLE}"
