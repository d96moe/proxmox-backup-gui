#!/bin/bash
# =============================================================================
# setup-lxc.sh
# Creates and configures an LXC for the Proxmox Backup GUI
#
# Run on the PVE host as root.
# After running: edit /opt/proxmox-backup-gui/backend/hosts.json with
# your real credentials, then: systemctl restart proxmox-backup-gui
# =============================================================================

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
LXC_ID="${LXC_ID:-199}"
LXC_NAME="proxmox-backup-gui"
LXC_MEMORY=256          # MB
LXC_DISK="2"            # GB
LXC_STORAGE="local-lvm" # Storage pool for rootfs
LXC_BRIDGE="vmbr0"
LXC_IP="${LXC_IP:-dhcp}" # e.g. "192.168.0.199/24" or "dhcp"
LXC_GW="${LXC_GW:-}"     # Gateway, only needed for static IP
APP_PORT=5000

# Find debian-12 template — pick the newest one available
TEMPLATE=$(pveam list local 2>/dev/null | awk '/debian-12/ {print $1}' | sort -V | tail -1)
if [ -z "${TEMPLATE}" ]; then
    echo "No debian-12 template found. Downloading..."
    pveam update
    pveam download local debian-12-standard_12.7-1_amd64.tar.zst
    TEMPLATE="local:vztmpl/debian-12-standard_12.7-1_amd64.tar.zst"
fi

echo "=== Creating LXC ${LXC_ID} (${LXC_NAME}) ==="

# Build network arg
if [ "${LXC_IP}" = "dhcp" ]; then
    NET_ARG="name=eth0,bridge=${LXC_BRIDGE},ip=dhcp"
else
    NET_ARG="name=eth0,bridge=${LXC_BRIDGE},ip=${LXC_IP}"
    [ -n "${LXC_GW}" ] && NET_ARG="${NET_ARG},gw=${LXC_GW}"
fi

pct create "${LXC_ID}" "${TEMPLATE}" \
    --hostname "${LXC_NAME}" \
    --memory "${LXC_MEMORY}" \
    --rootfs "${LXC_STORAGE}:${LXC_DISK}" \
    --net0 "${NET_ARG}" \
    --unprivileged 1 \
    --features nesting=0 \
    --start 1 \
    --onboot 1

echo "--- Waiting for LXC to boot..."
sleep 5

# ── Install dependencies ───────────────────────────────────────────────────────
echo "=== Installing Python + dependencies ==="
pct exec "${LXC_ID}" -- bash -c "
    apt-get update -qq
    apt-get install -y -qq python3 python3-pip python3-venv restic rclone curl mosquitto 2>/dev/null
"

# ── Mosquitto (MQTT broker for browser WebSocket) ────────────────────────────
echo "=== Configuring Mosquitto MQTT broker ==="
pct exec "${LXC_ID}" -- bash -c "cat > /etc/mosquitto/conf.d/gui.conf << 'EOF'
# TCP listener for pve-agent publishers
listener 1883
protocol mqtt
allow_anonymous true

# WebSocket listener for browser (MQTT.js)
listener 9001
protocol websockets
allow_anonymous true
EOF
systemctl enable mosquitto
systemctl restart mosquitto
"

# ── Deploy app files ───────────────────────────────────────────────────────────
echo "=== Deploying application ==="
pct exec "${LXC_ID}" -- mkdir -p /opt/proxmox-backup-gui/backend /opt/proxmox-backup-gui/frontend

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for f in app.py config.py pbs_client.py pve_client.py restic_client.py agent_client.py jobs.py auth.py; do
    [ -f "${SCRIPT_DIR}/backend/${f}" ] && \
        pct push "${LXC_ID}" "${SCRIPT_DIR}/backend/${f}" "/opt/proxmox-backup-gui/backend/${f}"
done
pct push "${LXC_ID}" "${SCRIPT_DIR}/requirements.txt" "/opt/proxmox-backup-gui/requirements.txt"
pct push "${LXC_ID}" "${SCRIPT_DIR}/frontend/index.html" "/opt/proxmox-backup-gui/frontend/index.html"
pct push "${LXC_ID}" "${SCRIPT_DIR}/frontend/login.html" "/opt/proxmox-backup-gui/frontend/login.html"
pct push "${LXC_ID}" "${SCRIPT_DIR}/frontend/mqtt.min.js" "/opt/proxmox-backup-gui/frontend/mqtt.min.js"

# Copy example hosts.json — will be overwritten later if user provides host details
pct push "${LXC_ID}" "${SCRIPT_DIR}/backend/hosts.json.example" \
    "/opt/proxmox-backup-gui/backend/hosts.json"
_NEED_HOSTS_CONFIG=1

# ── Python venv + pip ─────────────────────────────────────────────────────────
echo "=== Installing Python packages ==="
pct exec "${LXC_ID}" -- bash -c "
    python3 -m venv /opt/proxmox-backup-gui/.venv
    /opt/proxmox-backup-gui/.venv/bin/pip install -q -r /opt/proxmox-backup-gui/requirements.txt
"

# ── Create initial admin user ─────────────────────────────────────────────────
echo "=== Creating initial admin user ==="
GUI_USER="${GUI_USER:-admin}"
if [ -z "${GUI_PASSWORD:-}" ]; then
    GUI_PASSWORD="$(tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 16)"
    _GENERATED_PW=1
fi
pct exec "${LXC_ID}" -- bash -c "
    cd /opt/proxmox-backup-gui/backend
    /opt/proxmox-backup-gui/.venv/bin/python - <<'PYEOF'
import sys
sys.path.insert(0, '.')
from auth import add_user, get_user
import os
username = os.environ.get('GUI_USER', 'admin')
password = os.environ.get('GUI_PASSWORD', '')
if get_user(username):
    print(f'User {username!r} already exists — skipping')
else:
    add_user(username, password, 'admin')
    print(f'Created admin user: {username}')
PYEOF
" GUI_USER="${GUI_USER}" GUI_PASSWORD="${GUI_PASSWORD}"

# ── systemd service ───────────────────────────────────────────────────────────
echo "=== Creating systemd service ==="
LXC_ADDR=$(pct exec "${LXC_ID}" -- hostname -I 2>/dev/null | awk '{print $1}' || echo "127.0.0.1")
pct exec "${LXC_ID}" -- bash -c "cat > /etc/systemd/system/proxmox-backup-gui.service << EOF
[Unit]
Description=Proxmox Backup GUI
After=network-online.target mosquitto.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/proxmox-backup-gui/backend
ExecStart=/opt/proxmox-backup-gui/.venv/bin/python app.py
Restart=on-failure
RestartSec=5
Environment=PORT=${APP_PORT}
Environment=MQTT_WS_URL=ws://${LXC_ADDR}:9001

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable proxmox-backup-gui
"

# ── Configure first host ──────────────────────────────────────────────────────
if [ "${_NEED_HOSTS_CONFIG:-0}" = "1" ]; then
    PVE_SELF_IP=$(ip -4 addr show scope global | awk '/inet/{print $2}' | cut -d/ -f1 | head -1)
    # Non-interactive mode: FIRST_HOST_ID="" skips the prompt entirely
    if [ "${FIRST_HOST_ID+set}" = "set" ]; then
        _HOST_ID="${FIRST_HOST_ID}"
    else
        echo ""
        echo "=== Configure first host ==="
        echo "  (press Enter to skip — edit hosts.json inside the LXC manually later)"
        echo ""
        read -rp "  Host ID (must match mqtt_hostname in agent config, e.g. 'home') [skip]: " _HOST_ID
    fi
    if [ -n "${_HOST_ID}" ]; then
        # Auto-detect agent token from local agent install
        _HOST_TOKEN=""
        if [ -f /etc/pve-agent/config.json ]; then
            _HOST_TOKEN=$(python3 -c \
                "import json; print(json.load(open('/etc/pve-agent/config.json')).get('agent_token',''))" \
                2>/dev/null || true)
            [ -n "${_HOST_TOKEN}" ] && echo "  Agent token auto-detected from /etc/pve-agent/config.json"
        fi
        if [ -z "${_HOST_TOKEN}" ]; then
            read -rp "  Agent token: " _HOST_TOKEN
        fi

        read -rp "  Agent URL [http://${PVE_SELF_IP}:8099]: " _HOST_URL_INPUT
        _HOST_URL="${_HOST_URL_INPUT:-http://${PVE_SELF_IP}:8099}"

        _HOSTS_TMP=$(mktemp /tmp/hosts.json.XXXXXX)
        HOST_ID="${_HOST_ID}" HOST_URL="${_HOST_URL}" HOST_TOKEN="${_HOST_TOKEN}" \
        python3 -c "
import json, os
print(json.dumps([{
    'id': os.environ['HOST_ID'],
    'label': 'pve - ' + os.environ['HOST_ID'],
    'agent_url': os.environ['HOST_URL'],
    'agent_token': os.environ['HOST_TOKEN'],
}], indent=2))
" > "${_HOSTS_TMP}"
        pct push "${LXC_ID}" "${_HOSTS_TMP}" "/opt/proxmox-backup-gui/backend/hosts.json"
        rm -f "${_HOSTS_TMP}"
        echo "  hosts.json written — LXC is ready to use after starting the agent."
    else
        echo "  Skipped — edit hosts.json manually before use:"
        echo "    pct enter ${LXC_ID}"
        echo "    nano /opt/proxmox-backup-gui/backend/hosts.json"
    fi
fi

# ── Done ──────────────────────────────────────────────────────────────────────
LXC_ADDR=$(pct exec "${LXC_ID}" -- hostname -I 2>/dev/null | awk '{print $1}' || echo "?")

echo ""
echo "============================================================"
echo "  Setup complete"
echo "============================================================"
echo ""
echo "  GUI URL:  http://${LXC_ADDR}:${APP_PORT}"
echo "  Username: ${GUI_USER}"
if [ -n "${_GENERATED_PW:-}" ]; then
    echo "  Password: ${GUI_PASSWORD}  ← SAVE THIS"
else
    echo "  Password: (as provided)"
fi
echo ""
echo "  Start the GUI:   systemctl start proxmox-backup-gui"
echo "  Logs:            pct enter ${LXC_ID} && journalctl -u proxmox-backup-gui -f"
echo ""
echo "  Next: run setup-agent.sh on each PVE host you want to monitor."
echo "============================================================"
