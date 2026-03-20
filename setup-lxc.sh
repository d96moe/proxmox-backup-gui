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
    apt-get install -y -qq python3 python3-pip python3-venv restic rclone curl 2>/dev/null
"

# ── Deploy app files ───────────────────────────────────────────────────────────
echo "=== Deploying application ==="
pct exec "${LXC_ID}" -- mkdir -p /opt/proxmox-backup-gui/backend /opt/proxmox-backup-gui/frontend

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for f in app.py config.py pbs_client.py pve_client.py restic_client.py; do
    pct push "${LXC_ID}" "${SCRIPT_DIR}/backend/${f}" "/opt/proxmox-backup-gui/backend/${f}"
done
pct push "${LXC_ID}" "${SCRIPT_DIR}/requirements.txt" "/opt/proxmox-backup-gui/requirements.txt"
pct push "${LXC_ID}" "${SCRIPT_DIR}/frontend/index.html" "/opt/proxmox-backup-gui/frontend/index.html"

# Copy example hosts.json if no real one exists yet
if [ ! -f "${SCRIPT_DIR}/backend/hosts.json" ]; then
    pct push "${LXC_ID}" "${SCRIPT_DIR}/backend/hosts.json.example" \
        "/opt/proxmox-backup-gui/backend/hosts.json"
    echo ""
    echo "  !! Edit /opt/proxmox-backup-gui/backend/hosts.json inside the LXC"
    echo "     with your real credentials before starting the service."
    echo ""
else
    pct push "${LXC_ID}" "${SCRIPT_DIR}/backend/hosts.json" \
        "/opt/proxmox-backup-gui/backend/hosts.json"
fi

# ── Python venv + pip ─────────────────────────────────────────────────────────
echo "=== Installing Python packages ==="
pct exec "${LXC_ID}" -- bash -c "
    python3 -m venv /opt/proxmox-backup-gui/.venv
    /opt/proxmox-backup-gui/.venv/bin/pip install -q -r /opt/proxmox-backup-gui/requirements.txt
"

# ── systemd service ───────────────────────────────────────────────────────────
echo "=== Creating systemd service ==="
pct exec "${LXC_ID}" -- bash -c "cat > /etc/systemd/system/proxmox-backup-gui.service << 'EOF'
[Unit]
Description=Proxmox Backup GUI
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/proxmox-backup-gui/backend
ExecStart=/opt/proxmox-backup-gui/.venv/bin/python app.py
Restart=on-failure
RestartSec=5
Environment=PORT=${APP_PORT}

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable proxmox-backup-gui
"

# ── Done ──────────────────────────────────────────────────────────────────────
LXC_ADDR=$(pct exec "${LXC_ID}" -- hostname -I 2>/dev/null | awk '{print $1}' || echo "?")

echo ""
echo "=== Setup complete ==="
echo ""
echo "  LXC ${LXC_ID} IP: ${LXC_ADDR}"
echo ""
echo "  Next steps:"
echo "  1. Enter LXC:  pct enter ${LXC_ID}"
echo "  2. Edit creds: nano /opt/proxmox-backup-gui/backend/hosts.json"
echo "  3. Start:      systemctl start proxmox-backup-gui"
echo "  4. Open:       http://${LXC_ADDR}:${APP_PORT}"
echo ""
echo "  Logs: journalctl -u proxmox-backup-gui -f"
