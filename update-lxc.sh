#!/bin/bash
# =============================================================================
# update-lxc.sh
# Push updated app files to the LXC and restart the service.
# Run on the PVE host as root.
# =============================================================================

set -euo pipefail

LXC_ID="${LXC_ID:-199}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Updating proxmox-backup-gui in LXC ${LXC_ID} ==="

for f in app.py config.py pbs_client.py pve_client.py restic_client.py jobs.py agent_client.py auth.py; do
    [ -f "${SCRIPT_DIR}/backend/${f}" ] || continue
    pct push "${LXC_ID}" "${SCRIPT_DIR}/backend/${f}" "/opt/proxmox-backup-gui/backend/${f}"
    echo "  pushed: ${f}"
done
pct push "${LXC_ID}" "${SCRIPT_DIR}/requirements.txt" "/opt/proxmox-backup-gui/requirements.txt"
echo "  pushed: requirements.txt"
pct push "${LXC_ID}" "${SCRIPT_DIR}/frontend/index.html" "/opt/proxmox-backup-gui/frontend/index.html"
echo "  pushed: index.html"
pct push "${LXC_ID}" "${SCRIPT_DIR}/frontend/login.html" "/opt/proxmox-backup-gui/frontend/login.html"
echo "  pushed: login.html"
pct push "${LXC_ID}" "${SCRIPT_DIR}/frontend/mqtt.min.js" "/opt/proxmox-backup-gui/frontend/mqtt.min.js"
echo "  pushed: mqtt.min.js"

# Re-run pip in case dependencies changed
pct exec "${LXC_ID}" -- /opt/proxmox-backup-gui/.venv/bin/pip install -q -r /opt/proxmox-backup-gui/requirements.txt

# Ensure Mosquitto is installed and running
if ! pct exec "${LXC_ID}" -- systemctl is-active mosquitto >/dev/null 2>&1; then
    echo "  installing/starting mosquitto..."
    pct exec "${LXC_ID}" -- bash -c "
        apt-get install -y -qq mosquitto
        cat > /etc/mosquitto/conf.d/gui.conf << 'EOF'
listener 1883
protocol mqtt
allow_anonymous true

listener 9001
protocol websockets
allow_anonymous true
EOF
        systemctl enable mosquitto
        systemctl restart mosquitto
    "
    echo "  mosquitto started"
fi

pct exec "${LXC_ID}" -- systemctl restart proxmox-backup-gui
echo "  restarted service"

echo "=== Done. Logs: journalctl -u proxmox-backup-gui -f ==="
