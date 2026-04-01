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

for f in app.py config.py pbs_client.py pve_client.py restic_client.py jobs.py; do
    pct push "${LXC_ID}" "${SCRIPT_DIR}/backend/${f}" "/opt/proxmox-backup-gui/backend/${f}"
    echo "  pushed: ${f}"
done
pct push "${LXC_ID}" "${SCRIPT_DIR}/frontend/index.html" "/opt/proxmox-backup-gui/frontend/index.html"
echo "  pushed: index.html"

pct exec "${LXC_ID}" -- systemctl restart proxmox-backup-gui
echo "  restarted service"

echo "=== Done. Logs: journalctl -u proxmox-backup-gui -f ==="
