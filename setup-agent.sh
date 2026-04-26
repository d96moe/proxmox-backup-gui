#!/bin/bash
# =============================================================================
# setup-agent.sh
# Installs pve_agent.py on the current PVE host as a systemd service.
#
# Run on the PVE host as root:
#   bash setup-agent.sh
#
# Key env-var overrides:
#   AGENT_DIR      install directory     (default: /opt/pve-agent)
#   AGENT_PORT     HTTP API port         (default: 8099)
#   MQTT_HOST      Mosquitto broker IP   (required — IP of your GUI LXC)
#   MQTT_PORT      Mosquitto TCP port    (default: 1883)
#   MQTT_HOSTNAME  host ID in hosts.json (required — e.g. "home" or "cabin")
#   AGENT_TOKEN    shared API secret     (auto-generated if not set)
# =============================================================================

set -euo pipefail

AGENT_DIR="${AGENT_DIR:-/opt/pve-agent}"
CONFIG_DIR="/etc/pve-agent"
AGENT_PORT="${AGENT_PORT:-8099}"
MQTT_PORT="${MQTT_PORT:-1883}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Prompt for required values if not set via env ──────────────────────────────

if [ -z "${MQTT_HOST:-}" ]; then
    read -rp "Mosquitto broker IP (IP of your GUI LXC, e.g. 192.168.0.199): " MQTT_HOST
fi

if [ -z "${MQTT_HOSTNAME:-}" ]; then
    read -rp "Host ID (must match 'id' in hosts.json, e.g. home): " MQTT_HOSTNAME
fi

if [ -z "${AGENT_TOKEN:-}" ]; then
    AGENT_TOKEN="$(tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32)"
    _GENERATED_TOKEN=1
fi

# ── Install Python 3 + pip if missing ─────────────────────────────────────────
echo "=== Checking Python ==="
if ! command -v python3 &>/dev/null; then
    apt-get update -qq
    apt-get install -y -qq python3 python3-pip python3-venv
fi

# ── Create directories ─────────────────────────────────────────────────────────
echo "=== Creating directories ==="
mkdir -p "${AGENT_DIR}" "${CONFIG_DIR}"

# ── Copy agent files ───────────────────────────────────────────────────────────
echo "=== Installing agent files ==="
cp "${SCRIPT_DIR}/backend/pve_agent.py"  "${AGENT_DIR}/pve_agent.py"
cp "${SCRIPT_DIR}/backend/pbs_client.py" "${AGENT_DIR}/pbs_client.py"
cp "${SCRIPT_DIR}/backend/pve_client.py" "${AGENT_DIR}/pve_client.py"

# ── Python venv + deps ─────────────────────────────────────────────────────────
echo "=== Installing Python dependencies ==="
python3 -m venv "${AGENT_DIR}/.venv"
"${AGENT_DIR}/.venv/bin/pip" install -q \
    "flask>=3.0" "flask-sock>=0.7" "requests>=2.31" "paho-mqtt>=1.6" "urllib3>=2.0"

# ── Write config.json (only if it doesn't exist yet) ──────────────────────────
if [ -f "${CONFIG_DIR}/config.json" ]; then
    echo "=== Config already exists — skipping (${CONFIG_DIR}/config.json) ==="
else
    echo "=== Writing config template ==="
    # Detect local PVE IP (first non-loopback address)
    PVE_IP=$(ip -4 addr show scope global | awk '/inet/{print $2}' | cut -d/ -f1 | head -1)

    cat > "${CONFIG_DIR}/config.json" << EOF
{
  "pve_url":            "https://${PVE_IP}:8006",
  "pve_user":           "root@pam",
  "pve_password":       "CHANGE_ME",
  "pbs_url":            "https://${PVE_IP}:8007",
  "pbs_user":           "backup@pbs",
  "pbs_password":       "CHANGE_ME",
  "pbs_datastore":      "CHANGE_ME",
  "pbs_storage_id":     "CHANGE_ME",
  "pbs_datastore_path": "/mnt/datastore/CHANGE_ME",
  "restic_repo":        "",
  "restic_password":    "",
  "restic_env":         {},
  "mqtt_host":          "${MQTT_HOST}",
  "mqtt_port":          ${MQTT_PORT},
  "mqtt_hostname":      "${MQTT_HOSTNAME}",
  "agent_token":        "${AGENT_TOKEN}",
  "agent_port":         ${AGENT_PORT},
  "verify_ssl":         false
}
EOF
    chmod 600 "${CONFIG_DIR}/config.json"
    echo "  Config written to ${CONFIG_DIR}/config.json"
    echo "  Fill in the CHANGE_ME fields before starting the agent."
fi

# ── systemd unit ───────────────────────────────────────────────────────────────
echo "=== Creating systemd service ==="
cat > /etc/systemd/system/pve-agent.service << EOF
[Unit]
Description=PVE Agent (proxmox-backup-gui)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${AGENT_DIR}
ExecStart=${AGENT_DIR}/.venv/bin/python pve_agent.py ${CONFIG_DIR}/config.json
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable pve-agent

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "=== Agent installed ==="
echo ""
echo "  Files:   ${AGENT_DIR}/"
echo "  Config:  ${CONFIG_DIR}/config.json"
echo "  Service: pve-agent"
echo ""
if [ -f "${CONFIG_DIR}/config.json" ] && grep -q "CHANGE_ME" "${CONFIG_DIR}/config.json"; then
    echo "  Next steps:"
    echo "  1. Fill in credentials:"
    echo "     nano ${CONFIG_DIR}/config.json"
    echo "  2. Start the agent:"
    echo "     systemctl start pve-agent"
    echo "  3. Check logs:"
    echo "     journalctl -u pve-agent -f"
else
    echo "  Starting agent..."
    systemctl start pve-agent
    echo "  Done. Logs: journalctl -u pve-agent -f"
fi
echo ""
echo "  Add this host to hosts.json in your GUI LXC:"
echo "    {"
echo "      \"id\":          \"${MQTT_HOSTNAME}\","
echo "      \"label\":       \"pve · ${MQTT_HOSTNAME}\","
echo "      \"agent_url\":   \"http://$(hostname -I | awk '{print $1}'):${AGENT_PORT}\","
echo "      \"agent_token\": \"${AGENT_TOKEN}\""
echo "    }"
echo ""
if [ -n "${_GENERATED_TOKEN:-}" ]; then
    echo "  Agent token (auto-generated — already in config.json):"
    echo "    ${AGENT_TOKEN}"
    echo ""
fi
