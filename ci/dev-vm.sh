#!/bin/bash
# =============================================================================
# ci/dev-vm.sh  —  Spin up a local dev/test VM from template 9092
#
# Clones template 9092 → VM 9099, sets up PBS, rclone, LXC 301, seeds backup
# data (PBS + restic) so you can interact with the GUI manually.
#
# Usage:
#   bash ci/dev-vm.sh             # start
#   bash ci/dev-vm.sh destroy     # stop and destroy VM 9099
#
# Access the GUI:
#   ssh -L 5000:10.10.0.100:5000 root@192.168.0.250
#   open http://localhost:5000
# =============================================================================

set -euo pipefail

PVE_HOST="192.168.0.200"
VM_IP="192.168.0.250"
VM_ID="9099"
TEMPLATE_ID="9092"
SSH_KEY="/var/lib/jenkins/.ssh/id_ed25519"
SSH_OPTS="-o StrictHostKeyChecking=no -o BatchMode=yes"

# Read credentials from same place as prod
RESTIC_PASSWORD="$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} cat /etc/resticprofile/restic-password)"
# PBS CI password — set in /etc/proxmox-backup-restore/config.env or pass as env var
PBS_CI_PASSWORD="${PBS_CI_PASSWORD:-gui-test-pw}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# REPO_ROOT can be overridden via env var; otherwise derive from script location.
if [ -z "${REPO_ROOT:-}" ]; then
    if [ -f "${SCRIPT_DIR}/../backend/app.py" ]; then
        REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
    else
        echo "ERROR: cannot find repo root. Set REPO_ROOT=/path/to/proxmox-backup-gui"
        exit 1
    fi
fi

# ── Destroy mode ─────────────────────────────────────────────────────────────
if [ "${1:-}" = "destroy" ]; then
    echo "=== Destroying dev VM ${VM_ID} ==="
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} "qm stop ${VM_ID} 2>/dev/null || true; sleep 3; qm destroy ${VM_ID} --purge 1 2>/dev/null || true"
    echo "Done."
    exit 0
fi

# ── Clone + start ─────────────────────────────────────────────────────────────
echo "=== Cloning template ${TEMPLATE_ID} → VM ${VM_ID} ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} "qm clone ${TEMPLATE_ID} ${VM_ID} --name gui-ci --full 1"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} "qm start ${VM_ID}"

echo "=== Waiting for SSH at ${VM_IP} (max 150s) ==="
ssh-keygen -f "${HOME}/.ssh/known_hosts" -R "${VM_IP}" 2>/dev/null || true
for i in $(seq 1 30); do
    ssh ${SSH_OPTS} -i ${SSH_KEY} -o ConnectTimeout=5 root@${VM_IP} true 2>/dev/null && { echo "SSH ready"; break; }
    sleep 5; echo "  attempt ${i}/30..."
done
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} true || { echo "ERROR: VM not reachable"; exit 1; }

# ── Fix hosts + DNS + pve-cluster ─────────────────────────────────────────────
echo "=== Fixing /etc/hosts and pve-cluster ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} << 'FIXHOSTS'
sed -i '/^127[.]0[.]1[.]1/d' /etc/hosts
echo "192.168.0.250 $(hostname)" >> /etc/hosts
rm -f /etc/resolv.conf
echo "nameserver 8.8.8.8" > /etc/resolv.conf
systemctl reset-failed pve-cluster
systemctl start pve-cluster || true
FIXHOSTS

echo "=== Waiting for PVE ==="
for i in $(seq 1 24); do
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} "pct list" >/dev/null 2>&1 && { echo "PVE ready"; break; }
    sleep 5; echo "  attempt ${i}/24..."
done

# ── PBS fingerprint + storage ─────────────────────────────────────────────────
echo "=== Configuring PBS storage ==="
PBS_FP=$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "proxmox-backup-manager cert info 2>&1 | grep -i 'fingerprint.*sha256' | sed 's/.*: //' | tr -d ' '")
echo "PBS fingerprint: ${PBS_FP}"

ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "pvesm set pbs-local --fingerprint ${PBS_FP} 2>/dev/null || \
     pvesm add pbs pbs-local --server 10.10.0.1 --datastore ci-pbs \
     --username gui-test@pbs --password ${PBS_CI_PASSWORD} \
     --fingerprint ${PBS_FP}"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "proxmox-backup-manager acl update /datastore/ci-pbs DatastoreAdmin --auth-id gui-test@pbs"

# ── rclone.conf + binaries ────────────────────────────────────────────────────
echo "=== Deploying rclone.conf + binaries ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} "cat /root/.config/rclone/rclone.conf" | \
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
        "mkdir -p /root/.config/rclone && cat > /root/.config/rclone/rclone.conf"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} "cat /usr/bin/rclone" | \
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} "cat > /usr/local/bin/rclone && chmod +x /usr/local/bin/rclone"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} "cat /usr/bin/restic" | \
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} "cat > /usr/local/bin/restic && chmod +x /usr/local/bin/restic"

# ── LXC 301 + SSH key + hosts.json ───────────────────────────────────────────
echo "=== Creating LXC 301 and configuring LXC 300 ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} bash << STEP_B
set -e
pct stop 300 2>/dev/null || true
pct clone 300 301 --full 1
pct start 300
sleep 5

pct exec 300 -- bash -c '
    mkdir -p /root/.ssh && chmod 700 /root/.ssh
    [ -f /root/.ssh/id_ed25519 ] || ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519 -N ""
'
LXC300_PUBKEY=\$(pct exec 300 -- cat /root/.ssh/id_ed25519.pub)
grep -qF "\${LXC300_PUBKEY}" /root/.ssh/authorized_keys 2>/dev/null || \
    echo "\${LXC300_PUBKEY}" >> /root/.ssh/authorized_keys

pct exec 300 -- python3 -c "
import json
with open('/opt/proxmox-backup-gui/backend/hosts.json') as f: h=json.load(f)
h[0]['restic_repo'] = 'rclone:gdrive:bu/gui-ci-test'
h[0]['restic_password'] = '${RESTIC_PASSWORD}'
h[0]['restic_env'] = {'RCLONE_CONFIG': '/root/.config/rclone/rclone.conf'}
h[0]['pbs_storage_id'] = 'pbs-local'
h[0]['pbs_datastore_path'] = '/mnt/ci-pbs'
h[0]['pve_ssh_host'] = '10.10.0.1'
open('/opt/proxmox-backup-gui/backend/hosts.json','w').write(json.dumps(h))
print('hosts.json updated')
"

pct exec 300 -- bash -c '
    ssh-keygen -f /root/.ssh/known_hosts -R 10.10.0.1 2>/dev/null || true
    ssh-keyscan -t ed25519 10.10.0.1 >> /root/.ssh/known_hosts 2>/dev/null || true
'
STEP_B

# ── Deploy latest code ────────────────────────────────────────────────────────
echo "=== Deploying latest code from ${REPO_ROOT} ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} "mkdir -p /tmp/gui-update"
scp ${SSH_OPTS} -i ${SSH_KEY} \
    "${REPO_ROOT}/backend/app.py" "${REPO_ROOT}/backend/config.py" \
    "${REPO_ROOT}/backend/pbs_client.py" "${REPO_ROOT}/backend/pve_client.py" \
    "${REPO_ROOT}/backend/restic_client.py" "${REPO_ROOT}/backend/jobs.py" \
    "${REPO_ROOT}/requirements.txt" \
    root@${VM_IP}:/tmp/gui-update/
scp ${SSH_OPTS} -i ${SSH_KEY} \
    "${REPO_ROOT}/frontend/index.html" \
    root@${VM_IP}:/tmp/gui-update/

ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} << 'DEPLOY'
set -e
for f in app.py config.py pbs_client.py pve_client.py restic_client.py jobs.py; do
    pct push 300 /tmp/gui-update/${f} /opt/proxmox-backup-gui/backend/${f}
done
pct push 300 /tmp/gui-update/requirements.txt /opt/proxmox-backup-gui/requirements.txt
pct push 300 /tmp/gui-update/index.html /opt/proxmox-backup-gui/frontend/index.html
pct exec 300 -- systemctl restart proxmox-backup-gui
sleep 3
pct exec 300 -- systemctl is-active proxmox-backup-gui
DEPLOY

# ── Seed: PBS backup #1 ───────────────────────────────────────────────────────
echo "=== Seeding: PBS backup of LXC 301 ==="
JOB=$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "pct exec 300 -- python3 -c \"import urllib.request,json; req=urllib.request.Request('http://localhost:5000/api/host/ci/backup/pbs',data=json.dumps({'vmid':301,'type':'ct'}).encode(),headers={'Content-Type':'application/json'},method='POST'); print(urllib.request.urlopen(req,timeout=10).read().decode())\"")
JOB_ID=$(echo "${JOB}" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)
echo "PBS backup job: ${JOB_ID}"
for i in $(seq 1 60); do
    sleep 5
    STATUS=$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
        "pct exec 300 -- python3 -c \"import urllib.request; print(urllib.request.urlopen('http://localhost:5000/api/job/${JOB_ID}',timeout=5).read().decode())\"" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
    echo "  status: ${STATUS}"
    [ "${STATUS}" = "done" ] && { echo "PBS backup #1 done"; break; }
    [ "${STATUS}" = "error" ] && { echo "ERROR: PBS backup failed"; exit 1; }
done

# ── Seed: Restic backup ───────────────────────────────────────────────────────
echo "=== Seeding: Restic backup to GDrive ==="
RESTIC_ENV="RESTIC_REPOSITORY=rclone:gdrive:bu/gui-ci-test RESTIC_PASSWORD=${RESTIC_PASSWORD} RCLONE_CONFIG=/root/.config/rclone/rclone.conf"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "${RESTIC_ENV} restic init --no-lock 2>&1" || true

# Forget all old snapshots
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} bash << FORGET_OLD
RESTIC_ENV="RESTIC_REPOSITORY=rclone:gdrive:bu/gui-ci-test RESTIC_PASSWORD=${RESTIC_PASSWORD} RCLONE_CONFIG=/root/.config/rclone/rclone.conf"
IDS=\$(eval "\$RESTIC_ENV restic snapshots --json --no-lock 2>/dev/null" | \
    python3 -c "import json,sys; snaps=json.load(sys.stdin); print(' '.join(s['id'] for s in snaps))" 2>/dev/null || true)
if [ -n "\$IDS" ]; then
    echo "Forgetting old snapshots: \$IDS"
    eval "\$RESTIC_ENV restic forget \$IDS --prune 2>&1"
fi
FORGET_OLD

ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} "systemctl stop proxmox-backup proxmox-backup-proxy"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "${RESTIC_ENV} restic backup /mnt/ci-pbs --tag ct-301 --json --no-lock 2>&1 | tail -3"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} "systemctl start proxmox-backup proxmox-backup-proxy"

for i in $(seq 1 24); do
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
        "proxmox-backup-manager datastore list --output-format json >/dev/null 2>&1" && { echo "PBS ready"; break; }
    sleep 5; echo "  waiting for PBS... ${i}/24"
done

# ── Seed: PBS backup #2 ───────────────────────────────────────────────────────
echo "=== Seeding: PBS backup #2 (for cloud-only entry) ==="
JOB3=$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "pct exec 300 -- python3 -c \"import urllib.request,json; req=urllib.request.Request('http://localhost:5000/api/host/ci/backup/pbs',data=json.dumps({'vmid':301,'type':'ct'}).encode(),headers={'Content-Type':'application/json'},method='POST'); print(urllib.request.urlopen(req,timeout=10).read().decode())\"")
JOB3_ID=$(echo "${JOB3}" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)
for i in $(seq 1 60); do
    sleep 5
    STATUS=$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
        "pct exec 300 -- python3 -c \"import urllib.request; print(urllib.request.urlopen('http://localhost:5000/api/job/${JOB3_ID}',timeout=5).read().decode())\"" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
    echo "  status: ${STATUS}"
    [ "${STATUS}" = "done" ] && { echo "PBS backup #2 done"; break; }
    [ "${STATUS}" = "error" ] && { echo "ERROR: PBS backup #2 failed"; exit 1; }
done

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "=========================================================="
echo " Dev VM ready!"
echo ""
echo " GUI:  ssh -L 5000:10.10.0.100:5000 root@${VM_IP}"
echo "       then open http://localhost:5000"
echo ""
echo " Destroy when done:"
echo "       bash ci/dev-vm.sh destroy"
echo "=========================================================="
