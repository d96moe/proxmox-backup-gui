#!/bin/bash
# =============================================================================
# ci/dev-vm.sh  —  Manuell CI-VM för iterativ testning
#
# Kör FRÅN 192.168.0.200 (PVE-hosten), inte från Windows.
# Använder Jenkins SSH-nyckeln (/var/lib/jenkins/.ssh/id_ed25519).
#
# Usage:
#   bash ci/dev-vm.sh              # Full setup + seed (15-20 min)
#   bash ci/dev-vm.sh deploy       # Deploy ny kod + kör tester (VM måste finnas)
#   bash ci/dev-vm.sh test         # Kör bara tester (VM + seed måste finnas)
#   bash ci/dev-vm.sh destroy      # Stoppa och ta bort VM 9099
#
# Åtkomst till GUI under testning:
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
BRANCH="${BRANCH:-feature/delete-backup}"

# ── Credentials ───────────────────────────────────────────────────────────────
# RESTIC_PASSWORD: läs från prod-PVE precis som Jenkins gör
RESTIC_PASSWORD="$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} \
    "cat /etc/resticprofile/restic-password 2>/dev/null || \
     cat /root/.restic-password 2>/dev/null || true")"
if [ -z "${RESTIC_PASSWORD}" ]; then
    echo "ERROR: Kan inte läsa RESTIC_PASSWORD från ${PVE_HOST}"
    echo "Sätt: export RESTIC_PASSWORD=<lösenord>"
    exit 1
fi

# PBS CI-lösenord — hämta från Jenkins credentials store eller env
PBS_CI_PASSWORD="${PBS_CI_PASSWORD:-}"
if [ -z "${PBS_CI_PASSWORD}" ]; then
    # Försök läsa från Jenkins credential store via API
    PBS_CI_PASSWORD="$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} \
        "cat /var/lib/jenkins/secrets/gui-ci-pbs-password 2>/dev/null || \
         grep -A1 'gui-ci-pbs-password' /var/lib/jenkins/credentials.xml 2>/dev/null | \
         grep -o '<secret>[^<]*</secret>' | sed 's/<[^>]*>//g' || true" 2>/dev/null || true)"
fi
if [ -z "${PBS_CI_PASSWORD}" ]; then
    echo "ERROR: PBS_CI_PASSWORD inte satt."
    echo "Kör: export PBS_CI_PASSWORD=<lösenord>  och försök igen"
    exit 1
fi

# ── Repo root ─────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-}"
if [ -z "${REPO_ROOT}" ]; then
    if [ -f "${SCRIPT_DIR}/../backend/app.py" ]; then
        REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
    else
        echo "ERROR: Hitta inte repo root. Kör från repo-katalogen eller sätt REPO_ROOT="
        exit 1
    fi
fi
echo "Repo root: ${REPO_ROOT}"

# ─────────────────────────────────────────────────────────────────────────────
# Destroy
# ─────────────────────────────────────────────────────────────────────────────
if [ "${1:-}" = "destroy" ]; then
    echo "=== Tar bort dev-VM ${VM_ID} ==="
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} \
        "qm stop ${VM_ID} 2>/dev/null || true; sleep 3; qm destroy ${VM_ID} --purge 1 2>/dev/null || true"
    echo "Klar."
    exit 0
fi

# ─────────────────────────────────────────────────────────────────────────────
# Helper: Deploy senaste koden till VM (används av "deploy" och "full")
# ─────────────────────────────────────────────────────────────────────────────
deploy_code() {
    echo "=== Deploying latest code ==="
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} "mkdir -p /tmp/gui-update/tests"

    scp ${SSH_OPTS} -i ${SSH_KEY} \
        "${REPO_ROOT}/backend/app.py" \
        "${REPO_ROOT}/backend/config.py" \
        "${REPO_ROOT}/backend/pbs_client.py" \
        "${REPO_ROOT}/backend/pve_client.py" \
        "${REPO_ROOT}/backend/restic_client.py" \
        "${REPO_ROOT}/backend/jobs.py" \
        "${REPO_ROOT}/requirements.txt" \
        root@${VM_IP}:/tmp/gui-update/

    scp ${SSH_OPTS} -i ${SSH_KEY} \
        "${REPO_ROOT}/frontend/index.html" \
        root@${VM_IP}:/tmp/gui-update/

    scp ${SSH_OPTS} -i ${SSH_KEY} \
        "${REPO_ROOT}/ci/tests/test_frontend.py" \
        "${REPO_ROOT}/ci/tests/test_restore.py" \
        "${REPO_ROOT}/ci/tests/test_self_restore.py" \
        "${REPO_ROOT}/ci/tests/conftest.py" \
        root@${VM_IP}:/tmp/gui-update/tests/

    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} bash << 'DEPLOY'
set -e
pct start 300 || true
for f in app.py config.py pbs_client.py pve_client.py restic_client.py jobs.py; do
    pct push 300 /tmp/gui-update/${f} /opt/proxmox-backup-gui/backend/${f}
done
pct push 300 /tmp/gui-update/requirements.txt /opt/proxmox-backup-gui/requirements.txt
pct push 300 /tmp/gui-update/index.html /opt/proxmox-backup-gui/frontend/index.html
pct exec 300 -- systemctl restart proxmox-backup-gui
sleep 3
pct exec 300 -- systemctl is-active proxmox-backup-gui

mkdir -p /opt/ci/proxmox-backup-gui/tests /opt/ci/proxmox-backup-gui/frontend
cp /tmp/gui-update/tests/test_frontend.py     /opt/ci/proxmox-backup-gui/tests/
cp /tmp/gui-update/tests/test_restore.py      /opt/ci/proxmox-backup-gui/tests/
cp /tmp/gui-update/tests/test_self_restore.py /opt/ci/proxmox-backup-gui/tests/ 2>/dev/null || true
cp /tmp/gui-update/tests/conftest.py          /opt/ci/proxmox-backup-gui/tests/
cp /tmp/gui-update/index.html             /opt/ci/proxmox-backup-gui/frontend/
echo "Deploy klar."
DEPLOY
}

# ─────────────────────────────────────────────────────────────────────────────
# Helper: Kör tester på VM
# ─────────────────────────────────────────────────────────────────────────────
run_tests() {
    echo "=== Kör integration tests ==="
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} bash << 'RUNTESTS'
source /etc/profile.d/ci-venv.sh 2>/dev/null || true
mkdir -p /tmp/test-results

echo "Kontrollerar Flask backend..."
for i in $(seq 1 20); do
    curl -sf http://10.10.0.100:5000/api/hosts >/dev/null 2>&1 && { echo "Backend redo"; break; }
    echo "  väntar... ($i/20)"; sleep 3
done
curl -sf http://10.10.0.100:5000/api/hosts >/dev/null 2>&1 || {
    echo "ERROR: Flask backend ej nåbar vid 10.10.0.100:5000"
    pct status 300 || true
    exit 1
}

echo "--- PRE-TEST RESTIC DIAGNOSTICS ---"
curl -sf "http://10.10.0.100:5000/api/host/ci/items" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('LXC snapshots (vmid, backup_time, local, cloud, restic_short):')
for item in d.get('lxcs',[]):
    vmid=item['id']
    for s in item.get('snapshots',[]):
        rid=s.get('restic_id','')
        print(' ',vmid,s['backup_time'],s.get('local'),s.get('cloud'),rid[:8] if rid else '')
" 2>/dev/null || echo "items endpoint failed"
echo "--- END DIAGNOSTICS ---"

cd /opt/ci/proxmox-backup-gui
BACKEND_URL=http://10.10.0.100:5000 \
pytest tests/ -v --tb=short --junit-xml=/tmp/test-results/integration.xml 2>&1
RUNTESTS
}

# ─────────────────────────────────────────────────────────────────────────────
# "deploy" mode: kör bara deploy + tester (VM måste finnas)
# ─────────────────────────────────────────────────────────────────────────────
if [ "${1:-}" = "deploy" ]; then
    deploy_code
    run_tests
    exit 0
fi

# ─────────────────────────────────────────────────────────────────────────────
# "test" mode: kör bara tester (VM + seed måste finnas)
# ─────────────────────────────────────────────────────────────────────────────
if [ "${1:-}" = "test" ]; then
    run_tests
    exit 0
fi

# ─────────────────────────────────────────────────────────────────────────────
# Full setup (default)
# ─────────────────────────────────────────────────────────────────────────────

# ── Clone + start ─────────────────────────────────────────────────────────────
echo "=== Klonar template ${TEMPLATE_ID} → VM ${VM_ID} ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} \
    "qm clone ${TEMPLATE_ID} ${VM_ID} --name gui-ci --full 1"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} "qm start ${VM_ID}"

echo "=== Väntar på SSH vid ${VM_IP} ==="
ssh-keygen -f "${HOME}/.ssh/known_hosts" -R "${VM_IP}" 2>/dev/null || true
for i in $(seq 1 30); do
    ssh ${SSH_OPTS} -i ${SSH_KEY} -o ConnectTimeout=5 root@${VM_IP} true 2>/dev/null \
        && { echo "SSH redo"; break; }
    sleep 5; echo "  försök ${i}/30..."
done
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} true || { echo "ERROR: VM ej nåbar"; exit 1; }

# ── Fix hosts + DNS + pve-cluster ─────────────────────────────────────────────
echo "=== Fixar /etc/hosts och pve-cluster ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} << 'FIXHOSTS'
sed -i '/^127[.]0[.]1[.]1/d' /etc/hosts
echo "192.168.0.250 $(hostname)" >> /etc/hosts
rm -f /etc/resolv.conf
echo "nameserver 8.8.8.8" > /etc/resolv.conf
systemctl reset-failed pve-cluster
systemctl start pve-cluster || true
FIXHOSTS

echo "=== Väntar på PVE ==="
for i in $(seq 1 24); do
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} "pct list" >/dev/null 2>&1 \
        && { echo "PVE redo"; break; }
    sleep 5; echo "  PVE försök ${i}/24..."
done
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} "pct list" \
    || { echo "ERROR: PVE ej redo"; exit 1; }

# ── PBS fingerprint + storage ─────────────────────────────────────────────────
echo "=== Konfigurerar PBS storage ==="
PBS_FP=$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "proxmox-backup-manager cert info 2>&1 | grep -i 'fingerprint.*sha256' \
     | sed 's/.*: //' | tr -d ' '")
echo "PBS fingerprint: ${PBS_FP}"

ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "pvesm set pbs-local --fingerprint ${PBS_FP} 2>/dev/null || \
     pvesm add pbs pbs-local --server 10.10.0.1 --datastore ci-pbs \
     --username gui-test@pbs --password '${PBS_CI_PASSWORD}' \
     --fingerprint ${PBS_FP}"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "proxmox-backup-manager acl update /datastore/ci-pbs DatastoreAdmin \
     --auth-id gui-test@pbs"
echo "PBS storage klar"

# ── rclone.conf + binaries ────────────────────────────────────────────────────
echo "=== Deployer rclone.conf + binaries ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} \
    "cat /root/.config/rclone/rclone.conf" | \
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "mkdir -p /root/.config/rclone && cat > /root/.config/rclone/rclone.conf"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} "cat /usr/bin/rclone" | \
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "cat > /usr/local/bin/rclone && chmod +x /usr/local/bin/rclone"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${PVE_HOST} "cat /usr/bin/restic" | \
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "cat > /usr/local/bin/restic && chmod +x /usr/local/bin/restic"

# ── Skriv lösenord till /tmp/.restic_pw på VM (för quoted heredocs) ──────────
echo "=== Skriver restic-lösenord till VM ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "printf '%s' '${RESTIC_PASSWORD}' > /tmp/.restic_pw && chmod 600 /tmp/.restic_pw"

# ── LXC 301 + SSH-nyckel + hosts.json ────────────────────────────────────────
echo "=== Skapar LXC 301 och konfigurerar LXC 300 ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} bash << 'STEP_B'
set -e
pct stop 300 2>/dev/null || true
pct stop 301 2>/dev/null || true
pct destroy 301 --purge 1 2>/dev/null || true
pct clone 300 301 --full 1
pct start 300
sleep 5

pct exec 300 -- bash -c '
    mkdir -p /root/.ssh && chmod 700 /root/.ssh
    [ -f /root/.ssh/id_ed25519 ] || ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519 -N ""
'
LXC300_PUBKEY=$(pct exec 300 -- cat /root/.ssh/id_ed25519.pub)
grep -qF "${LXC300_PUBKEY}" /root/.ssh/authorized_keys 2>/dev/null || \
    echo "${LXC300_PUBKEY}" >> /root/.ssh/authorized_keys

# hosts.json: läs lösenord från temp-filen (quoted heredoc → $(cat) istf ${VAR})
pct exec 300 -- python3 -c "
import json
with open('/opt/proxmox-backup-gui/backend/hosts.json') as f: h=json.load(f)
h[0]['restic_repo'] = 'rclone:gdrive:bu/gui-ci-test'
h[0]['restic_password'] = '$(cat /tmp/.restic_pw)'
h[0]['restic_env'] = {'RCLONE_CONFIG': '/root/.config/rclone/rclone.conf'}
h[0]['pbs_storage_id'] = 'pbs-local'
h[0]['pbs_datastore_path'] = '/mnt/ci-pbs'
h[0]['pve_ssh_host'] = '10.10.0.1'
open('/opt/proxmox-backup-gui/backend/hosts.json','w').write(json.dumps(h))
print('hosts.json uppdaterad')
"

pct exec 300 -- bash -c '
    ssh-keygen -f /root/.ssh/known_hosts -R 10.10.0.1 2>/dev/null || true
    ssh-keyscan -t ed25519 10.10.0.1 >> /root/.ssh/known_hosts 2>/dev/null || true
'
echo "STEP_B: klar"
STEP_B

# ── Deploy kod ────────────────────────────────────────────────────────────────
deploy_code

# ── SEED: Rensa stale PBS-backups ─────────────────────────────────────────────
echo "=== Rensar stale PBS-backups (t.ex. ct/300 från template) ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} bash << 'PURGE_STALE_PBS'
systemctl stop proxmox-backup proxmox-backup-proxy 2>/dev/null || true
for id_dir in /mnt/ci-pbs/ct/*/; do
    id=$(basename "$id_dir")
    [ "$id" = "301" ] && continue
    echo "Tar bort stale PBS ct/$id..."
    rm -rf "$id_dir"
done
for id_dir in /mnt/ci-pbs/vm/*/; do
    id=$(basename "$id_dir")
    echo "Tar bort stale PBS vm/$id..."
    rm -rf "$id_dir"
done
systemctl start proxmox-backup proxmox-backup-proxy 2>/dev/null || true
for i in $(seq 1 24); do
    proxmox-backup-manager datastore list --output-format json >/dev/null 2>&1 \
        && { echo "PBS redo efter rensning"; break; }
    echo "  väntar på PBS... ($i/24)"; sleep 5
done
PURGE_STALE_PBS

# ── SEED: T1 — PBS backup #1 ─────────────────────────────────────────────────
echo "=== Seed: PBS backup T1 (ct/301) ==="
JOB=$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "pct exec 300 -- python3 -c \"import urllib.request,json; \
     req=urllib.request.Request('http://localhost:5000/api/host/ci/backup/pbs', \
     data=json.dumps({'vmid':301,'type':'ct'}).encode(), \
     headers={'Content-Type':'application/json'},method='POST'); \
     print(urllib.request.urlopen(req,timeout=10).read().decode())\"")
JOB_ID=$(echo "${JOB}" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)
echo "T1 job: ${JOB_ID}"
for i in $(seq 1 60); do
    sleep 5
    STATUS=$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
        "pct exec 300 -- python3 -c \"import urllib.request; \
         print(urllib.request.urlopen('http://localhost:5000/api/job/${JOB_ID}',timeout=5).read().decode())\"" \
        | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
    echo "  T1 status: ${STATUS}"
    [ "${STATUS}" = "done" ] && { echo "T1 klar"; break; }
    [ "${STATUS}" = "error" ] && { echo "ERROR: T1 misslyckades"; exit 1; }
done

# ── SEED: FORGET_OLD + R1 — Restic backup #1 ─────────────────────────────────
echo "=== Seed: Glömmer gamla restic-snapshots ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "RESTIC_REPOSITORY=rclone:gdrive:bu/gui-ci-test RESTIC_PASSWORD=$(printf '%s' "${RESTIC_PASSWORD}") \
     RCLONE_CONFIG=/root/.config/rclone/rclone.conf \
     restic init --no-lock 2>&1" || true

# Quoted heredoc → använd /tmp/.restic_pw på remote
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} bash << 'FORGET_OLD'
RESTIC_ENV="RESTIC_REPOSITORY=rclone:gdrive:bu/gui-ci-test RESTIC_PASSWORD=$(cat /tmp/.restic_pw) RCLONE_CONFIG=/root/.config/rclone/rclone.conf"
IDS=$(eval "$RESTIC_ENV restic snapshots --json --no-lock 2>/dev/null" | \
    python3 -c "import json,sys; snaps=json.load(sys.stdin); print(' '.join(s['id'] for s in snaps))" 2>/dev/null || true)
if [ -n "$IDS" ]; then
    echo "Glömmer: $IDS"
    eval "$RESTIC_ENV restic forget $IDS --prune 2>&1"
else
    echo "Inga gamla snapshots att glömma"
fi
FORGET_OLD

echo "=== Seed: Restic backup R1 (täcker T1) ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "systemctl stop proxmox-backup proxmox-backup-proxy"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} bash << 'RESTIC_BACKUP_R1'
TAG_ARGS=()
for backup_type in vm ct; do
    type_dir="/mnt/ci-pbs/${backup_type}"
    [ -d "${type_dir}" ] || continue
    for id_dir in "${type_dir}"/*/; do
        id=$(basename "${id_dir}")
        [[ "${id}" =~ ^[0-9]+$ ]] || continue
        for snap_dir in "${id_dir}"*/; do
            snap=$(basename "${snap_dir}")
            ts=$(date -d "${snap}" +%s 2>/dev/null) || continue
            TAG_ARGS+=("--tag" "${backup_type}-${id}-${ts}")
        done
    done
done
echo "R1 tags: ${TAG_ARGS[*]}"
RESTIC_REPOSITORY=rclone:gdrive:bu/gui-ci-test RESTIC_PASSWORD=$(cat /tmp/.restic_pw) \
RCLONE_CONFIG=/root/.config/rclone/rclone.conf \
    restic backup /mnt/ci-pbs "${TAG_ARGS[@]}" --json --no-lock 2>&1 | tail -5
RESTIC_BACKUP_R1
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "systemctl start proxmox-backup proxmox-backup-proxy"
for i in $(seq 1 24); do
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
        "proxmox-backup-manager datastore list --output-format json >/dev/null 2>&1" \
        && { echo "PBS redo efter R1"; break; }
    sleep 5; echo "  väntar på PBS... ${i}/24"
done

# ── SEED: T2 — PBS backup #2 ─────────────────────────────────────────────────
echo "=== Seed: PBS backup T2 (ct/301) ==="
JOB2=$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "pct exec 300 -- python3 -c \"import urllib.request,json; \
     req=urllib.request.Request('http://localhost:5000/api/host/ci/backup/pbs', \
     data=json.dumps({'vmid':301,'type':'ct'}).encode(), \
     headers={'Content-Type':'application/json'},method='POST'); \
     print(urllib.request.urlopen(req,timeout=10).read().decode())\"")
JOB2_ID=$(echo "${JOB2}" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)
echo "T2 job: ${JOB2_ID}"
for i in $(seq 1 60); do
    sleep 5
    STATUS=$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
        "pct exec 300 -- python3 -c \"import urllib.request; \
         print(urllib.request.urlopen('http://localhost:5000/api/job/${JOB2_ID}',timeout=5).read().decode())\"" \
        | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
    echo "  T2 status: ${STATUS}"
    [ "${STATUS}" = "done" ] && { echo "T2 klar"; break; }
    [ "${STATUS}" = "error" ] && { echo "ERROR: T2 misslyckades"; exit 1; }
done

# ── SEED: R2 — Restic backup #2 (täcker T1+T2) ───────────────────────────────
echo "=== Seed: Restic backup R2 (täcker T1+T2) ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "systemctl stop proxmox-backup proxmox-backup-proxy"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} bash << 'RESTIC_BACKUP_R2'
TAG_ARGS=()
for backup_type in vm ct; do
    type_dir="/mnt/ci-pbs/${backup_type}"
    [ -d "${type_dir}" ] || continue
    for id_dir in "${type_dir}"/*/; do
        id=$(basename "${id_dir}")
        [[ "${id}" =~ ^[0-9]+$ ]] || continue
        for snap_dir in "${id_dir}"*/; do
            snap=$(basename "${snap_dir}")
            ts=$(date -d "${snap}" +%s 2>/dev/null) || continue
            TAG_ARGS+=("--tag" "${backup_type}-${id}-${ts}")
        done
    done
done
echo "R2 tags: ${TAG_ARGS[*]}"
RESTIC_REPOSITORY=rclone:gdrive:bu/gui-ci-test RESTIC_PASSWORD=$(cat /tmp/.restic_pw) \
RCLONE_CONFIG=/root/.config/rclone/rclone.conf \
    restic backup /mnt/ci-pbs "${TAG_ARGS[@]}" --json --no-lock 2>&1 | tail -5
RESTIC_BACKUP_R2
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} "rm -f /tmp/.restic_pw"
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "systemctl start proxmox-backup proxmox-backup-proxy"
for i in $(seq 1 24); do
    ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
        "proxmox-backup-manager datastore list --output-format json >/dev/null 2>&1" \
        && { echo "PBS redo efter R2"; break; }
    sleep 5; echo "  väntar på PBS... ${i}/24"
done

# ── SEED: T3 — PBS backup #3 (local-only) ────────────────────────────────────
echo "=== Seed: PBS backup T3 (local-only, ingen restic-täckning) ==="
JOB3=$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "pct exec 300 -- python3 -c \"import urllib.request,json; \
     req=urllib.request.Request('http://localhost:5000/api/host/ci/backup/pbs', \
     data=json.dumps({'vmid':301,'type':'ct'}).encode(), \
     headers={'Content-Type':'application/json'},method='POST'); \
     print(urllib.request.urlopen(req,timeout=10).read().decode())\"")
JOB3_ID=$(echo "${JOB3}" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)
echo "T3 job: ${JOB3_ID}"
for i in $(seq 1 60); do
    sleep 5
    STATUS=$(ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
        "pct exec 300 -- python3 -c \"import urllib.request; \
         print(urllib.request.urlopen('http://localhost:5000/api/job/${JOB3_ID}',timeout=5).read().decode())\"" \
        | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
    echo "  T3 status: ${STATUS}"
    [ "${STATUS}" = "done" ] && { echo "T3 klar"; break; }
    [ "${STATUS}" = "error" ] && { echo "ERROR: T3 misslyckades"; exit 1; }
done

# ── SEED: Steg 3d — Ghost PBS-backup för ct/399 (in_pve=False-scenario) ──────
# Strategi: skapa tillfällig LXC 399, ta PBS-backup via vzdump, förstör LXC.
# PBS-snapshot kvarstår men ct/399 finns ej i PVE → in_pve=False i GUI.
echo "=== Seed: Skapar ghost PBS-backup för ct/399 (in_pve=False) ==="
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} bash << 'GHOST_PBS'
set -e

# Rensa eventuell gammal ct/399
pct status 399 >/dev/null 2>&1 && {
    pct stop 399 --skiplock 1 2>/dev/null || true
    sleep 2
    pct destroy 399 --purge 1 2>/dev/null || true
}

# Hitta tillgänglig CT-mall i local-lagring
TEMPLATE=$(pvesm list local --content vztmpl 2>/dev/null | awk 'NR>1 {print $1; exit}')
if [ -z "${TEMPLATE}" ]; then
    echo "Ingen mall i local — laddar ner Alpine..."
    pveam update 2>/dev/null || true
    TNAME=$(pveam available --section system 2>/dev/null | grep -i alpine | head -1 | awk '{print $2}')
    [ -z "${TNAME}" ] && { echo "ERROR: ingen CT-mall tillgänglig"; exit 1; }
    pveam download local "${TNAME}" 2>&1 | tail -3
    TEMPLATE=$(pvesm list local --content vztmpl 2>/dev/null | awk 'NR>1 {print $1; exit}')
fi
echo "Använder mall: ${TEMPLATE}"

# Skapa minimal LXC 399 (stoppad — vzdump fungerar på stoppade containers)
pct create 399 "${TEMPLATE}" \
    --storage local-lvm --rootfs local-lvm:0.1 \
    --memory 64 --hostname ghost-399-ci \
    --net0 name=eth0,bridge=vmbr0 2>&1 || \
pct create 399 "${TEMPLATE}" \
    --storage local --rootfs local:0.1 \
    --memory 64 --hostname ghost-399-ci \
    --net0 name=eth0,bridge=vmbr0 2>&1
echo "Skapade ct/399"

# Säkerhetskopiera ct/399 till PBS-lagring
NODE=$(hostname)
UPID=$(pvesh create /nodes/${NODE}/vzdump \
    --vmid 399 --storage pbs-local \
    --mode stop --compress zstd --remove 0 \
    --output-format json 2>/dev/null | \
    python3 -c "import sys,json; print(json.load(sys.stdin).get('data',''))" 2>/dev/null || echo "")

if [ -z "${UPID}" ]; then
    echo "ERROR: vzdump returnerade inget UPID för ct/399"
    pct destroy 399 --purge 1 2>/dev/null || true
    exit 1
fi
echo "vzdump-task: ${UPID}"

# Vänta på att vzdump-task är klar (upp till 3 min)
for i in $(seq 1 60); do
    ENCODED=$(python3 -c "import urllib.parse; print(urllib.parse.quote('${UPID}',safe=''))")
    TSTATUS=$(pvesh get /nodes/${NODE}/tasks/${ENCODED}/status \
        --output-format json 2>/dev/null | \
        python3 -c "import sys,json; d=json.load(sys.stdin).get('data',{}); print(d.get('exitstatus','') if d.get('status')=='stopped' else 'running')" 2>/dev/null || echo "unknown")
    [ "${TSTATUS}" = "OK" ] && { echo "  Ghost-backup klar (steg $i)"; break; }
    [ "${TSTATUS}" = "running" ] && { sleep 3; continue; }
    echo "  backup-status: ${TSTATUS} (steg $i/60)"; sleep 3
done

# Förstör ct/399 — PBS-snapshot finns kvar, LXC är borta → in_pve=False
pct destroy 399 --purge 1 2>&1 || pct destroy 399 2>&1 || true
echo "Ghost ct/399: LXC förstörd, PBS-snapshot kvarstår (in_pve=False redo)"
GHOST_PBS

# ── SEED: Steg 4 — Ta bort T1 (äldst PBS-snapshot) → cloud-only ──────────────
echo "=== Seed: Tar bort T1 från PBS (skapar cloud-only scenario) ==="
cat > /tmp/delete_oldest_snap.py << 'PYEOF'
"""Tar bort äldsta PBS-snapshot för ct/301 → cloud-only restic-entry."""
import json, ssl, urllib.parse, urllib.request

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

with open('/opt/proxmox-backup-gui/backend/hosts.json') as f:
    host = json.load(f)[0]

base = host['pbs_url'].rstrip('/')
ds   = host['pbs_datastore']
user = host['pbs_user']
pw   = host['pbs_password']

hdrs = {}
if '!' in user:
    hdrs['Authorization'] = f'PBSAPIToken {user}:{pw}'
else:
    data = urllib.parse.urlencode({'username': user, 'password': pw}).encode()
    d = json.loads(urllib.request.urlopen(
        urllib.request.Request(f'{base}/api2/json/access/ticket', data=data),
        context=ctx, timeout=10).read())['data']
    hdrs['Cookie'] = f'PBSAuthCookie={d["ticket"]}'
    hdrs['CSRFPreventionToken'] = d['CSRFPreventionToken']

def api(url, method='GET'):
    req = urllib.request.Request(url, method=method)
    for k, v in hdrs.items(): req.add_header(k, v)
    body = urllib.request.urlopen(req, context=ctx, timeout=30).read()
    return json.loads(body) if body else {}

snaps = api(f'{base}/api2/json/admin/datastore/{ds}/snapshots?backup-type=ct&backup-id=301')['data']
snaps.sort(key=lambda s: s['backup-time'])
if len(snaps) < 2:
    raise SystemExit(f'Bara {len(snaps)} snapshot(s) — behöver >=2 för cloud-only')
t = snaps[0]['backup-time']
print(f'Tar bort äldsta PBS-snapshot ct/301/{t} ({len(snaps)} totalt)...')
api(f'{base}/api2/json/admin/datastore/{ds}/snapshots?backup-type=ct&backup-id=301&backup-time={t}', method='DELETE')
print('Borttagen — cloud-only restic-entry visas nu i GUI')
PYEOF

scp ${SSH_OPTS} -i ${SSH_KEY} /tmp/delete_oldest_snap.py root@${VM_IP}:/tmp/
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "pct push 300 /tmp/delete_oldest_snap.py /tmp/delete_oldest_snap.py && \
     pct exec 300 -- python3 /tmp/delete_oldest_snap.py"

# ── SEED: Steg 5 — Rensa ct/300 PBS-snapshots via PBS API ────────────────────
echo "=== Seed: Rensar eventuella ct/300 PBS-snapshots via API ==="
cat > /tmp/purge_ct300_snaps.py << 'PYEOF'
"""Tar bort alla PBS-snapshots för ct/300 — restore-tester riktar sig mot ct/301."""
import json, ssl, urllib.parse, urllib.request

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

with open('/opt/proxmox-backup-gui/backend/hosts.json') as f:
    host = json.load(f)[0]

base = host['pbs_url'].rstrip('/')
ds   = host['pbs_datastore']
user = host['pbs_user']
pw   = host['pbs_password']

hdrs = {}
if '!' in user:
    hdrs['Authorization'] = f'PBSAPIToken {user}:{pw}'
else:
    data = urllib.parse.urlencode({'username': user, 'password': pw}).encode()
    d = json.loads(urllib.request.urlopen(
        urllib.request.Request(f'{base}/api2/json/access/ticket', data=data),
        context=ctx, timeout=10).read())['data']
    hdrs['Cookie'] = f'PBSAuthCookie={d["ticket"]}'
    hdrs['CSRFPreventionToken'] = d['CSRFPreventionToken']

def api(url, method='GET'):
    req = urllib.request.Request(url, method=method)
    for k, v in hdrs.items(): req.add_header(k, v)
    body = urllib.request.urlopen(req, context=ctx, timeout=30).read()
    return json.loads(body) if body else {}

try:
    snaps = api(f'{base}/api2/json/admin/datastore/{ds}/snapshots?backup-type=ct&backup-id=300')['data']
except Exception as e:
    print(f'Inga ct/300-snapshots (eller fel): {e}'); snaps = []

for s in snaps:
    t = s['backup-time']
    api(f'{base}/api2/json/admin/datastore/{ds}/snapshots?backup-type=ct&backup-id=300&backup-time={t}', method='DELETE')
    print(f'Tog bort PBS-snapshot ct/300/{t}')

print(f'Rensade {len(snaps)} ct/300-snapshot(s)')
PYEOF

scp ${SSH_OPTS} -i ${SSH_KEY} /tmp/purge_ct300_snaps.py root@${VM_IP}:/tmp/
ssh ${SSH_OPTS} -i ${SSH_KEY} root@${VM_IP} \
    "pct push 300 /tmp/purge_ct300_snaps.py /tmp/purge_ct300_snaps.py && \
     pct exec 300 -- python3 /tmp/purge_ct300_snaps.py"

# ── Klar ──────────────────────────────────────────────────────────────────────
echo ""
echo "=========================================================="
echo " Dev-VM redo!  Seedad state:"
echo "   T1: cloud-only  (täcks av R1 + R2)"
echo "   T2: local+cloud (täcks av R2)"
echo "   T3: local-only  (ingen restic-täckning)"
echo "   ct/399: ghost PBS-snap (in_pve=False, ingen LXC 399 i PVE)"
echo ""
echo " GUI-åtkomst (öppna ny terminal):"
echo "   ssh -L 5000:10.10.0.100:5000 root@${VM_IP}"
echo "   open http://localhost:5000"
echo ""
echo " Iterera:"
echo "   bash ci/dev-vm.sh deploy   # ny kod + kör tester"
echo "   bash ci/dev-vm.sh test     # bara kör tester"
echo "   bash ci/dev-vm.sh destroy  # ta bort VM"
echo "=========================================================="
