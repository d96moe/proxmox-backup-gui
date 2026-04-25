# CI & Testing

## Contents

- [Overview](#overview)
- [How It Works](#how-it-works)
- [Test Infrastructure](#test-infrastructure)
- [What the Tests Verify](#what-the-tests-verify)
- [Jenkins Setup](#jenkins-setup)
- [Credentials and Secrets](#credentials-and-secrets)

---

## Overview

Two Jenkins pipelines test the GUI at different levels:

| Pipeline | Jenkins job | Jenkinsfile | Trigger | What it tests |
|---|---|---|---|---|
| Mock / fast | `proxmox-backup-gui` | `ci/Jenkinsfile` | Every push (webhook) | Playwright against mock server + MQTT unit tests, ~60s, no VM |
| Integration | `proxmox-backup-gui-integration` | `ci/Jenkinsfile.integration` | Nightly ~23:00 + manual | Playwright against real Flask + PBS + Google Drive + MQTT unit tests, ~20 min |

**Integration test suites** (selectable via `TEST_SUITE` parameter):

| Suite | Files | Description |
|---|---|---|
| `all` | `tests/` | All test files |
| `frontend` | `test_frontend.py` | Playwright mock tests only |
| `restore` | `test_restore.py` | Integration backup/restore tests |
| `self_restore` | `test_self_restore.py` | Datastore self-restore tests |
| `mqtt` | `test_mqtt.py` | MQTT agent unit tests only |

The fast pipeline gives rapid feedback on every push. The integration pipeline verifies end-to-end behaviour against a real Proxmox environment.

---

## How It Works

### Fast pipeline

Runs entirely inside Jenkins LXC 200. No VM, no network calls to Proxmox or Google Drive.

1. Checks out the branch
2. Installs Python deps + Playwright Chromium
3. Runs `ci/tests/test_frontend.py` against a configurable in-process mock HTTP server

### Integration pipeline

Spins up a fresh clone of template 9092 (PVE + LXC 300 with Flask GUI + minimal PBS) for each build.

1. Clones template 9092 → fresh VM at `192.168.0.250`
2. Deploys the latest code (backend + frontend + tests) to the VM
3. Seeds backup data: PBS backup of LXC 301, restic snapshot to Google Drive
4. Runs `ci/tests/test_frontend.py` + `ci/tests/test_restore.py` against the real Flask backend at `http://10.10.0.100:5000`
5. Destroys the VM (always, even on failure)

Every build starts from a clean slate. Nothing touches the production LXC 199.

---

## Test Infrastructure

```
Physical x86_64 host (PVE, 192.168.0.200)
    ├── LXC 200                — Jenkins agent (runs both pipelines)
    └── VM (clone of 9092)     — fresh each integration build
            ├── PVE host node (192.168.0.250 / internal 10.10.0.1)
            ├── LXC 300        — Flask GUI (10.10.0.100:5000)
            ├── LXC 301        — restore target (clone of 300, stopped)
            └── PBS            — local datastore at /mnt/ci-pbs
```

Template 9092 must be created once — see [Jenkins Setup](#jenkins-setup).

---

## What the Tests Verify

### Mock tests (`ci/tests/test_frontend.py`)

Playwright tests against an in-process mock server. Cover:

- **DEPLOY** — correct JS is being served (no stale builds)
- **DOM** — required elements exist, no null-dereference JS errors on load/switch
- **HAPPY** — normal data rendering (VMs, LXCs, storage meters, snapshot counts)
- **ERROR** — backend failures render error states, not infinite Loading…
- **RACE** — rapid host switching, stale-data prevention, AbortController usage
- **EDGE** — empty data, LXC-only, no cloud storage
- **CONCURRENCY** — multiple simultaneous users from different browser contexts
- **MQTT BUS** — host isolation (`_onMessage` filter drops messages from non-current hosts); `resubscribe()` on host switch sends a `replay` request to `/mqtt-ws` so the Flask proxy re-delivers retained messages for the new host without JS errors
- **SORT ORDER** — VM cards appear in ascending VMID order; LXC cards in ascending VMID order; VM section always before LXC section regardless of MQTT message arrival order

### Unit tests (`ci/tests/test_mqtt.py`)

Pure Python unit tests (no VM, no network). Run in both the fast pipeline and as the `mqtt` suite in the integration pipeline. Cover the MQTT agent logic:

- **STORAGE_MERGE** — `_scan_storage` preserves `cloud_used`/`cloud_total` across scans
- **RESTIC_COVERS** — PBS snapshots annotated with `local`/`cloud` flags and `pbs_date` for hover tooltips
- **RESTIC_RESCAN** — `_run_in_background` triggers `_scan_restic` after restic backup (not just `rescan_now`)
- **BACKUP_PROGRESS** — `LocalResticClient.backup_datastore` parses `--json` output into readable log lines
- **MQTT_TOPIC_BASE** — `MQTTPublisher._base` uses `mqtt_hostname` from config, not OS hostname
- **RESTIC_PRUNE** — pruned snapshot IDs absent after rescan; newest restic wins for same PBS time
- **RESTIC_ID_FORMAT** — `restic_id` = full 64-char hash (for API); `restic_short_id` = 8-char display
- **CLOUD_ONLY** — PBS entries with `local=False, cloud=True` appear for pruned-locally times
- **TIMESTAMP_FIELDS** — cloud-only entries have `date`; restic covers have `pbs_date` for tooltips
- **RESTIC_ONLY_VM** — VM deleted from PVE+PBS but still in restic appears as cloud-only (regression: was invisible because `all_vmids` excluded restic-only VMIDs)

### Integration tests (`ci/tests/test_restore.py` + `ci/tests/test_frontend.py`)

Playwright + `requests` against the real Flask backend. Cover:

- **Backup now** — trigger PBS backup of a VM/LXC via GUI, watch job modal, verify snapshot appears
- **Local restore** — restore LXC 301 from PBS snapshot via GUI, verify job completes
- **Cloud restore** — restore PBS datastore from restic/Google Drive snapshot, restart PBS, verify snapshots
- **Cloud restore + backup after** — full flow: restore from cloud, re-backup PBS, verify new snapshot
- **Concurrent backups** — two backup jobs triggered simultaneously both get job IDs
- **Settings — restic retention** — GET/POST roundtrip, partial update, unknown keys ignored, type validation (400), isolation
- **Settings — PBS prune policy** — GET/POST roundtrip, all keep-* keys, clear-key deletes from PBS live, zero=unset, empty retention clears all, unknown job id (500), non-integer (400), green path (POST→GET→live PBS verify via SSH)
- **Settings — schedules** — PBS schedule roundtrip + verified via `pvesh` on PVE host; restic schedule roundtrip + verified in timer file via SSH; error cases
- **Settings — VM selection** — exclude mode (all=1 + exclude field), include mode (all=0 + vmid field), mode switch, PVE verification, error cases
- **PBS tasks** — GC, external backup, and prune triggered via SSH to PVE host; task visible in `/pbs/tasks` API; running card appears in sidebar; GUI backup suppresses duplicate PBS card for same VMID
- **Restic nightly log** — API returns lines + running flag; seeded log file used for SSE stream tests; `__done__` emitted when no process active; `openResticLogModal()` opens job modal with log content

---

## Jenkins Setup

### Prerequisites

- Jenkins running in LXC 200 on the PVE host
- Jenkins Pipeline plugin installed
- Template VM 9092 created (see below)
- SSH key pair in LXC 200 at `/var/lib/jenkins/.ssh/id_ed25519`, public key on PVE host's `authorized_keys`

**Adapt IPs to your network:**

| Variable | Where | Default (this repo) | What to set |
|---|---|---|---|
| `PVE_HOST` | Both Jenkinsfiles | `192.168.0.200` | Your PVE host IP |
| `VM_IP` | `Jenkinsfile.integration` env block | `192.168.0.250` | Free IP for the CI VM |
| `BACKEND_URL` | `Jenkinsfile.integration` run-tests stage | `http://10.10.0.100:5000` | Flask URL inside CI VM |

### mqtt_hostname alignment

The pve-agent `config.json` key `mqtt_hostname` **must match** the `id` field in `hosts.json` on LXC 300. The pipeline writes both — if they diverge, the browser's WebSocket replay request (`proxmox/<host-id>`) won't match any retained MQTT topics (`proxmox/<mqtt_hostname>/...`), VM cards never render, and all `test_restore.py` UI tests will timeout.

In `Jenkinsfile.integration`, the DEPLOY_AGENT block writes:
```python
'mqtt_hostname': 'ci',   # must match hosts[0].id in hosts.json on LXC 300
```

### Template 9092

The integration pipeline clones template 9092 for each build. Create it once by hand (or document your setup script here). The template must have:

- PVE installed
- LXC 300 running the GUI (`/opt/proxmox-backup-gui/`)
- LXC 301 as a stopped clone of 300 (restore target)
- PBS datastore at `/mnt/ci-pbs`
- rclone + restic configured with access to Google Drive CI path (`bu/gui-ci-test`)
- SSH access from Jenkins LXC 200 as root

### Job configuration

Set the **Script Path** in each Jenkins job:

| Job | Script Path |
|---|---|
| `proxmox-backup-gui` | `ci/Jenkinsfile` |
| `proxmox-backup-gui-integration` | `ci/Jenkinsfile.integration` |

---

## Credentials and Secrets

| What | Jenkins credential ID | How CI gets it |
|---|---|---|
| restic repository password | `gui-ci-restic-password` | Jenkins secret text — injected as `$RESTIC_PASSWORD` at runtime, written into `hosts.json` on the CI VM by the pipeline |
| PBS GUI test user password | `gui-ci-pbs-password` | Jenkins secret text — injected as `$PBS_CI_PASSWORD` at runtime, used to configure `pbs-local` storage on the CI VM |
| rclone OAuth token (`rclone.conf`) | — | Relayed at runtime from prod PVE host (`cat /root/.config/rclone/rclone.conf` → CI VM via SSH pipe) — never stored in Jenkins or git |
| rclone + restic binaries | — | Relayed at runtime from prod PVE host (same SSH pipe approach) — keeps CI VM on the same binary version as prod |
| SSH private key | — | LXC 200 at `/var/lib/jenkins/.ssh/id_ed25519` — never in git |
| PBS + PVE host credentials | — | Baked into `hosts.json` on template 9092 LXC 300 — restic password and restic repo overwritten by pipeline at runtime |

### Adding the Jenkins credentials

In **Jenkins → Manage Jenkins → Credentials → (global)**:

1. **`gui-ci-restic-password`** — Kind: Secret text, value: restic password for `rclone:gdrive:bu/gui-ci-test`
2. **`gui-ci-pbs-password`** — Kind: Secret text, value: password for `gui-test@pbs` on the CI PBS instance

No other secrets need to be registered — everything else is relayed from the prod PVE host or baked into template 9092.

### Jenkins LXC 200 resources

The Playwright tests are CPU and memory intensive. LXC 200 must have at least:

- **6 cores**
- **6 GB RAM**

With less, Jenkins may OOM mid-run or time out before the tests finish.
