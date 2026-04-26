# proxmox-backup-gui

A self-hosted web dashboard for monitoring Proxmox Backup Server (PBS) and restic cloud backups.

Shows per-VM backup status, local/cloud coverage, storage usage, and historical snapshots in a clean dark UI. Includes a settings modal for retention policies, backup schedules, and VM selection — all written back to the PVE host via the agent.

![til](./p-b-g_ui.gif)

> **⚠️ HOBBY PROJECT — USE AT YOUR OWN RISK**
>
> This is a personal homelab project built for fun and convenience. The code, the CI pipelines, the tests, and this README were all written with Claude Code assistance. It is not production software, has no guarantees, and comes with no support. It works on my hardware — it may or may not work on yours. If you use this and something goes wrong, that's on you.
>
> The GUI can trigger real backup and restore operations on your Proxmox host. A restore from cloud will stop PBS, overwrite your local datastore, and restart PBS. Make sure you understand what each operation does before using it.

## Relationship to proxmox-backup-restore

This GUI is designed to work alongside **[proxmox-backup-restore](https://github.com/d96moe/proxmox-backup-restore)** — a set of scripts that set up PBS local backups and restic cloud backups on a Proxmox host. If you have installed and configured that project, this GUI will work out of the box.

You can use this GUI without those scripts, but you must replicate the same environment on your PVE host (see [Prerequisites](#prerequisites) below).

## Prerequisites

The GUI LXC talks to your PVE host over both the API and SSH. The following must be in place before deploying:

### In LXC 199 (installed automatically by `setup-lxc.sh`)

| Requirement | Notes |
|---|---|
| **Mosquitto MQTT broker** | Installed in LXC 199 by `setup-lxc.sh`; listens on TCP :1883 (agents only — browser never connects directly) |
| Flask + Python deps | Installed via pip into a venv |

### On each PVE host

| Requirement | Notes |
|---|---|
| Proxmox VE 8+ with PBS | PBS must be running with at least one datastore configured |
| `pve_agent.py` running | The agent publishes data to Mosquitto; install from this repo |
| `paho-mqtt` Python library | Required by the agent; install with `pip install paho-mqtt` |
| `restic` binary | Required for cloud backup/restore; must be in `$PATH` |
| `rclone` binary | Required for cloud features; must be in `$PATH` |
| rclone configured | A remote (`rclone.conf`) with a path for the restic repo |
| restic repo initialized | `restic -r rclone:<remote>:<path> init` must have been run |

### Optional (cloud features only)

Cloud backup, cloud restore, and ☁ sync are skipped gracefully if `restic_repo` / `restic_password` are omitted from the agent config. Local PBS features work without restic/rclone.

## Features

- **Per-VM/LXC overview** — snapshot count, latest backup age, local/cloud coverage badges
- **Cloud detection** — marks PBS snapshots as cloud-covered if restic ran after them
- **Cloud-only snapshots** — shows older restic backups that no longer exist locally in PBS
- **Storage meters** — PBS local usage and Google Drive family quota
- **Backup now** — trigger a PBS-only or PBS+cloud backup of any VM/LXC; live log in job modal
- **☁ sync** — trigger a standalone restic cloud sync from any VM card or snapshot row
- **Restore** — restore from a PBS snapshot (local) or restic snapshot (cloud)
- **Job indicator** — running jobs show a pulsing indicator on the VM card; click to reopen progress modal
- **PBS task visibility** — running PBS operations (GC, external backup, prune) appear as clickable cards in the sidebar; click to view live log stream
- **Restic nightly log streaming** — when the nightly restic job is running the sidebar badge is clickable; opens a modal that replays existing log lines and streams new lines live via SSE until the job finishes
- **Settings modal** — editable per-host settings written back to the PVE host via agent:
  - Restic cloud retention (keep-last / keep-daily / keep-weekly / keep-monthly)
  - PBS prune policy (keep-last / keep-daily / keep-weekly / keep-monthly / keep-yearly)
  - PBS backup schedule and restic backup schedule (systemd OnCalendar)
  - VM/LXC backup selection — exclude-list (backup all, mark exclusions) or include-list (only named VMs)
- **Authentication** — login page with bcrypt-hashed credentials; role system (admin / viewer)
- **Multi-host** — monitor multiple PVE/PBS hosts simultaneously in one UI
- **Real-time updates** — browser receives live data via MQTT WebSocket; no polling

## Architecture

The GUI is **MQTT-driven**: the browser never polls the backend for VM/snapshot data. A lightweight agent (`pve_agent.py`) on each PVE host publishes retained MQTT messages to a Mosquitto broker co-located in LXC 199. Flask proxies those messages to the browser over a WebSocket at `/mqtt-ws` — the browser connects to Flask only on one port, never to Mosquitto directly. Full data appears in < 200 ms on page load; live updates arrive as agents publish. There is no REST polling path for VM/snapshot data.

```
PVE host (home, 192.168.0.200)
  └── pve_agent.py  ──MQTT──► Mosquitto :1883 ◄──── pve_agent.py (Pi5 / cabin)
                                   │
                               LXC 199
                          ┌────────┴────────┐
                          │  Flask :5000     │  ◄── browser (HTTP + WebSocket /mqtt-ws)
                          │  (MQTT proxy)    │
                          └─────────────────┘
```

### MQTT topics

Each agent publishes under `proxmox/<mqtt_hostname>/`. The GUI subscribes to `proxmox/+/#` (all hosts, all topics) and filters by `currentHost` in `_onMessage`.

#### Published by agent → received by browser

| Topic suffix | Retained | Content |
|---|---|---|
| `agent/status` | yes | `{"status": "online"}` — LWT; broker publishes `"offline"` on disconnect |
| `state/ready` | yes | `{"ts": …, "pve_ok": true, "pbs_ok": true}` — agent health heartbeat |
| `vms/index` | yes | Sorted list of all VMIDs: `["101", "200", "301"]` |
| `vm/<id>/meta` | yes | `{vmid, name, type, status, os, template, in_pve}` |
| `vm/<id>/pbs` | yes | `{snapshots: [{backup_time, date, local, cloud, incremental, size, size_bytes, restic_id, restic_short_id}]}` — cloud-only entries have `local=false, cloud=true` |
| `vm/<id>/restic` | yes | `{snapshots: [{id, short_id, time, covers: [{vmid, pbs_time, pbs_date, local, cloud}]}]}` |
| `vm/<id>/backup/progress` | no | `{line: "…"}` — live log lines during a backup job |
| `storage` | yes | `{local_used, local_total, cloud_used, cloud_total, cloud_quota_used, dedup_factor}` |
| `info` | yes | `{pbs: "4.1.4", pve: "9.0.0", restic: "0.18.0"}` — version strings |
| `schedules` | yes | `{pbs_jobs, pbs_running, restic_next, restic_running, pbs_retention, restic_retention}` |
| `pbs/tasks/running` | yes | `[{upid, worker_type, worker_id, starttime}]` — running PBS tasks (GC, backup, prune); polled every 15 s |
| `job/<corr_id>/ack` | no | `{op_id: "…"}` — correlation-ID response after the agent accepts a command |

#### Published by browser → received by agent

| Topic suffix | Content |
|---|---|
| `cmd/rescan` | `{}` — request a full rescan and republish |
| `cmd/backup/pbs` | `{vmid, vmtype, corr_id}` — trigger PBS backup of a VM/LXC |
| `cmd/backup/restic` | `{vmid, corr_id}` — trigger restic cloud sync |
| `cmd/restore` | `{vmid, vmtype, backup_time, source, backup_after, corr_id}` — trigger restore |

> **`mqtt_hostname` must match the `id` field in `hosts.json`.**  
> If the agent's `mqtt_hostname` is different (e.g. the OS hostname), the GUI shows "Connecting…" forever. Set `mqtt_hostname` explicitly in the agent's `config.json` and verify with `journalctl -u pve-agent` on the PVE host.

## Deployment

The GUI is a plain Python/Flask app — deploy it wherever makes sense for your setup. `setup-lxc.sh` is a convenience script for the common case of running it in a Proxmox LXC, but you can also run it in a VM, on bare-metal, in Docker, or anywhere else that can reach your PVE hosts over the network.

### Option A — LXC (quickstart)

Run on your PVE host as root:

```bash
git clone https://github.com/d96moe/proxmox-backup-gui.git /tmp/proxmox-backup-gui
cd /tmp/proxmox-backup-gui
bash setup-lxc.sh
```

Creates LXC 199 with Debian 12, installs Mosquitto + Flask, deploys the app, creates an initial admin user, and registers a systemd service. Override defaults with env vars:

```bash
LXC_ID=200 LXC_IP=192.168.0.51/24 LXC_GW=192.168.0.1 bash setup-lxc.sh
```

The script prints the login credentials and a `hosts.json` snippet at the end.

### Option B — Anywhere else

Requirements: Python 3.11+, Mosquitto (or any MQTT broker), network access to your PVE hosts.

```bash
git clone https://github.com/d96moe/proxmox-backup-gui.git
cd proxmox-backup-gui
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# Create an initial admin user
bash add-user.sh admin admin

# Edit config, then:
PORT=5000 MQTT_WS_URL=ws://<mosquitto-host>:9001 .venv/bin/python backend/app.py
```

### Step 2 — Install the agent (each PVE host)

Run on each PVE host you want to monitor:

```bash
cd /tmp/proxmox-backup-gui
bash setup-agent.sh
```

Prompts for the Mosquitto broker IP and a host ID (must match `id` in `hosts.json`). Installs the agent to `/opt/pve-agent/`, writes a config template to `/etc/pve-agent/config.json`, and registers a systemd service.

```bash
nano /etc/pve-agent/config.json   # fill in PBS/PVE passwords, pbs_datastore, etc.
systemctl start pve-agent
journalctl -u pve-agent -f
```

### Step 3 — Connect agent to GUI

Edit `hosts.json` (wherever the GUI is running) and add the agent. The `setup-agent.sh` script prints the exact JSON snippet to paste. Restart the GUI service.

### Adding users

Run `add-user.sh` on the machine where the GUI is installed:

```bash
bash add-user.sh alice           # viewer (read-only)
bash add-user.sh bob admin       # admin (full access)
```

### Updating

```bash
bash update-lxc.sh               # if using the LXC setup
```

For other deployments: pull the repo, copy the changed files, restart the service. Re-run `setup-agent.sh` on each PVE host to update the agent.

## Configuration

### `hosts.json`

Edit `backend/hosts.json` in your GUI install directory:

```json
[
  {
    "id": "home",
    "label": "pve · home",
    "agent_url": "http://192.168.0.200:8099",
    "agent_token": "your-token"
  },
  {
    "id": "cabin",
    "label": "pve · cabin",
    "agent_url": "http://192.168.1.200:8099",
    "agent_token": "your-token"
  }
]
```

The `id` field **must match** `mqtt_hostname` in the agent's `config.json` on each PVE host.

### Agent `config.json` (on each PVE host)

`/etc/pve-agent/config.json`:

```json
{
  "pve_url":          "https://192.168.0.200:8006",
  "pve_user":         "root@pam",
  "pve_password":     "your-pve-password",
  "pbs_url":          "https://192.168.0.200:8007",
  "pbs_user":         "backup@pbs",
  "pbs_password":     "your-pbs-password",
  "pbs_datastore":    "local-store",
  "pbs_storage_id":   "pbs-local",
  "pbs_datastore_path": "/mnt/pbs",
  "restic_repo":      "rclone:gdrive:bu/proxmox_home",
  "restic_password":  "your-restic-password",
  "restic_env":       { "RCLONE_CONFIG": "/root/.config/rclone/rclone.conf" },
  "mqtt_host":        "192.168.0.50",
  "mqtt_port":        1883,
  "mqtt_hostname":    "home",
  "agent_token":      "your-token",
  "verify_ssl":       false
}
```

> `mqtt_hostname` must match the `id` in `hosts.json`. The agent logs the effective hostname at startup — check `journalctl -u pve-agent` if the GUI shows "Connecting…".

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/hosts` | List configured hosts |
| `GET /api/host/<id>/items` | VMs + LXCs with merged PBS + restic snapshots |
| `GET /api/host/<id>/storage` | PBS and Google Drive storage usage (async) |
| `GET /api/host/<id>/ha/sensors` | Flat sensor dict for Home Assistant |
| `GET /api/host/<id>/info` | PBS and restic version info |
| `GET /api/host/<id>/settings` | Read restic retention, PBS prune policy, schedules, VM selection |
| `POST /api/host/<id>/settings` | Write any combination of the above settings to the PVE host |
| `GET /api/host/<id>/pbs/tasks` | List PBS tasks (`?running=1` for running only) |
| `GET /api/host/<id>/pbs/tasks/<upid>/log` | Full log of a PBS task |
| `GET /api/host/<id>/pbs/tasks/<upid>/stream` | SSE stream of a running PBS task log |
| `GET /api/host/<id>/restic/log` | Last 500 lines of the restic nightly log + `running` bool |
| `GET /api/host/<id>/restic/log/stream` | SSE stream of the restic nightly log; emits `__done__` when the job finishes |
| `WS  /mqtt-ws` | MQTT proxy WebSocket — browser subscribes here instead of directly to Mosquitto; requires session cookie (login required) |

## CI & Testing

See [ci/README.md](ci/README.md) for the full CI setup — two Jenkins pipelines (fast mock tests on every push, nightly integration tests against a real Proxmox VM).

## Restic conflict avoidance

The GUI uses `rclone lsjson locks/` to check if a restic backup is in progress before making restic calls. Locks older than 8 hours are treated as stale (from crashed backups) and ignored.

## Roadmap

- **Delete backup (cloud)** — guided workflow to remove a specific VM's backup from the restic repo: restore full datastore → delete from PBS → re-backup → forget old snapshot. Expensive but correct given the whole-datastore restic architecture.
- **Host/connection settings** — PBS credentials, restic repo/password, agent URL editable in GUI (currently requires editing config files on the host).
- **Host/connection settings** — PBS credentials, restic repo/password, agent URL editable in GUI (currently requires editing config files on the host).

## Related

- [proxmox-backup-restore](https://github.com/d96moe/proxmox-backup-restore) — PBS + restic setup and disaster recovery scripts; sets up the environment this GUI expects
- [proxmox-backup-ha](https://github.com/d96moe/proxmox-backup-ha) — Home Assistant integration using the `/api/host/<id>/ha/sensors` endpoint
