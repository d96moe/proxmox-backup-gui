# proxmox-backup-gui

A self-hosted web dashboard for monitoring Proxmox Backup Server (PBS) and restic cloud backups.

Shows per-VM backup status, local/cloud coverage, storage usage, and historical snapshots in a clean dark UI.

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
| **Mosquitto MQTT broker** | Installed in LXC 199 by `setup-lxc.sh`; listens on TCP :1883 (agents) and WebSocket :9001 (browser) |
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
- **Multi-host** — monitor multiple PVE/PBS hosts simultaneously in one UI
- **Real-time updates** — browser receives live data via MQTT WebSocket; no polling

## Architecture

The GUI is **MQTT-driven**: the browser never polls the backend for VM/snapshot data. A lightweight agent (`pve_agent.py`) on each PVE host publishes retained MQTT messages to a Mosquitto broker co-located in LXC 199. The browser subscribes via WebSocket and renders the UI directly from those retained messages — full data in < 200 ms on page load, live updates as agents publish. There is no REST polling path for VM/snapshot data.

```
PVE host (home, 192.168.0.200)
  └── pve_agent.py  ──MQTT──► Mosquitto :1883 ◄──── pve_agent.py (Pi5 / cabin)
                                   │
                               LXC 199
                          ┌────────┴────────┐
                          │  Mosquitto :9001 │  ◄── browser (WebSocket)
                          │  Flask :5000     │  ◄── browser (HTTP login + job API)
                          └─────────────────┘
```

### MQTT topics

Each agent publishes under `proxmox/<mqtt_hostname>/`:

| Topic suffix | Content |
|---|---|
| `vm/<id>/meta` | VM/LXC name, type, status |
| `vm/<id>/pbs` | PBS snapshots with `local`/`cloud` flags and `restic_id` |
| `vm/<id>/restic` | Restic snapshots with per-VM covers and `pbs_date` |
| `vms/index` | Sorted list of all VMIDs |
| `storage` | Local PBS usage + cloud quota |
| `schedules` | PBS job and restic retention config |
| `state/ready` | Agent health heartbeat |
| `agent/status` | LWT online/offline |

> **`mqtt_hostname` must match the `id` field in `hosts.json`.**  
> The GUI subscribes to `proxmox/<id>/#`. If the agent's `mqtt_hostname` is different (e.g. the OS hostname), the GUI shows "Connecting…" forever. Set `mqtt_hostname` explicitly in the agent's `config.json`.

## Deployment

### First-time setup

Run on your PVE host as root:

```bash
git clone https://github.com/d96moe/proxmox-backup-gui.git /tmp/proxmox-backup-gui
cd /tmp/proxmox-backup-gui
bash setup-lxc.sh
```

This creates LXC 199 with Debian 12, installs dependencies, deploys the app and registers a systemd service. The GUI is then available at `http://192.168.0.50:5000`.

Override defaults with env vars:

```bash
LXC_ID=200 LXC_IP=192.168.0.51 bash setup-lxc.sh
```

### Updating

```bash
bash update-lxc.sh
```

Pushes updated backend + frontend files to the LXC and restarts the service.

## Configuration

### `hosts.json` (in LXC 199)

Edit `/opt/proxmox-backup-gui/backend/hosts.json`:

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

## CI & Testing

See [ci/README.md](ci/README.md) for the full CI setup — two Jenkins pipelines (fast mock tests on every push, nightly integration tests against a real Proxmox VM).

## Restic conflict avoidance

The GUI uses `rclone lsjson locks/` to check if a restic backup is in progress before making restic calls. Locks older than 8 hours are treated as stale (from crashed backups) and ignored.

## Roadmap

- **Authentication** — login page with hashed credentials; currently the GUI is open to anyone who can reach the LXC
- **VM/LXC backup mask** — per-VM include/exclude toggles so not every container is backed up to cloud
- **Prune / retention settings** — UI for both PBS retention (`prune-backups` per storage in PVE) and restic `--keep-last / --keep-daily / --keep-weekly`; both written to PVE host config via SSH
- **Backup scheduler** — view and edit schedules for both PBS (vzdump) and restic (cloud) jobs; currently both are configured statically on the PVE host via systemd timers / cron outside the GUI
- **Delete backup (cloud)** — guided workflow to remove a specific VM's backup from the restic repo: restore full datastore → delete from PBS → re-backup → forget old snapshot. Expensive but correct given the whole-datastore restic architecture.
- **Restic prune after sync** — after a successful `☁ sync`, automatically run `restic forget --prune` according to configured retention policy; currently pruning is handled outside the GUI

## Related

- [proxmox-backup-restore](https://github.com/d96moe/proxmox-backup-restore) — PBS + restic setup and disaster recovery scripts; sets up the environment this GUI expects
- [proxmox-backup-ha](https://github.com/d96moe/proxmox-backup-ha) — Home Assistant integration using the `/api/host/<id>/ha/sensors` endpoint
