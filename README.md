# proxmox-backup-gui

A self-hosted web dashboard for monitoring Proxmox Backup Server (PBS) and restic cloud backups.

Shows per-VM backup status, local/cloud coverage, storage usage, and historical snapshots in a clean dark UI.

## Features

- **Per-VM/LXC overview** — snapshot count, latest backup age, local/cloud coverage badges
- **Cloud detection** — marks PBS snapshots as cloud-covered if restic ran after them
- **Cloud-only snapshots** — shows older restic backups that no longer exist locally
- **Storage meters** — PBS local usage and Google Drive family quota
- **Backup folder size** — actual restic repo size via `rclone size`
- **Backup now** — trigger a PBS backup of any VM/LXC directly from the GUI; live log in job modal
- **Restore** — restore any VM/LXC from a PBS snapshot (local) or from a restic snapshot (cloud); restic operations run on the PVE host via SSH, never inside the GUI container
- **Multi-host** — configure multiple PVE/PBS hosts in `hosts.json`
- **HA integration** — `/api/host/<id>/ha/sensors` endpoint for the [proxmox-backup-ha](https://github.com/d96moe/proxmox-backup-ha) integration

## Architecture

```
PVE/PBS ──────────────────────────► pbs_client.py ──┐
PVE API ──────────────────────────► pve_client.py ──┤
restic/rclone (Google Drive) ──────► restic_client.py ──┤
                                                     └──► Flask app.py ──► index.html
```

Runs as a systemd service inside an LXC container (LXC 199 by default).

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

Edit `/opt/proxmox-backup-gui/backend/hosts.json` on the LXC:

```json
[
  {
    "id": "home",
    "label": "pve · home",
    "pbs_host": "192.168.0.200",
    "pbs_user": "backup@pbs",
    "pbs_password": "your-pbs-password",
    "pbs_datastore": "local-store",
    "pve_host": "192.168.0.200",
    "pve_user": "root@pam",
    "pve_password": "your-pve-password",
    "restic_repo": "rclone:gdrive:bu/proxmox_home",
    "restic_password": "your-restic-password"
  }
]
```

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

## Related

- [proxmox-backup-restore](https://github.com/d96moe/proxmox-backup-restore) — PBS + restic setup and disaster recovery scripts
- [proxmox-backup-ha](https://github.com/d96moe/proxmox-backup-ha) — Home Assistant integration
