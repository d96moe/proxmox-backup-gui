"""Host configuration loaded from environment or config file."""
import os
import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class HostConfig:
    id: str
    label: str
    pve_url: str           # e.g. https://192.168.0.200:8006
    pve_user: str          # e.g. root@pam
    pve_password: str
    pbs_url: str           # e.g. https://192.168.0.200:8007
    pbs_user: str          # e.g. backup@pbs  or  user!tokenid for API token
    pbs_password: str
    pbs_datastore: str     # e.g. local-store
    pbs_storage_id: str = "pbs-local"   # PVE storage ID for PBS (used in vzdump/restore)
    pbs_datastore_path: str = "/mnt/pbs" # Local path to PBS datastore (for restic restore target)
    restic_repo: str = ""               # e.g. rclone:gdrive:bu/proxmox_home
    restic_password: str = ""
    restic_env: dict = field(default_factory=dict)
    verify_ssl: bool = False


def load_hosts() -> list[HostConfig]:
    """Load host configs from HOSTS_CONFIG env var (JSON) or hosts.json file."""
    raw = os.environ.get("HOSTS_CONFIG")
    if not raw:
        cfg_path = Path(__file__).parent / "hosts.json"
        if cfg_path.exists():
            raw = cfg_path.read_text()
        else:
            # Return empty list; caller should handle gracefully
            return []

    data = json.loads(raw)
    return [HostConfig(**h) for h in data]
