"""Host configuration loaded from environment or config file."""
import os
import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class HostConfig:
    id: str
    label: str
    pve_url: str          # e.g. https://192.168.0.10:8006
    pbs_url: str          # e.g. https://192.168.0.10:8007
    pbs_user: str         # e.g. root@pam
    pbs_password: str
    pbs_datastore: str    # e.g. main
    restic_repo: str      # e.g. rclone:gdrive:pve-backups
    restic_password: str
    restic_env: dict = field(default_factory=dict)  # extra env vars for restic
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
