"""CI API helper — authenticates and makes Flask API calls from the seed stage.

Usage (inside LXC 300):
  python3 /tmp/ci_api.py backup <vmid> <type>
  python3 /tmp/ci_api.py job <job_id>

Called from Jenkinsfile.integration via pct exec 300 to avoid JSON quoting
hell when passing data through multiple shell layers (Groovy → bash → SSH → pct exec).
"""
import json
import http.cookiejar
import urllib.request
import sys

BASE = "http://localhost:5000"
_PW  = "proxmox-ci-2024"

# Authenticate — one login per invocation, session cookie kept in-process
_jar = http.cookiejar.CookieJar()
_o   = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_jar))
_o.open(urllib.request.Request(
    f"{BASE}/api/auth/login",
    data=json.dumps({"username": "admin", "password": _PW}).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
))


def _get(path: str) -> str:
    return _o.open(f"{BASE}{path}", timeout=60).read().decode()


def _post(path: str, body: dict) -> str:
    req = urllib.request.Request(
        f"{BASE}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return _o.open(req, timeout=60).read().decode()


action = sys.argv[1]

if action == "backup":
    vmid  = int(sys.argv[2])
    vtype = sys.argv[3]
    print(_post("/api/host/ci/backup/pbs", {"vmid": vmid, "type": vtype}))

elif action == "job":
    print(_get(f"/api/job/{sys.argv[2]}"))

else:
    sys.exit(f"Unknown action: {action}")
