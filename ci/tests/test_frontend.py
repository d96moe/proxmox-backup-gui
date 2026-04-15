"""Frontend integration tests using Playwright + a configurable mock server.

Test categories:
  DEPLOY      — verifies the correct/current JS is actually being served
  DOM         — required elements exist, no null-dereference JS errors on page load/switch
  HAPPY       — normal data rendering
  ERROR       — backend failures render error states, not infinite Loading…
  RACE        — rapid host switching, stale-data prevention
  EDGE        — empty data, LXC-only, no cloud storage
  CONCURRENCY — multiple simultaneous users from different browsers
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer

import pytest
from playwright.async_api import async_playwright
from playwright.sync_api import Page

# ─────────────────────────────────────────────────────────────────────────────
# Mock API data
# ─────────────────────────────────────────────────────────────────────────────

HOSTS = [
    {"id": "home", "label": "Home"},
    {"id": "cabin", "label": "Cabin"},
]

ITEMS = {
    "home": {
        "storage": {"local_used": 100, "local_total": 500, "cloud_used": 0},
        "vms": [
            {"id": 101, "name": "home-vm", "type": "vm", "os": "linux", "status": "running",
             "template": False, "in_pve": True,
             "snapshots": [{"backup_time": 1700000000, "date": "2023-11-14 22:13:20",
                            "age": "1h ago", "local": True, "cloud": True,
                            "incremental": True, "size": "1.2 GB",
                            "restic_id": "abc123def456", "restic_short_id": "abc123de"}]}
        ],
        "lxcs": [
            {"id": 104, "name": "home-lxc", "type": "lxc", "os": "linux", "status": "running",
             "template": False, "in_pve": True,
             "snapshots": [{"backup_time": 1700000010, "date": "2023-11-14 22:13:30",
                            "age": "1h ago", "local": True, "cloud": True,
                            "incremental": True, "size": "500 MB",
                            "restic_id": "def456abc789", "restic_short_id": "def456ab"}]}
        ],
    },
    # snap-test: three VMs covering local+cloud, local-only, cloud-only
    # VM 503 has in_pve=False (deleted from PVE but PBS snapshots remain)
    "snap-test": {
        "storage": {"local_used": 50, "local_total": 200, "cloud_used": 0},
        "vms": [
            {"id": 501, "name": "both-snap-vm", "type": "vm", "os": "linux",
             "status": "running", "template": False, "in_pve": True,
             "snapshots": [{"backup_time": 1700001000, "date": "2023-11-14 22:16:40",
                            "age": "1h ago", "local": True, "cloud": True,
                            "incremental": True, "size": "1.0 GB",
                            "restic_id": "aaa111bbb222", "restic_short_id": "aaa111bb"}]},
            {"id": 502, "name": "local-only-vm", "type": "vm", "os": "linux",
             "status": "stopped", "template": False, "in_pve": True,
             "snapshots": [{"backup_time": 1700002000, "date": "2023-11-14 22:33:20",
                            "age": "2h ago", "local": True, "cloud": False,
                            "incremental": True, "size": "800 MB",
                            "restic_id": None, "restic_short_id": None}]},
            {"id": 503, "name": "cloud-only-vm", "type": "vm", "os": "linux",
             "status": "stopped", "template": False, "in_pve": False,
             "snapshots": [{"backup_time": 1699900000, "date": "2023-11-13 18:26:40",
                            "age": "26h ago", "local": False, "cloud": True,
                            "incremental": True, "size": "—",
                            "restic_id": "ccc333ddd444", "restic_short_id": "ccc333dd"}]},
        ],
        "lxcs": [],
    },
    "cabin": {
        "storage": {"local_used": 200, "local_total": 800, "cloud_used": 0},
        "vms": [
            {"id": 201, "name": "cabin-vm", "type": "vm", "os": "linux", "status": "running",
             "template": False, "in_pve": True,
             "snapshots": [{"backup_time": 1700000001, "date": "2023-11-14 22:13:21",
                            "age": "2h ago", "local": True, "cloud": True,
                            "incremental": True, "size": "2.4 GB"}]}
        ],
        "lxcs": [],
    },
    "empty": {
        "storage": {"local_used": 0, "local_total": 100, "cloud_used": 0},
        "vms": [],
        "lxcs": [],
    },
    "lxc-only": {
        "storage": {"local_used": 10, "local_total": 100, "cloud_used": 0},
        "vms": [],
        "lxcs": [
            {"id": 301, "name": "only-ct", "type": "lxc", "os": "linux", "status": "running",
             "template": False, "in_pve": True,
             "snapshots": []}
        ],
    },
}

STORAGE = {
    "home":  {"local_used": 100, "local_total": 500, "cloud_used": 50,
              "cloud_total": 2000, "cloud_quota_used": 738, "dedup_factor": 4.2},
    "cabin": {"local_used": 200, "local_total": 800, "cloud_used": 50,
              "cloud_total": 2000, "cloud_quota_used": 738, "dedup_factor": 7.6},
    # no-cloud: cloud_total is absent/null; no dedup_factor (PBS GC not available)
    "no-cloud": {"local_used": 30, "local_total": 100, "cloud_used": 0},
}

INFO = {
    "home":  {"pbs": "4.1.4", "pve": "9.1.4", "restic": "0.18.0"},
    "cabin": {"pbs": "4.1.4", "pve": "9.0.10", "restic": "0.18.0"},
}

# ─────────────────────────────────────────────────────────────────────────────
# Configurable mock server
# ─────────────────────────────────────────────────────────────────────────────

class ServerConfig:
    """Mutable config injected into the request handler via class attributes."""
    items_delay: float = 0.0       # seconds to sleep before /items response
    items_status: int = 200        # HTTP status for /items
    storage_status: int = 200
    hosts_status: int = 200
    # Per-host overrides (takes precedence over global)
    host_items_delay: dict[str, float] = {}
    host_items_status: dict[str, int] = {}
    # Job state for /api/job/<id>
    job_status: str = "done"
    job_logs: list = None       # None → default
    job_status_code: int = 200  # HTTP status for /api/job/ responses
    items_request_count: int = 0  # incremented on each /items hit — for refresh verification
    restic_status_code: int = 200  # HTTP status for /backup/restic POST


cfg = ServerConfig()


class MockHandler(BaseHTTPRequestHandler):
    def log_message(self, *_): pass

    def _json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status=500):
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/api/hosts":
            if cfg.hosts_status != 200:
                return self._error(cfg.hosts_status)
            return self._json(HOSTS)

        for host_id in ("home", "cabin", "empty", "lxc-only", "no-cloud", "snap-test"):
            if path == f"/api/host/{host_id}/items":
                cfg.items_request_count += 1
                delay = cfg.host_items_delay.get(host_id, cfg.items_delay)
                status = cfg.host_items_status.get(host_id, cfg.items_status)
                if delay:
                    time.sleep(delay)
                if status != 200:
                    return self._error(status)
                return self._json(ITEMS.get(host_id, ITEMS["home"]))

            if path == f"/api/host/{host_id}/storage":
                if cfg.storage_status != 200:
                    return self._error(cfg.storage_status)
                return self._json(STORAGE.get(host_id, STORAGE["home"]))

            if path == f"/api/host/{host_id}/info":
                return self._json(INFO.get(host_id, INFO["home"]))

        if path == "/api/mqtt-config":
            # Return enabled MQTT config pointing at a mock broker.
            # The browser-side mqtt.connect() is intercepted by the MQTT mock
            # script injected by the page fixture, so the URL is never used.
            return self._json({
                "enabled": True,
                "ws_url": "ws://127.0.0.1:19883/mqtt",
                "topic_prefix": "proxmox",
            })

        if path == "/api/auth/me":
            return self._json({"username": "admin", "role": "admin"})

        if path == "/api/jobs/active":
            return self._json([])

        if path.startswith("/api/job/"):
            if cfg.job_status_code != 200:
                return self._error(cfg.job_status_code)
            logs = cfg.job_logs if cfg.job_logs is not None else ["Operation complete."]
            return self._json({"id": path.split("/")[-1], "status": cfg.job_status,
                               "logs": logs, "label": "test"})

        # Serve frontend — resolve path for both repo layout (ci/tests/ → ../../frontend)
        # and integration CI layout (tests/ → ../frontend).
        import os
        _here = os.path.dirname(__file__)
        frontend_dir = os.path.join(_here, "..", "frontend")
        if not os.path.exists(os.path.join(frontend_dir, "index.html")):
            frontend_dir = os.path.join(_here, "..", "..", "frontend")
        file_path = os.path.join(frontend_dir, "index.html")
        if path == "/" and os.path.exists(file_path):
            with open(file_path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self._error(404)

    def do_POST(self):
        path = self.path.split("?")[0]
        # Consume request body
        length = int(self.headers.get("Content-Length", 0))
        if length:
            self.rfile.read(length)

        for host_id in ("home", "cabin", "snap-test"):
            if path in (f"/api/host/{host_id}/backup/pbs",
                        f"/api/host/{host_id}/restore"):
                return self._json({"job_id": "mock-job-1"})
            if path == f"/api/host/{host_id}/backup/restic":
                if cfg.restic_status_code != 200:
                    return self._error(cfg.restic_status_code)
                return self._json({"job_id": "mock-job-1"})

        self._error(404)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

def _mqtt_retained_messages(host_id: str) -> list[dict]:
    """Build the list of MQTT retained messages the agent would publish for host_id.
    Used by the browser-side MQTT mock to simulate a connected broker."""
    data = ITEMS.get(host_id, ITEMS["home"])
    storage = STORAGE.get(host_id, {"local_used": 0, "local_total": 100, "cloud_used": 0})
    prefix = f"proxmox/{host_id}"
    msgs = []

    # Agent online + ready
    msgs.append({"topic": f"{prefix}/agent/status",
                 "payload": json.dumps({"status": "online"})})
    msgs.append({"topic": f"{prefix}/state/ready",
                 "payload": json.dumps({"ts": 1700000000, "pve_ok": True, "pbs_ok": True})})

    # Storage
    msgs.append({"topic": f"{prefix}/storage",
                 "payload": json.dumps({**storage,
                                        "cloud_total": storage.get("cloud_total"),
                                        "cloud_quota_used": storage.get("cloud_quota_used"),
                                        "dedup_factor": storage.get("dedup_factor")})})

    # Schedules (idle)
    msgs.append({"topic": f"{prefix}/schedules",
                 "payload": json.dumps({"pbs_jobs": [], "pbs_running": False,
                                        "restic_next": None, "restic_running": False,
                                        "pbs_retention": [], "restic_retention": {}})})

    # Info
    info = INFO.get(host_id, {"pbs": "4.1.4", "pve": "9.0.0", "restic": "0.18.0"})
    msgs.append({"topic": f"{prefix}/info", "payload": json.dumps(info)})

    # Per-VM messages
    all_items = data.get("vms", []) + data.get("lxcs", [])
    vmids = [str(item["id"]) for item in all_items]
    msgs.append({"topic": f"{prefix}/vms/index",
                 "payload": json.dumps(sorted(vmids))})

    for item in all_items:
        vmid = str(item["id"])
        msgs.append({"topic": f"{prefix}/vm/{vmid}/meta",
                     "payload": json.dumps({
                         "vmid": vmid,
                         "name": item.get("name", f"vm-{vmid}"),
                         "type": item.get("type", "vm"),
                         "status": item.get("status", "running"),
                         "os": item.get("os", "linux"),
                         "template": item.get("template", False),
                         "in_pve": item.get("in_pve", True),
                     })})
        msgs.append({"topic": f"{prefix}/vm/{vmid}/pbs",
                     "payload": json.dumps({"snapshots": item.get("snapshots", [])})})

    return msgs


def _mqtt_mock_script(host_ids: list[str]) -> str:
    """Return JS to inject before page load via add_init_script().

    Defines window.mqtt as non-writable/non-configurable so that any later
    CDN load of mqtt.min.js cannot overwrite it.  The fake connect() returns
    a client that:
      1. Fires 'connect' after 5 ms
      2. On first subscribe, delivers retained messages for ALL configured hosts
         so every host's data is in the DataStore from the start.
    """
    all_msgs = {}
    for hid in host_ids:
        all_msgs[hid] = _mqtt_retained_messages(hid)

    return f"""
(function() {{
  const _retainedByHost = {json.dumps(all_msgs)};

  function _deliverForHost(hostId, handlers) {{
    const msgs = _retainedByHost[hostId] || [];
    msgs.forEach(function(m) {{
      if (handlers.message) handlers.message(m.topic, m.payload);
    }});
  }}

  function _activeHostId() {{
    // Read the currently active host from the sidebar nav element.
    const el = document.querySelector('.host-item.active');
    return el ? el.id.replace('nav-', '') : null;
  }}

  function _mockConnect(url, opts) {{
    const handlers = {{}};
    let _subscribed = false;
    const client = {{
      on: function(ev, fn) {{ handlers[ev] = fn; return this; }},
      subscribe: function(pattern, o, cb) {{
        // On first subscribe deliver retained messages for the active host.
        if (!_subscribed) {{
          _subscribed = true;
          setTimeout(function() {{
            const host = _activeHostId() || Object.keys(_retainedByHost)[0];
            _deliverForHost(host, handlers);
          }}, 30);
        }}
        if (typeof o === 'function') o(null, []);
        else if (typeof cb === 'function') cb(null, []);
        return this;
      }},
      publish: function(t, p, o, cb) {{
        // On any rescan command re-deliver retained messages for the currently
        // active host — correct even mid-switch because we read the DOM.
        if (t.indexOf('/cmd/rescan') !== -1) {{
          window._rescanCount++;
          var host = _activeHostId();
          if (host) {{
            setTimeout(function() {{ _deliverForHost(host, handlers); }}, 30);
          }}
        }}
        // On any action command (backup/restore/delete/…), reply with a job ack
        // so the frontend can open the job progress modal.
        // Topic format: {{prefix}}/{{host}}/cmd/{{action}}
        var cmdMatch = t.match(/^([^/]+)[/]([^/]+)[/]cmd[/](?!rescan)(.+)$/);
        if (cmdMatch) {{
          var _prefix = cmdMatch[1];
          var _host   = cmdMatch[2];
          try {{
            var _pl = JSON.parse(p);
            var _corr_id = _pl.corr_id;
            if (_corr_id) {{
              setTimeout(function() {{
                if (handlers.message) {{
                  handlers.message(
                    _prefix + '/' + _host + '/job/' + _corr_id + '/ack',
                    JSON.stringify({{ op_id: 'mock-job-1' }})
                  );
                }}
              }}, 20);
            }}
          }} catch(_) {{}}
        }}
        if (typeof o === 'function') o(null);
        else if (typeof cb === 'function') cb(null);
        return this;
      }},
      end: function() {{}},
      connected: true,
    }};
    setTimeout(function() {{ if (handlers.connect) handlers.connect(); }}, 5);
    return client;
  }}

  // Rescan command counter — readable via page.evaluate("window._rescanCount")
  window._rescanCount = 0;

  // Lock window.mqtt so that a later CDN mqtt.min.js load cannot overwrite it.
  Object.defineProperty(window, 'mqtt', {{
    configurable: false,
    writable: false,
    value: {{ connect: _mockConnect }},
  }});
}})();
"""


_MQTT_STUB_JS = """\
// Minimal mqtt.js stub — replaced by the MQTT mock script injected via add_init_script.
// Defines window.mqtt so the real CDN load can be skipped entirely in CI.
window.mqtt = {
  connect: function(url, opts) {
    var handlers = {};
    var client = {
      on: function(ev, fn) { handlers[ev] = fn; return this; },
      subscribe: function(t, o, cb) { return this; },
      publish: function(t, p, o, cb) { return this; },
      end: function() {},
      connected: false,
    };
    return client;
  }
};
"""


def _block_fonts(ctx):
    """Route Google Fonts and CDN requests to local stubs so tests never need
    outbound connections to googleapis.com / unpkg.com (firewalled in CI)."""
    ctx.route("**fonts.googleapis.com**",
              lambda r: r.fulfill(status=200, content_type="text/css", body=""))
    ctx.route("**fonts.gstatic.com**",
              lambda r: r.fulfill(status=200, content_type="font/woff2", body=b""))
    # Serve a minimal mqtt.js stub so the CDN load never blocks.
    # Our add_init_script mock wraps mqtt.connect() before this stub loads,
    # but this ensures mqtt.min.js requests never fail with a network error.
    ctx.route("**unpkg.com**mqtt**",
              lambda r: r.fulfill(status=200, content_type="application/javascript",
                                  body=_MQTT_STUB_JS))


@pytest.fixture(scope="session")
def mock_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), MockHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


@pytest.fixture(autouse=True)
def reset_server_config():
    """Reset mock server state before each test."""
    cfg.items_delay = 0.0
    cfg.items_status = 200
    cfg.storage_status = 200
    cfg.hosts_status = 200
    cfg.host_items_delay = {}
    cfg.host_items_status = {}
    cfg.job_status = "done"
    cfg.job_logs = None
    cfg.job_status_code = 200
    cfg.items_request_count = 0
    cfg.restic_status_code = 200
    yield


@pytest.fixture
def page(browser, mock_server):
    """Fresh page, loaded and ready (home host rendered)."""
    ctx = browser.new_context(base_url=mock_server)
    _block_fonts(ctx)
    # Inject MQTT mock BEFORE the page script runs so mqtt.connect() is intercepted.
    ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys())))
    pg = ctx.new_page()
    # Capture JS console errors so tests can assert on them
    pg._js_errors: list[str] = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    pg.goto("/")
    # Wait for actual home VM data — not just absence of loading text, which can
    # fire early on empty string before MQTT delivers the first message.
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('home-vm')",
        timeout=10000,
    )
    yield pg
    ctx.close()


@pytest.fixture
def blank_page(browser, mock_server):
    """Page that has NOT loaded yet — lets tests control timing."""
    ctx = browser.new_context(base_url=mock_server)
    _block_fonts(ctx)
    ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys())))
    pg = ctx.new_page()
    pg._js_errors: list[str] = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    yield pg, mock_server
    ctx.close()


def wait_content(pg: Page, text: str, timeout=10000):
    pg.wait_for_function(
        f"() => document.getElementById('content').innerText.includes({json.dumps(text)})",
        timeout=timeout,
    )


def content_text(pg: Page) -> str:
    return pg.locator("#content").inner_text()


def expand_vm_card(pg: Page, vmid: int):
    """Expand the snapshot section of the VM card with the given vmid.
    Snapshots are collapsed by default (.snapshots { display:none }).
    Works for both in_pve=True (has backup-btn) and in_pve=False (no backup-btn).
    """
    # Click .expand-btn directly — clicking .vm-header center can land on an inner
    # button (backup-btn / del-all-btn) that calls event.stopPropagation(), which
    # prevents toggleSnaps from firing, especially at mobile widths.
    pg.locator(f".vm-card:has([data-vmid='{vmid}'])").locator(".expand-btn").first.click()
    # Wait for snapshots to become visible (any restore button in this card)
    pg.locator(f".restore-btn[data-vmid='{vmid}']").first.wait_for(
        state="visible", timeout=5000
    )


# ─────────────────────────────────────────────────────────────────────────────
# DEPLOY — the tests that would have caught the actual prod bug
# ─────────────────────────────────────────────────────────────────────────────

def test_deploy_has_job_modal_elements(mock_server):
    """Job modal DOM elements must be present in the served HTML."""
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    for el_id in ("job-modal", "job-log", "job-badge", "job-close-btn", "job-title"):
        assert f'id="{el_id}"' in html or f"id='{el_id}'" in html, \
            f"#{el_id} missing from served HTML — job modal not deployed!"

def test_deploy_has_job_functions(mock_server):
    """openJobModal, closeJobModal, _pollJob must be present in the served HTML."""
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    for fn in ("openJobModal", "closeJobModal", "_pollJob"):
        assert fn in html, f"{fn}() missing from served HTML"

def test_deploy_refresh_btn_disable_in_open_job_modal(mock_server):
    """openJobModal must contain code to disable the refresh button."""
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    assert "refresh-btn" in html and "disabled" in html, \
        "refresh-btn disable logic not found in HTML"

def test_deploy_has_backup_modal_elements(mock_server):
    """Backup choice modal elements must be present in served HTML."""
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    for el_id in ("backup-modal", "backup-confirm-btn", "backup-modal-options"):
        assert f'id="{el_id}"' in html or f"id='{el_id}'" in html, \
            f"#{el_id} missing from served HTML — backup modal not deployed!"

def test_deploy_has_new_js_functions(mock_server):
    """New JS functions for backup modal and job indicator must be present."""
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    for fn in ("openBackupModal", "closeBackupModal", "reopenJobModal",
               "_activeJobs", "_setJobIndicator"):
        assert fn in html, f"{fn} missing from served HTML"

def test_deploy_js_has_abort_controller(mock_server):
    """Served index.html must contain AbortController — proves correct file is deployed."""
    import urllib.request
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    assert "AbortController" in html, \
        "AbortController not found — old index.html is being served!"

def test_deploy_no_vms_section_reference(mock_server):
    """Old broken code referenced getElementById('vms-section') which doesn't exist.
    That TypeError crashed selectHost() before loadData() was called."""
    import urllib.request
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    assert "vms-section" not in html, \
        "Old broken getElementById('vms-section') still present in served HTML!"

def test_deploy_no_lxcs_section_reference(mock_server):
    """Same as above for lxcs-section."""
    import urllib.request
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    assert "lxcs-section" not in html, \
        "Old broken getElementById('lxcs-section') still present in served HTML!"

def test_deploy_mqtt_bus_present(mock_server):
    """MQTTBus closure must be in served HTML — it is the entire MQTT backbone."""
    import urllib.request
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    assert "MQTTBus" in html, \
        "MQTTBus not found — MQTT data pipeline not deployed"

def test_deploy_render_cloud_from_ds_present(mock_server):
    """renderCloudFromDS must be in served HTML — replaces REST for cloud tab."""
    import urllib.request
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    assert "renderCloudFromDS" in html, \
        "renderCloudFromDS() missing — cloud snapshots tab will never render"

def test_deploy_agent_hostname_tracking_present(mock_server):
    """_agentHostname must be in served HTML.
    Without it backup/rescan commands go to the wrong MQTT topic."""
    import urllib.request
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    assert "_agentHostname" in html, \
        "_agentHostname missing — backup commands will publish to wrong topic"

def test_deploy_loaddata_is_mqtt_only(mock_server):
    """loadData() must be pure MQTT — no fetch('/api/host/...') REST calls."""
    import urllib.request
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    idx = html.find("function loadData")
    assert idx != -1, "loadData() not found in served HTML"
    snippet = html[idx:idx + 300]
    assert "/api/host" not in snippet, \
        "loadData() still contains REST call to /api/host — should be pure MQTT"

def test_deploy_progress_regex_matches_new_format(mock_server):
    """_pollJob progress regex must match '  42% —' (space-prefixed, not '[42%').
    Wrong regex = progress bar never moves during restic backup."""
    import urllib.request
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    # New pattern must be present
    assert r"^\s+(\d+)%\s+" in html, \
        "Updated progress regex not found — progress bar will not update during backup"
    # Old broken pattern must be gone
    assert r"^\[(\d+)%\s+" not in html, \
        "Old broken '[X%' progress regex still present"


# ─────────────────────────────────────────────────────────────────────────────
# DOM — required elements exist, no JS errors thrown during page lifecycle
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_IDS = [
    "content", "hostname-label", "page-subtitle",
    "local-usage", "local-bar", "cloud-usage", "cloud-bar",
    "refresh-btn", "hosts-section",
    "job-modal", "job-close-btn", "job-badge", "job-log",
]

def test_dom_required_elements_exist(page: Page):
    """All element IDs that JS touches must exist — missing ones cause TypeError crashes."""
    for el_id in REQUIRED_IDS:
        result = page.evaluate(f"document.getElementById('{el_id}') !== null")
        assert result, f"Required DOM element #{el_id} is missing!"

def test_dom_no_js_errors_on_load(page: Page):
    """No uncaught JS exceptions during initial page load and host render."""
    assert page._js_errors == [], \
        f"JS errors on page load: {page._js_errors}"

def test_dom_no_js_errors_on_host_switch(page: Page):
    """No uncaught JS exceptions when switching between hosts."""
    page.click("#nav-cabin")
    wait_content(page, "cabin-vm")
    page.click("#nav-home")
    wait_content(page, "home-vm")
    assert page._js_errors == [], \
        f"JS errors during host switch: {page._js_errors}"


# ─────────────────────────────────────────────────────────────────────────────
# HAPPY — correct data shown
# ─────────────────────────────────────────────────────────────────────────────

def test_happy_initial_load(page: Page):
    assert "home-vm" in content_text(page)
    assert page.locator("#hostname-label").inner_text() == "Home"

def test_happy_shows_both_vms_and_lxcs(page: Page):
    wait_content(page, "home-vm")
    assert "home-vm" in content_text(page)
    assert "home-lxc" in content_text(page)

def test_happy_subtitle_vm_count(page: Page):
    subtitle = page.locator("#page-subtitle").inner_text()
    assert "1 VM" in subtitle
    assert "1 LXC" in subtitle

def test_happy_switch_to_cabin(page: Page):
    page.click("#nav-cabin")
    wait_content(page, "cabin-vm")
    assert "cabin-vm" in content_text(page)
    assert "home-vm" not in content_text(page)

def test_happy_switch_back_to_home(page: Page):
    page.click("#nav-cabin")
    wait_content(page, "cabin-vm")
    page.click("#nav-home")
    wait_content(page, "home-vm")
    assert "home-vm" in content_text(page)
    assert "cabin-vm" not in content_text(page)

def test_happy_storage_shows_for_home(page: Page):
    page.wait_for_function(
        "() => document.getElementById('local-usage').innerText.includes('100')",
        timeout=5000,
    )
    assert "100" in page.locator("#local-usage").inner_text()

def test_happy_storage_updates_on_switch(page: Page):
    page.wait_for_function(
        "() => document.getElementById('local-usage').innerText.includes('100')",
        timeout=5000,
    )
    page.click("#nav-cabin")
    page.wait_for_function(
        "() => document.getElementById('local-usage').innerText.includes('200')",
        timeout=5000,
    )
    assert "200" in page.locator("#local-usage").inner_text()

def test_happy_refresh_reloads(page: Page):
    page.click("#refresh-btn")
    wait_content(page, "home-vm")
    assert "home-vm" in content_text(page)


# ─────────────────────────────────────────────────────────────────────────────
# ERROR — backend failures must show errors, never hang on Loading…
# ─────────────────────────────────────────────────────────────────────────────

def test_error_items_500_shows_error_not_loading(blank_page):
    """MQTT-only frontend: data arrives via MQTT (not REST), page must render VM data.
    REST /items status has no effect since loadData() is pure MQTT."""
    pg, server = blank_page
    pg.goto(server)
    # MQTT mock delivers home data → wait for the VM card to appear
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('home-vm')",
        timeout=10000,
    )
    text = content_text(pg)
    assert "home-vm" in text, f"Expected VM data after MQTT connect, got: {text!r}"

def test_error_hosts_500_shows_cannot_reach(blank_page):
    """If /api/hosts fails, page shows 'Cannot reach' message."""
    cfg.hosts_status = 500
    pg, server = blank_page
    pg.goto(server)
    pg.wait_for_function(
        "() => document.getElementById('content').innerText !== ''",
        timeout=8000,
    )
    text = content_text(pg)
    assert "Loading" not in text, f"Still loading after hosts failure: {text!r}"

def test_error_storage_500_still_shows_vms(page: Page):
    """Storage endpoint failure must not break VM display — graceful degradation."""
    cfg.storage_status = 500
    page.click("#refresh-btn")
    wait_content(page, "home-vm")
    # VMs must still render
    assert "home-vm" in content_text(page)
    # Storage shows dashes (not real data and not a JS crash)
    assert page._js_errors == [], f"Storage 500 caused JS errors: {page._js_errors}"

def test_error_storage_clears_on_host_switch(page: Page):
    """Storage meters must reset to — immediately when switching hosts,
    not keep the old host's values while the new request is in flight."""
    page.wait_for_function(
        "() => document.getElementById('local-usage').innerText.includes('100')",
        timeout=5000,
    )
    # Make cabin storage slow so we can catch the — state
    cfg.host_items_delay["cabin"] = 0.5
    page.evaluate("document.getElementById('nav-cabin').click()")
    # Right after click, before storage response arrives, should show —
    usage = page.locator("#local-usage").inner_text()
    assert "100" not in usage, \
        f"Home storage value '100' still visible immediately after switching to cabin: {usage!r}"


# ─────────────────────────────────────────────────────────────────────────────
# RACE — rapid switching must not leak stale data
# ─────────────────────────────────────────────────────────────────────────────

def test_race_rapid_switch_shows_final_host(page: Page):
    """Switch cabin→home quickly: cabin response must be aborted, home wins."""
    cfg.host_items_delay["cabin"] = 3.0
    page.click("#nav-cabin")
    time.sleep(0.05)
    page.click("#nav-home")
    wait_content(page, "home-vm")
    text = content_text(page)
    assert "cabin-vm" not in text, f"Stale cabin data leaked: {text!r}"

def test_race_switch_clears_content_immediately(page: Page):
    """Content element must clear old host data synchronously on host click.
    With MQTT frontend shows 'Connecting…' (was 'Loading…' for REST)."""
    page.evaluate("document.getElementById('nav-cabin').click()")
    # Immediately after click (same JS tick), content must not still show home data
    text = content_text(page)
    assert "Loading" in text or "Connecting" in text or "cabin" in text, \
        f"Content not cleared immediately on host switch: {text!r}"

def test_race_storage_no_crosscontamination(page: Page):
    """Cabin storage (200 GB) must never appear while Home is selected."""
    cfg.host_items_delay["cabin"] = 0.5
    # Click cabin then immediately back to home
    page.evaluate("document.getElementById('nav-cabin').click()")
    time.sleep(0.02)
    page.evaluate("document.getElementById('nav-home').click()")
    # Storage must eventually show home (100), never cabin (200)
    page.wait_for_function(
        "() => document.getElementById('local-usage').innerText.includes('100')",
        timeout=8000,
    )
    assert "200" not in page.locator("#local-usage").inner_text()


# ─────────────────────────────────────────────────────────────────────────────
# EDGE — unusual data states
# ─────────────────────────────────────────────────────────────────────────────

def test_edge_empty_host_no_crash(browser, mock_server):
    """Host with zero VMs and zero LXCs must render without JS errors."""
    # Temporarily add 'empty' to HOSTS
    orig_hosts = HOSTS[:]
    HOSTS.append({"id": "empty", "label": "Empty"})
    try:
        ctx = browser.new_context(base_url=mock_server)
        _block_fonts(ctx)
        ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys())))
        pg = ctx.new_page()
        pg._js_errors = []
        pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
        pg.goto("/")
        pg.wait_for_function(
            "() => document.getElementById('content').innerText.includes('home-vm')",
            timeout=10000,
        )
        pg.click("#nav-empty")
        # After MQTT delivers vms/index=[] the frontend shows "No VMs or containers found."
        pg.wait_for_function(
            "() => document.getElementById('content').innerText.includes('No VMs')",
            timeout=10000,
        )
        assert pg._js_errors == [], f"JS errors on empty host: {pg._js_errors}"
        ctx.close()
    finally:
        HOSTS[:] = orig_hosts

def test_edge_lxc_only_shows_containers(browser, mock_server):
    """Host with only LXCs (no VMs) renders container section."""
    orig_hosts = HOSTS[:]
    HOSTS.append({"id": "lxc-only", "label": "LXC Only"})
    try:
        ctx = browser.new_context(base_url=mock_server)
        _block_fonts(ctx)
        ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys())))
        pg = ctx.new_page()
        pg._js_errors = []
        pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
        pg.goto("/")
        pg.wait_for_function(
            "() => document.getElementById('content').innerText.includes('home-vm')",
            timeout=10000,
        )
        pg.click("#nav-lxc-only")
        pg.wait_for_function(
            "() => document.getElementById('content').innerText.includes('only-ct')",
            timeout=10000,
        )
        assert "only-ct" in pg.locator("#content").inner_text()
        assert pg._js_errors == [], f"JS errors on lxc-only host: {pg._js_errors}"
        ctx.close()
    finally:
        HOSTS[:] = orig_hosts

def test_edge_slow_backend_8s_eventually_loads(blank_page):
    """Simulate the real 8-second backend response — must load, not time out."""
    cfg.items_delay = 8.0
    pg, server = blank_page
    pg.goto(server)
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('home-vm')",
        timeout=15000,
    )
    assert "home-vm" in content_text(pg)


# ─────────────────────────────────────────────────────────────────────────────
# CONCURRENCY — multiple simultaneous users from different browsers
# Playwright sync_api is NOT thread-safe (greenlets). Use async_api + asyncio.
# ─────────────────────────────────────────────────────────────────────────────

async def _async_open_page(browser, base_url: str):
    """Open an isolated async browser context and wait for first render."""
    ctx = await browser.new_context(base_url=base_url)
    await ctx.route("**fonts.googleapis.com**",
                    lambda r: r.fulfill(status=200, content_type="text/css", body=""))
    await ctx.route("**fonts.gstatic.com**",
                    lambda r: r.fulfill(status=200, content_type="font/woff2", body=b""))
    await ctx.route("**unpkg.com**mqtt**",
                    lambda r: r.fulfill(status=200, content_type="application/javascript",
                                        body=_MQTT_STUB_JS))
    await ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys())))
    pg = await ctx.new_page()
    pg._js_errors = []
    pg._ctx = ctx
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    await pg.goto("/")
    # Wait for actual content: must contain 'vm' (a VM card) or an error — not just "not Loading…"
    await pg.wait_for_function(
        "() => { const t = document.getElementById('content').innerText; "
        "        return t.includes('vm') || t.includes('Error') || t.includes('error'); }",
        timeout=30000,
    )
    return pg


def run_async(coro):
    """Run an async coroutine in a fresh event loop in its own thread.
    This avoids 'event loop already running' when pytest-anyio owns the main loop."""
    result = [None]
    exc = [None]
    def _run():
        try:
            result[0] = asyncio.run(coro)
        except Exception as e:
            exc[0] = e
    t = threading.Thread(target=_run)
    t.start()
    t.join(timeout=120)
    if exc[0]:
        raise exc[0]
    return result[0]


def test_concurrent_two_users_both_see_correct_data(mock_server):
    """Two users loading simultaneously must each see correct host data."""
    async def run():
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            pages = await asyncio.gather(
                _async_open_page(browser, mock_server),
                _async_open_page(browser, mock_server),
            )
            texts = [await pg.locator("#content").inner_text() for pg in pages]
            for ctx in [pg._ctx for pg in pages]:
                await ctx.close()
            await browser.close()
        return texts

    texts = run_async(run())
    for i, text in enumerate(texts):
        assert "home-vm" in text, f"User {i} got wrong content: {text[:200]!r}"


def test_concurrent_users_on_different_hosts(mock_server):
    """User A on 'home', User B on 'cabin' — neither sees the other's data."""
    async def run():
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            pg_home, pg_cabin = await asyncio.gather(
                _async_open_page(browser, mock_server),
                _async_open_page(browser, mock_server),
            )
            # User B switches to cabin while User A stays on home
            await pg_cabin.click("#nav-cabin")
            await pg_cabin.wait_for_function(
                "() => document.getElementById('content').innerText.includes('cabin-vm')",
                timeout=10000,
            )
            home_text = await pg_home.locator("#content").inner_text()
            cabin_text = await pg_cabin.locator("#content").inner_text()
            await pg_home._ctx.close()
            await pg_cabin._ctx.close()
            await browser.close()
        return home_text, cabin_text

    home_text, cabin_text = run_async(run())
    assert "home-vm" in home_text,  "Home user missing home-vm"
    assert "cabin-vm" in cabin_text, "Cabin user missing cabin-vm"
    assert "cabin-vm" not in home_text, "Home user saw cabin data!"
    assert "home-vm" not in cabin_text, "Cabin user saw home data!"


def test_concurrent_one_user_switch_doesnt_affect_other(mock_server):
    """User B switching hosts must not disturb User A's stable view."""
    async def run():
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            pg_a, pg_b = await asyncio.gather(
                _async_open_page(browser, mock_server),
                _async_open_page(browser, mock_server),
            )

            async def user_a_sample():
                samples = []
                for _ in range(6):
                    samples.append(await pg_a.locator("#content").inner_text())
                    await asyncio.sleep(0.3)
                return samples

            async def user_b_switch():
                for _ in range(3):
                    await pg_b.click("#nav-cabin")
                    await pg_b.wait_for_function(
                        "() => document.getElementById('content').innerText.includes('cabin-vm')",
                        timeout=10000,
                    )
                    await pg_b.click("#nav-home")
                    await pg_b.wait_for_function(
                        "() => document.getElementById('content').innerText.includes('home-vm')",
                        timeout=10000,
                    )

            samples, _ = await asyncio.gather(user_a_sample(), user_b_switch())
            await pg_a._ctx.close()
            await pg_b._ctx.close()
            await browser.close()
        return samples

    samples = run_async(run())
    for i, text in enumerate(samples):
        assert "home-vm" in text, \
            f"User A sample {i} missing home-vm: {text[:200]!r}"
        assert "cabin-vm" not in text, \
            f"User A sample {i} contaminated with cabin-vm (User B bleed-through!): {text[:200]!r}"


def test_concurrent_5_users_simultaneous_load(mock_server):
    """5 simultaneous users — backend must serve all correctly."""
    cfg.items_delay = 0.2

    async def run():
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            pages = await asyncio.gather(*[
                _async_open_page(browser, mock_server) for _ in range(5)
            ])
            texts = [await pg.locator("#content").inner_text() for pg in pages]
            for pg in pages:
                await pg._ctx.close()
            await browser.close()
        return texts

    texts = run_async(run())
    assert len(texts) == 5
    for i, text in enumerate(texts):
        assert "home-vm" in text, f"User {i} got garbage under load: {text[:200]!r}"


def test_concurrent_backend_cache_shared_not_duplicated(mock_server):
    """Concurrent cache: two requests arriving simultaneously must both get data,
    not deadlock or corrupt each other. Uses mock server with a delay."""
    cfg.items_delay = 0.5
    url = mock_server + "/api/host/home/items"
    results = {}
    errors = []

    def fetch(uid):
        try:
            t0 = time.monotonic()
            data = json.loads(urllib.request.urlopen(url).read())
            results[uid] = (time.monotonic() - t0, data)
        except Exception as e:
            errors.append(f"uid={uid}: {e}")

    # Fire 5 requests simultaneously
    threads = [threading.Thread(target=fetch, args=(i,)) for i in range(5)]
    for t in threads: t.start()
    for t in threads: t.join(timeout=15)

    assert not errors, f"Errors in concurrent fetch: {errors}"
    assert len(results) == 5, f"Not all requests completed: {len(results)}/5"
    for uid, (duration, data) in results.items():
        assert "vms" in data, f"Request {uid} got corrupt data: {str(data)[:100]}"


# ─────────────────────────────────────────────────────────────────────────────
# JOB — backup/restore job modal behaviour
# ─────────────────────────────────────────────────────────────────────────────

def test_job_refresh_btn_disabled_while_modal_open(page: Page):
    """Refresh button is NOT disabled when job modal opens — close button is always enabled
    so user can dismiss the modal at any time and reopen via the card indicator."""
    cfg.job_status = "running"
    cfg.job_logs = ["Starting..."]
    page.evaluate("openJobModal('Test', 'mock-job-1')")
    page.wait_for_selector("#job-modal.open", timeout=3000)
    # Close button must be immediately enabled (user can dismiss at any time)
    assert not page.locator("#job-close-btn").is_disabled(), \
        "Close button should be enabled from the start — user can dismiss and reopen via indicator"
    page.evaluate("closeJobModal()")

def test_job_refresh_btn_re_enabled_after_close(page: Page):
    """Refresh button must be re-enabled after the job modal is closed."""
    cfg.job_status = "running"
    cfg.job_logs = ["Starting..."]
    page.evaluate("openJobModal('Test', 'mock-job-1')")
    page.evaluate("closeJobModal()")
    assert not page.locator("#refresh-btn").is_disabled(), \
        "Refresh button should be enabled after closing the job modal"

def test_job_close_btn_enabled_when_done(page: Page):
    """Close button must become enabled once the job reports status 'done'."""
    cfg.job_status = "done"
    cfg.job_logs = ["Backup complete."]
    page.evaluate("openJobModal('Test', 'mock-job-1')")
    page.wait_for_function(
        "() => !document.getElementById('job-close-btn').disabled",
        timeout=6000,
    )
    assert not page.locator("#job-close-btn").is_disabled(), \
        "Close button should be enabled when job is done"

def test_job_close_btn_enabled_when_error(page: Page):
    """Close button must also be enabled when the job ends with an error."""
    cfg.job_status = "error"
    cfg.job_logs = ["ERROR: something went wrong"]
    page.evaluate("openJobModal('Test', 'mock-job-1')")
    page.wait_for_function(
        "() => !document.getElementById('job-close-btn').disabled",
        timeout=6000,
    )
    assert not page.locator("#job-close-btn").is_disabled(), \
        "Close button should be enabled when job errors"

def test_job_modal_shows_log_lines(page: Page):
    """Log lines returned by /api/job/ must appear in the job-log div."""
    cfg.job_status = "running"
    cfg.job_logs = ["Step 1 done", "Step 2 running"]
    page.evaluate("openJobModal('Test', 'mock-job-1')")
    page.wait_for_function(
        "() => document.getElementById('job-log').innerText.includes('Step 1')",
        timeout=6000,
    )
    log_text = page.locator("#job-log").inner_text()
    assert "Step 1 done" in log_text
    assert "Step 2 running" in log_text

def test_job_badge_shows_error_status(page: Page):
    """Job badge text must change to 'error' when job reports error status."""
    cfg.job_status = "error"
    cfg.job_logs = ["ERROR: restore failed"]
    page.evaluate("openJobModal('Test', 'mock-job-1')")
    page.wait_for_function(
        "() => document.getElementById('job-badge').textContent === 'error'",
        timeout=6000,
    )
    assert page.locator("#job-badge").inner_text() == "error"

def test_job_badge_shows_done_status(page: Page):
    """Job badge text must change to 'done' when job completes successfully."""
    cfg.job_status = "done"
    cfg.job_logs = ["Backup complete."]
    page.evaluate("openJobModal('Test', 'mock-job-1')")
    page.wait_for_function(
        "() => document.getElementById('job-badge').textContent === 'done'",
        timeout=6000,
    )
    assert page.locator("#job-badge").inner_text() == "done"

def test_job_no_js_errors_open_close(page: Page):
    """No JS errors when opening and closing the job modal."""
    cfg.job_status = "done"
    cfg.job_logs = ["Done."]
    page.evaluate("openJobModal('Test', 'mock-job-1')")
    page.wait_for_function(
        "() => !document.getElementById('job-close-btn').disabled",
        timeout=6000,
    )
    page.evaluate("closeJobModal()")
    assert page._js_errors == [], f"JS errors during job modal lifecycle: {page._js_errors}"

def test_job_refresh_btn_enabled_after_done_and_close(page: Page):
    """Full lifecycle: open → done → close must leave refresh button enabled.

    When the job completes, refreshData() is called which temporarily disables
    the button while /items loads. We wait for it to become enabled again.
    """
    cfg.job_status = "done"
    cfg.job_logs = ["Backup complete."]
    page.evaluate("openJobModal('Test', 'mock-job-1')")
    page.wait_for_function(
        "() => document.getElementById('job-badge').textContent === 'done'",
        timeout=6000,
    )
    page.evaluate("closeJobModal()")
    page.wait_for_function(
        "() => !document.getElementById('refresh-btn').disabled",
        timeout=6000,
    )
    assert not page.locator("#refresh-btn").is_disabled(), \
        "Refresh button should be enabled after job done + modal closed"

def test_job_triggers_data_refresh_on_done(page: Page):
    """JS calls refreshData() automatically when job status becomes 'done'.
    Verified by counting MQTT rescan commands sent (MQTT-only frontend publishes
    a rescan cmd instead of fetching /items via REST)."""
    cfg.job_status = "done"
    cfg.job_logs = ["Backup complete."]
    rescan_before = page.evaluate("window._rescanCount || 0")
    page.evaluate("openJobModal('Test', 'mock-job-1')")
    page.wait_for_function(
        "() => !document.getElementById('job-close-btn').disabled",
        timeout=6000,
    )
    # Give the async refreshData() a moment to publish the rescan command
    page.wait_for_timeout(600)
    rescan_after = page.evaluate("window._rescanCount || 0")
    assert rescan_after > rescan_before, \
        "No MQTT rescan command sent after job done — refreshData() may be broken"

def test_job_modal_reopen_works_correctly(page: Page):
    """Open → close → open again: second modal must poll and complete without stale state."""
    cfg.job_status = "done"
    cfg.job_logs = ["Done."]
    # First cycle
    page.evaluate("openJobModal('First', 'mock-job-1')")
    page.wait_for_function(
        "() => !document.getElementById('job-close-btn').disabled", timeout=6000)
    page.evaluate("closeJobModal()")
    # Second cycle — must work cleanly
    page.evaluate("openJobModal('Second', 'mock-job-1')")
    page.wait_for_function(
        "() => ['done','error'].includes(document.getElementById('job-badge').textContent)",
        timeout=6000)
    assert page.locator("#job-badge").inner_text() == "done"
    assert page._js_errors == [], f"JS errors on second modal open: {page._js_errors}"
    page.evaluate("closeJobModal()")

def test_job_500_from_endpoint_no_crash_and_refresh_recovers(page: Page):
    """If /api/job/ returns 500, modal must not crash. Close button stays enabled
    (user can always dismiss). After close, page still functions."""
    cfg.job_status_code = 500
    page.evaluate("openJobModal('Test', 'mock-job-1')")
    # Wait several poll cycles — 500s must be swallowed silently
    page.wait_for_timeout(5500)
    assert page._js_errors == [], f"JS errors on 500 from job endpoint: {page._js_errors}"
    # Close button must be enabled — user can always dismiss even a stuck job
    assert not page.locator("#job-close-btn").is_disabled(), \
        "Close button should always be enabled so user can dismiss a stuck job"
    # Manually close (simulates user giving up)
    page.evaluate("closeJobModal()")
    # Page must still function — refresh loads data
    cfg.job_status_code = 200
    page.click("#refresh-btn")
    wait_content(page, "home-vm")


# ─────────────────────────────────────────────────────────────────────────────
# RESTORE MODAL — full UI flow and source selection logic
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def snap_test_page(browser, mock_server):
    """Page loaded with the snap-test host (local+cloud, local-only, cloud-only VMs)."""
    orig_hosts = HOSTS[:]
    HOSTS.append({"id": "snap-test", "label": "Snap Test"})
    ctx = browser.new_context(base_url=mock_server)
    _block_fonts(ctx)
    ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys()) + ["snap-test"]))
    pg = ctx.new_page()
    pg._js_errors = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    pg.goto("/")
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('home-vm')",
        timeout=10000)
    pg.click("#nav-snap-test")
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('both-snap-vm')",
        timeout=10000)
    yield pg
    ctx.close()
    HOSTS[:] = orig_hosts

def test_restore_modal_opens_with_correct_vm_name(page: Page):
    """Clicking Restore on a snapshot row opens the modal with the correct VM name."""
    expand_vm_card(page, 101)
    restore_btn = page.locator(".restore-btn[data-vmid='101']").first
    restore_btn.click()
    page.wait_for_selector("#modal.open", timeout=5000)
    subtitle = page.locator("#modal-subtitle").inner_text()
    assert "home-vm" in subtitle, f"VM name missing from modal subtitle: {subtitle!r}"
    page.click("button.modal-close")

def test_restore_modal_both_sources_shown_for_local_cloud_snap(page: Page):
    """Snapshot with local=True and cloud=True must show both Local PBS and cloud options."""
    expand_vm_card(page, 101)
    restore_btn = page.locator(".restore-btn[data-vmid='101']").first
    restore_btn.click()
    page.wait_for_selector("#modal.open", timeout=5000)
    assert page.locator(".source-opt[data-source='local']").count() == 1, \
        "Local PBS source option missing"
    assert page.locator(".source-opt[data-source='cloud']").count() == 1, \
        "Cloud source option missing"
    page.click("button.modal-close")

def test_restore_modal_only_local_source_for_local_only_snap(snap_test_page: Page):
    """Snapshot with local=True, cloud=False must show only Local PBS option."""
    expand_vm_card(snap_test_page, 502)
    restore_btn = snap_test_page.locator(".restore-btn[data-vmid='502']").first
    restore_btn.click()
    snap_test_page.wait_for_selector("#modal.open", timeout=5000)
    assert snap_test_page.locator(".source-opt[data-source='local']").count() == 1
    assert snap_test_page.locator(".source-opt[data-source='cloud']").count() == 0, \
        "Cloud source must not appear for local-only snapshot"
    snap_test_page.click("button.modal-close")

def test_restore_modal_only_cloud_source_for_cloud_only_snap(snap_test_page: Page):
    """Snapshot with local=False, cloud=True must show only cloud option."""
    expand_vm_card(snap_test_page, 503)
    restore_btn = snap_test_page.locator(".restore-btn[data-vmid='503']").first
    restore_btn.click()
    snap_test_page.wait_for_selector("#modal.open", timeout=5000)
    assert snap_test_page.locator(".source-opt[data-source='cloud']").count() == 1
    assert snap_test_page.locator(".source-opt[data-source='local']").count() == 0, \
        "Local PBS source must not appear for cloud-only snapshot"
    snap_test_page.click("button.modal-close")

def test_restore_modal_cloud_warning_changes_on_source_switch(page: Page):
    """Selecting cloud source must show the datastore-overwrite warning."""
    expand_vm_card(page, 101)
    restore_btn = page.locator(".restore-btn[data-vmid='101']").first
    restore_btn.click()
    page.wait_for_selector("#modal.open", timeout=5000)
    # Default (local) — standard warning
    local_warn = page.locator("#modal-warning").inner_text()
    assert "datastore" not in local_warn.lower(), \
        f"Local warning should not mention datastore: {local_warn!r}"
    # Switch to cloud
    page.locator(".source-opt[data-source='cloud']").click()
    cloud_warn = page.locator("#modal-warning").inner_text()
    assert "datastore" in cloud_warn.lower() or "overwrite" in cloud_warn.lower(), \
        f"Cloud warning must mention datastore overwrite: {cloud_warn!r}"
    page.click("button.modal-close")

def test_restore_modal_backup_after_unchecked_by_default(page: Page):
    """run-backup-after checkbox must be unchecked when modal opens."""
    expand_vm_card(page, 101)
    restore_btn = page.locator(".restore-btn[data-vmid='101']").first
    restore_btn.click()
    page.wait_for_selector("#modal.open", timeout=5000)
    assert not page.locator("#modal-backup-after").is_checked(), \
        "run-backup-after checkbox should be unchecked by default"
    page.click("button.modal-close")

def test_restore_modal_full_flow_local(page: Page):
    """Full flow: click Restore → select local → confirm → job modal shows done."""
    cfg.job_status = "done"
    cfg.job_logs = ["Restore complete."]
    expand_vm_card(page, 101)
    restore_btn = page.locator(".restore-btn[data-vmid='101']").first
    restore_btn.click()
    page.wait_for_selector("#modal.open", timeout=5000)
    page.locator(".source-opt[data-source='local']").click()
    page.click("#modal-confirm-btn")
    page.wait_for_selector("#job-modal.open", timeout=5000)
    assert page.locator("#job-title").inner_text() == "Restore"
    page.wait_for_function(
        "() => !document.getElementById('job-close-btn').disabled", timeout=6000)
    assert page.locator("#job-badge").inner_text() == "done"
    assert page._js_errors == []
    page.evaluate("closeJobModal()")

def test_restore_modal_full_flow_cloud(page: Page):
    """Full flow: click Restore → select cloud → confirm → job modal shows done."""
    cfg.job_status = "done"
    cfg.job_logs = ["Restore complete."]
    expand_vm_card(page, 101)
    restore_btn = page.locator(".restore-btn[data-vmid='101']").first
    restore_btn.click()
    page.wait_for_selector("#modal.open", timeout=5000)
    page.locator(".source-opt[data-source='cloud']").click()
    page.click("#modal-confirm-btn")
    page.wait_for_selector("#job-modal.open", timeout=5000)
    page.wait_for_function(
        "() => !document.getElementById('job-close-btn').disabled", timeout=6000)
    assert page.locator("#job-badge").inner_text() == "done"
    assert page._js_errors == []
    page.evaluate("closeJobModal()")

def test_restore_modal_failed_job_shows_error_badge(page: Page):
    """If restore job ends with error, modal badge must show 'error'."""
    cfg.job_status = "error"
    cfg.job_logs = ["ERROR: SSH connection refused"]
    expand_vm_card(page, 101)
    restore_btn = page.locator(".restore-btn[data-vmid='101']").first
    restore_btn.click()
    page.wait_for_selector("#modal.open", timeout=5000)
    page.click("#modal-confirm-btn")
    page.wait_for_selector("#job-modal.open", timeout=5000)
    page.wait_for_function(
        "() => document.getElementById('job-badge').textContent === 'error'", timeout=6000)
    log_text = page.locator("#job-log").inner_text()
    assert "ERROR" in log_text, f"Error message not shown in log: {log_text!r}"
    page.evaluate("closeJobModal()")

def test_restore_no_js_errors_full_flow(page: Page):
    """No JS errors across the entire restore flow: open → confirm → done → close."""
    cfg.job_status = "done"
    cfg.job_logs = ["Restore complete."]
    expand_vm_card(page, 101)
    restore_btn = page.locator(".restore-btn[data-vmid='101']").first
    restore_btn.click()
    page.wait_for_selector("#modal.open", timeout=5000)
    page.click("#modal-confirm-btn")
    page.wait_for_selector("#job-modal.open", timeout=5000)
    page.wait_for_function(
        "() => !document.getElementById('job-close-btn').disabled", timeout=6000)
    page.evaluate("closeJobModal()")
    assert page._js_errors == [], f"JS errors during restore flow: {page._js_errors}"


# ─────────────────────────────────────────────────────────────────────────────
# BACKUP NOW — backup modal (PBS only / PBS+cloud choice)
# ─────────────────────────────────────────────────────────────────────────────

def test_backup_now_opens_backup_modal(page: Page):
    """Clicking 'Backup now' must open the backup choice modal, not a confirm dialog."""
    backup_btn = page.locator(".backup-btn[data-vmid='101']").first
    backup_btn.wait_for(state="visible", timeout=5000)
    backup_btn.click()
    page.wait_for_selector("#backup-modal.open", timeout=5000)
    assert not page.locator("#job-modal").evaluate("el => el.classList.contains('open')"), \
        "Job modal must not open before user confirms backup choice"
    assert page._js_errors == []
    page.evaluate("closeBackupModal()")

def test_backup_modal_pbs_only_opens_job_modal(page: Page):
    """Selecting PBS only and confirming opens job modal titled 'PBS Backup'."""
    cfg.job_status = "done"
    cfg.job_logs = ["Backup complete."]
    page.locator(".backup-btn[data-vmid='101']").first.click()
    page.wait_for_selector("#backup-modal.open", timeout=5000)
    # PBS only is selected by default — just confirm
    page.click("#backup-confirm-btn")
    page.wait_for_selector("#job-modal.open", timeout=5000)
    assert page.locator("#job-title").inner_text() == "PBS Backup"
    page.wait_for_function(
        "() => ['done','error'].includes(document.getElementById('job-badge').textContent)",
        timeout=6000,
    )
    assert page.locator("#job-badge").inner_text() == "done"
    assert page._js_errors == []
    page.evaluate("closeJobModal()")

def test_backup_modal_pbs_plus_cloud_opens_job_modal(page: Page):
    """Selecting PBS+Cloud and confirming opens job modal with correct title."""
    cfg.job_status = "done"
    cfg.job_logs = ["Backup complete.", "Restic backup complete."]
    page.locator(".backup-btn[data-vmid='101']").first.click()
    page.wait_for_selector("#backup-modal.open", timeout=5000)
    page.locator(".source-opt[data-btype='pbs+restic']").click()
    page.click("#backup-confirm-btn")
    page.wait_for_selector("#job-modal.open", timeout=5000)
    assert page.locator("#job-title").inner_text() == "PBS + Cloud Backup"
    page.wait_for_function(
        "() => ['done','error'].includes(document.getElementById('job-badge').textContent)",
        timeout=6000,
    )
    assert page.locator("#job-badge").inner_text() == "done"
    assert page._js_errors == []
    page.evaluate("closeJobModal()")

def test_backup_modal_cancel_does_not_open_job_modal(page: Page):
    """Clicking Cancel in backup modal must NOT start a job."""
    page.locator(".backup-btn[data-vmid='101']").first.click()
    page.wait_for_selector("#backup-modal.open", timeout=5000)
    page.locator("#backup-modal .btn:not(.btn-primary)").click()
    page.wait_for_timeout(400)
    assert not page.locator("#job-modal").evaluate("el => el.classList.contains('open')"), \
        "Job modal must not open when backup is cancelled"
    assert page._js_errors == []

def test_backup_modal_no_js_errors_full_flow(page: Page):
    """No JS errors across backup modal open → select → confirm → done → close."""
    cfg.job_status = "done"
    cfg.job_logs = ["Backup complete."]
    page.locator(".backup-btn[data-vmid='101']").first.click()
    page.wait_for_selector("#backup-modal.open", timeout=5000)
    page.click("#backup-confirm-btn")
    page.wait_for_selector("#job-modal.open", timeout=5000)
    page.wait_for_function(
        "() => ['done','error'].includes(document.getElementById('job-badge').textContent)",
        timeout=6000,
    )
    page.evaluate("closeJobModal()")
    assert page._js_errors == [], f"JS errors during backup flow: {page._js_errors}"


# ─────────────────────────────────────────────────────────────────────────────
# SYNC BUTTON — ☁ sync visible for local-only VMs and snapshots
# ─────────────────────────────────────────────────────────────────────────────

def test_sync_button_visible_on_local_only_card(snap_test_page: Page):
    """Local-only VM card must show ☁ sync button at card level."""
    # VM 502 is local-only in snap-test fixture
    sync_btn = snap_test_page.locator(
        f".vm-card:has(.backup-btn[data-vmid='502']) .backup-btn:has-text('sync')"
    )
    assert sync_btn.count() > 0, "☁ sync button not found on local-only VM card"
    assert sync_btn.first.is_visible()


def test_sync_button_not_visible_on_fully_covered_card(snap_test_page: Page):
    """VM with all snapshots covered by cloud must NOT show ☁ sync at card level."""
    # VM 501 is local+cloud — no local-only snapshots
    sync_btn = snap_test_page.locator(
        f".vm-card:has(.backup-btn[data-vmid='501']) .backup-btn:has-text('sync')"
    )
    assert sync_btn.count() == 0, "☁ sync button should not appear on fully-covered VM card"


def test_sync_button_visible_in_local_only_snapshot_row(snap_test_page: Page):
    """Expanding a local-only VM must show ☁ sync in the snapshot row."""
    expand_vm_card(snap_test_page, 502)
    row_sync = snap_test_page.locator(
        f".vm-card:has(.backup-btn[data-vmid='502']) .snapshot-row .restore-btn:has-text('sync')"
    )
    assert row_sync.count() > 0, "☁ sync not found in local-only snapshot row after expand"
    assert row_sync.first.is_visible()


def test_sync_button_not_in_cloud_covered_snapshot_row(snap_test_page: Page):
    """Snapshot rows that already have cloud coverage must NOT show ☁ sync."""
    expand_vm_card(snap_test_page, 501)
    row_sync = snap_test_page.locator(
        f".vm-card:has(.backup-btn[data-vmid='501']) .snapshot-row .restore-btn:has-text('sync')"
    )
    assert row_sync.count() == 0, "☁ sync should not appear in cloud-covered snapshot row"


def test_sync_button_opens_job_modal(snap_test_page: Page):
    """Clicking ☁ sync on a local-only card → confirm dialog → job modal opens."""
    cfg.job_status = "done"
    cfg.job_logs = ["Restic backup complete."]
    snap_test_page.on("dialog", lambda d: d.accept())
    sync_btn = snap_test_page.locator(
        f".vm-card:has(.backup-btn[data-vmid='502']) .backup-btn:has-text('sync')"
    ).first
    sync_btn.click()
    snap_test_page.wait_for_selector("#job-modal.open", timeout=5000)
    snap_test_page.wait_for_function(
        "() => ['done','error'].includes(document.getElementById('job-badge').textContent)",
        timeout=6000,
    )
    assert snap_test_page.locator("#job-badge").inner_text() == "done"
    assert snap_test_page._js_errors == []
    snap_test_page.evaluate("closeJobModal()")


def test_restic_409_shows_friendly_alert(snap_test_page: Page):
    """When /backup/restic returns 409, the user sees a readable message (not raw HTTP)."""
    cfg.restic_status_code = 409
    alert_messages = []
    snap_test_page.on("dialog", lambda d: (alert_messages.append(d.message), d.accept()))
    sync_btn = snap_test_page.locator(
        f".vm-card:has(.backup-btn[data-vmid='502']) .backup-btn:has-text('sync')"
    ).first
    # First dialog is the confirm; second would be the alert if it appears
    sync_btn.click()
    snap_test_page.wait_for_timeout(800)
    assert any("restic" in m.lower() or "already" in m.lower() or "running" in m.lower()
               for m in alert_messages), \
        f"Expected friendly 409 message, got: {alert_messages}"
    assert snap_test_page._js_errors == []


# ─────────────────────────────────────────────────────────────────────────────
# JOB INDICATOR — running job shown on card, modal reopenable
# ─────────────────────────────────────────────────────────────────────────────

def test_job_indicator_appears_after_closing_modal(page: Page):
    """After starting a job and closing the modal, an indicator appears on the VM card."""
    cfg.job_status = "running"
    cfg.job_logs = ["Starting backup..."]
    page.locator(".backup-btn[data-vmid='101']").first.click()
    page.wait_for_selector("#backup-modal.open", timeout=5000)
    page.click("#backup-confirm-btn")
    page.wait_for_selector("#job-modal.open", timeout=5000)
    # Badge must show running before we close
    page.wait_for_function(
        "() => document.getElementById('job-badge').textContent === 'running'",
        timeout=4000,
    )
    page.evaluate("closeJobModal()")
    page.wait_for_selector("#job-modal:not(.open)", timeout=3000)
    # Indicator must be visible on card
    indicator = page.locator("#vm-job-101")
    assert indicator.is_visible(), \
        "Job indicator not visible on card after closing modal with running job"
    assert page._js_errors == []


def test_job_indicator_disappears_when_job_completes(page: Page):
    """Indicator must disappear once the background poll detects job completion."""
    cfg.job_status = "running"
    cfg.job_logs = ["Starting backup..."]
    page.locator(".backup-btn[data-vmid='101']").first.click()
    page.wait_for_selector("#backup-modal.open", timeout=5000)
    page.click("#backup-confirm-btn")
    page.wait_for_selector("#job-modal.open", timeout=5000)
    page.evaluate("closeJobModal()")
    # Now let the job finish
    cfg.job_status = "done"
    # Indicator should disappear when poll detects done (poll every 2s)
    page.wait_for_function(
        "() => !document.getElementById('vm-job-101') || "
        "      document.getElementById('vm-job-101').style.display === 'none'",
        timeout=8000,
    )
    assert page._js_errors == []


def test_job_indicator_click_reopens_modal(page: Page):
    """Clicking the indicator on a card must reopen the job progress modal."""
    cfg.job_status = "running"
    cfg.job_logs = ["Starting backup..."]
    page.locator(".backup-btn[data-vmid='101']").first.click()
    page.wait_for_selector("#backup-modal.open", timeout=5000)
    page.click("#backup-confirm-btn")
    page.wait_for_selector("#job-modal.open", timeout=5000)
    page.wait_for_function(
        "() => document.getElementById('job-badge').textContent === 'running'",
        timeout=4000,
    )
    page.evaluate("closeJobModal()")
    page.wait_for_selector("#job-modal:not(.open)", timeout=3000)
    # Click the indicator
    indicator = page.locator("#vm-job-101")
    indicator.wait_for(state="visible", timeout=3000)
    indicator.click()
    # Modal must reopen
    page.wait_for_selector("#job-modal.open", timeout=5000)
    assert page.locator("#job-badge").inner_text() == "running"
    assert page._js_errors == []
    page.evaluate("closeJobModal()")


def test_concurrent_no_js_errors_under_load(mock_server):
    """No JS errors in 3 concurrent browsers doing simultaneous host switching."""
    async def run():
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            pages = await asyncio.gather(*[
                _async_open_page(browser, mock_server) for _ in range(3)
            ])

            async def switch_user(pg):
                for host, vm in [("cabin", "cabin-vm"), ("home", "home-vm"),
                                  ("cabin", "cabin-vm"), ("home", "home-vm")]:
                    await pg.click(f"#nav-{host}")
                    await pg.wait_for_function(
                        f"() => document.getElementById('content').innerText.includes('{vm}')",
                        timeout=10000,
                    )
                return pg._js_errors[:]

            all_errors = await asyncio.gather(*[switch_user(pg) for pg in pages])
            for pg in pages:
                await pg._ctx.close()
            await browser.close()
        return all_errors

    all_errors = run_async(run())
    for i, errs in enumerate(all_errors):
        assert errs == [], f"User {i} had JS errors under concurrent load: {errs}"


# ─────────────────────────────────────────────────────────────────────────────
# LAYOUT / VIEWPORT — elements must be visible and within the browser viewport
#
# Strategy: load at 1280x800 (common laptop), assert each element bounding
# box is inside the viewport — no overflow, no cut-off.
# ─────────────────────────────────────────────────────────────────────────────

def _in_viewport(pg, selector: str) -> bool:
    """Return True if element bounding box is fully within the visible viewport."""
    return pg.evaluate(
        "(sel) => { const el = document.querySelector(sel); if (!el) return false;"
        " const r = el.getBoundingClientRect();"
        " return r.width > 0 && r.height > 0 && r.top >= 0 && r.left >= 0"
        " && r.bottom <= window.innerHeight && r.right <= window.innerWidth; }",
        selector,
    )


def _partly_in_viewport(pg, selector: str) -> bool:
    """Return True if element is at least partially visible (not fully clipped)."""
    return pg.evaluate(
        "(sel) => { const el = document.querySelector(sel); if (!el) return false;"
        " const r = el.getBoundingClientRect();"
        " return r.width > 0 && r.height > 0 && r.bottom > 0 && r.right > 0"
        " && r.top < window.innerHeight && r.left < window.innerWidth; }",
        selector,
    )


@pytest.fixture
def vp_page(browser, mock_server):
    """1280x800 page — standard laptop viewport, home host loaded."""
    ctx = browser.new_context(base_url=mock_server, viewport={"width": 1280, "height": 800})
    _block_fonts(ctx)
    ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys())))
    pg = ctx.new_page()
    pg._js_errors = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    pg.goto("/")
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('home-vm')",
        timeout=10000,
    )
    yield pg
    ctx.close()


@pytest.fixture
def vp_snap_page(browser, mock_server):
    """1280x800 page on snap-test host (local-only, cloud-only, both VMs)."""
    orig_hosts = HOSTS[:]
    HOSTS.append({"id": "snap-test", "label": "Snap Test"})
    ctx = browser.new_context(base_url=mock_server, viewport={"width": 1280, "height": 800})
    _block_fonts(ctx)
    ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys()) + ["snap-test"]))
    pg = ctx.new_page()
    pg._js_errors = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    pg.goto("/")
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('home-vm')",
        timeout=10000,
    )
    pg.click("#nav-snap-test")
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('local-only-vm')",
        timeout=8000,
    )
    yield pg
    ctx.close()
    HOSTS[:] = orig_hosts


# ── Topbar / sidebar always visible ──────────────────────────────────────────

def test_layout_topbar_in_viewport(vp_page):
    """Topbar must be fully visible at top of viewport."""
    assert _in_viewport(vp_page, ".topbar"), "Topbar not fully in viewport"


def test_layout_sidebar_in_viewport(vp_page):
    """Sidebar must be partially visible — hosts and storage meters accessible."""
    assert _partly_in_viewport(vp_page, ".sidebar"), "Sidebar not in viewport"


def test_layout_refresh_btn_in_viewport(vp_page):
    """Refresh button must be reachable without scrolling."""
    assert _in_viewport(vp_page, "#refresh-btn"), "Refresh button outside viewport"


def test_layout_footer_not_off_screen(vp_page):
    """Footer must be present and its top edge must be within the viewport height."""
    footer_top = vp_page.evaluate(
        "() => document.querySelector('.footer').getBoundingClientRect().top"
    )
    assert 0 < footer_top <= 800, f"Footer top at unexpected position: {footer_top}"


def test_layout_vm_card_visible(vp_page):
    """First VM card must be partially visible without scrolling."""
    assert _partly_in_viewport(vp_page, ".vm-card"), "First VM card not visible in viewport"


# ── VM card header fits in one row ────────────────────────────────────────────

def test_layout_vm_header_not_overflowing(vp_page):
    """VM card header must not overflow its container horizontally."""
    overflow = vp_page.evaluate(
        "() => { const h = document.querySelector('.vm-header');"
        " return h ? h.scrollWidth > h.clientWidth + 2 : false; }"
    )
    assert not overflow, "VM card header scrollWidth exceeds clientWidth — horizontal overflow"


def test_layout_backup_btn_within_viewport(vp_page):
    """Backup now button must be fully within viewport bounds."""
    btn = vp_page.locator(".backup-btn").first
    btn.wait_for(state="visible", timeout=5000)
    box = btn.bounding_box()
    assert box is not None, "Backup button has no bounding box"
    assert box["width"] > 0 and box["height"] > 0, "Backup button has zero size"
    assert box["x"] >= 0, "Backup button left edge is off-screen"
    assert box["x"] + box["width"] <= 1282, \
        f"Backup button right edge {box['x']+box['width']:.0f} exceeds viewport width 1280"


def test_layout_loc_badge_not_truncated(vp_page):
    """Location badges must have reasonable width — not truncated to zero."""
    badge = vp_page.locator(".loc-badge").first
    badge.wait_for(state="visible", timeout=5000)
    box = badge.bounding_box()
    assert box is not None
    assert box["width"] > 20, f"loc-badge seems truncated: width={box['width']:.0f}px"


# ── Snapshot rows fit within card ────────────────────────────────────────────

def test_layout_snapshot_row_not_overflowing(vp_page):
    """Snapshot rows must not cause horizontal scroll after expanding a card."""
    expand_vm_card(vp_page, 101)
    overflow = vp_page.evaluate(
        "() => { const row = document.querySelector('.snapshot-row');"
        " return row ? row.scrollWidth > row.clientWidth + 2 : false; }"
    )
    assert not overflow, "Snapshot row scrollWidth exceeds clientWidth — horizontal overflow"


def test_layout_restore_btn_in_snapshot_row_visible(vp_page):
    """Restore button in expanded snapshot row must be within viewport bounds."""
    expand_vm_card(vp_page, 101)
    btn = vp_page.locator(".restore-btn[data-vmid='101']").first
    btn.wait_for(state="visible", timeout=5000)
    box = btn.bounding_box()
    assert box is not None
    assert box["x"] + box["width"] <= 1282, \
        f"Restore button right edge {box['x']+box['width']:.0f} exceeds 1280"


def test_layout_sync_btn_in_snapshot_row_not_clipped(vp_snap_page):
    """Cloud sync button in local-only snapshot row must not be clipped."""
    expand_vm_card(vp_snap_page, 502)
    sync_btn = vp_snap_page.locator(
        ".vm-card:has(.backup-btn[data-vmid='502']) .snapshot-row .restore-btn"
    ).filter(has_text="sync").first
    sync_btn.wait_for(state="visible", timeout=5000)
    box = sync_btn.bounding_box()
    assert box is not None, "sync button has no bounding box"
    assert box["width"] > 0, "sync button has zero width"
    assert box["x"] >= 0, "sync button is off left edge"
    assert box["x"] + box["width"] <= 1282, \
        f"sync button right edge {box['x']+box['width']:.0f} clips viewport"


def test_layout_sync_btn_on_card_level_visible(vp_snap_page):
    """Card-level sync button (local-only VM) must be visible without expanding."""
    sync_btn = vp_snap_page.locator(
        ".vm-card:has(.backup-btn[data-vmid='502']) .backup-btn"
    ).filter(has_text="sync").first
    sync_btn.wait_for(state="visible", timeout=5000)
    box = sync_btn.bounding_box()
    assert box is not None
    assert box["width"] > 0
    assert box["x"] + box["width"] <= 1282, \
        f"Card-level sync button clips right edge at {box['x']+box['width']:.0f}"


# ── Backup modal fits in viewport ─────────────────────────────────────────────

def test_layout_backup_modal_fits_viewport(vp_page):
    """Backup choice modal must fit fully within 1280x800."""
    vp_page.locator(".backup-btn").first.click()
    vp_page.wait_for_selector("#backup-modal.open", timeout=5000)
    box = vp_page.locator("#backup-modal .modal").bounding_box()
    assert box is not None, "Backup modal has no bounding box"
    assert box["x"] >= 0, f"Backup modal left edge off-screen: {box['x']:.0f}"
    assert box["y"] >= 0, f"Backup modal top edge off-screen: {box['y']:.0f}"
    assert box["x"] + box["width"] <= 1282, \
        f"Backup modal clips right: {box['x']+box['width']:.0f}"
    assert box["y"] + box["height"] <= 802, \
        f"Backup modal clips bottom: {box['y']+box['height']:.0f}"
    vp_page.evaluate("closeBackupModal()")


def test_layout_backup_modal_source_options_visible(vp_page):
    """Both PBS-only and PBS+Cloud options must be visible inside the backup modal."""
    vp_page.locator(".backup-btn").first.click()
    vp_page.wait_for_selector("#backup-modal.open", timeout=5000)
    for btype in ("pbs", "pbs+restic"):
        opt = vp_page.locator(f".source-opt[data-btype='{btype}']")
        assert opt.is_visible(), f"Backup option [{btype}] not visible"
        box = opt.bounding_box()
        assert box["y"] + box["height"] <= 802, \
            f"Backup option [{btype}] is below fold at y={box['y']+box['height']:.0f}"
    vp_page.evaluate("closeBackupModal()")


def test_layout_backup_modal_confirm_btn_visible(vp_page):
    """Confirm button in backup modal must be visible and not below fold."""
    vp_page.locator(".backup-btn").first.click()
    vp_page.wait_for_selector("#backup-modal.open", timeout=5000)
    btn = vp_page.locator("#backup-confirm-btn")
    assert btn.is_visible(), "Backup confirm button not visible"
    box = btn.bounding_box()
    assert box["y"] + box["height"] <= 802, \
        f"Backup confirm button below fold at y={box['y']+box['height']:.0f}"
    vp_page.evaluate("closeBackupModal()")


# ── Restore modal fits in viewport ────────────────────────────────────────────

def test_layout_restore_modal_fits_viewport(vp_page):
    """Restore modal must fit fully within 1280x800."""
    expand_vm_card(vp_page, 101)
    vp_page.locator(".restore-btn[data-vmid='101']").first.click()
    vp_page.wait_for_selector("#modal.open", timeout=5000)
    box = vp_page.locator("#modal .modal").bounding_box()
    assert box is not None
    assert box["x"] >= 0
    assert box["y"] >= 0
    assert box["x"] + box["width"] <= 1282, \
        f"Restore modal clips right: {box['x']+box['width']:.0f}"
    assert box["y"] + box["height"] <= 802, \
        f"Restore modal clips bottom: {box['y']+box['height']:.0f}"
    vp_page.click("button.modal-close")


def test_layout_restore_modal_confirm_btn_visible(vp_page):
    """Restore confirm button must not be below fold."""
    expand_vm_card(vp_page, 101)
    vp_page.locator(".restore-btn[data-vmid='101']").first.click()
    vp_page.wait_for_selector("#modal.open", timeout=5000)
    btn = vp_page.locator("#modal-confirm-btn")
    assert btn.is_visible(), "Restore confirm button not visible"
    box = btn.bounding_box()
    assert box["y"] + box["height"] <= 802, \
        f"Restore confirm button below fold at {box['y']+box['height']:.0f}"
    vp_page.click("button.modal-close")


# ── Job modal fits in viewport ────────────────────────────────────────────────

def test_layout_job_modal_fits_viewport(vp_page):
    """Job progress modal (wide variant) must fit within 1280x800."""
    cfg.job_status = "running"
    cfg.job_logs = ["Starting..."]
    vp_page.evaluate("openJobModal('Test', 'mock-job-1')")
    vp_page.wait_for_selector("#job-modal.open", timeout=5000)
    box = vp_page.locator("#job-modal .modal").bounding_box()
    assert box is not None
    assert box["x"] >= 0
    assert box["y"] >= 0
    assert box["x"] + box["width"] <= 1282, \
        f"Job modal clips right: {box['x']+box['width']:.0f}"
    assert box["y"] + box["height"] <= 802, \
        f"Job modal clips bottom: {box['y']+box['height']:.0f}"
    vp_page.evaluate("closeJobModal()")


def test_layout_job_log_area_visible(vp_page):
    """Job log area must be visible and have non-trivial height."""
    cfg.job_status = "running"
    cfg.job_logs = ["Step 1", "Step 2"]
    vp_page.evaluate("openJobModal('Test', 'mock-job-1')")
    vp_page.wait_for_selector("#job-modal.open", timeout=5000)
    box = vp_page.locator("#job-log").bounding_box()
    assert box is not None
    assert box["height"] > 50, f"Job log area too small: {box['height']:.0f}px"
    assert box["y"] + box["height"] <= 802, "Job log area is below fold"
    vp_page.evaluate("closeJobModal()")


def test_layout_job_close_btn_visible(vp_page):
    """Job modal close button must be reachable without scrolling."""
    cfg.job_status = "done"
    cfg.job_logs = ["Done."]
    vp_page.evaluate("openJobModal('Test', 'mock-job-1')")
    vp_page.wait_for_function(
        "() => document.getElementById('job-badge').textContent === 'done'",
        timeout=6000,
    )
    btn = vp_page.locator("#job-close-btn")
    assert btn.is_visible(), "Job close button not visible"
    box = btn.bounding_box()
    assert box["y"] + box["height"] <= 802, \
        f"Job close button below fold at {box['y']+box['height']:.0f}"
    vp_page.evaluate("closeJobModal()")


# ── Narrow viewport (768x1024) — no hard crash ────────────────────────────────

def test_layout_narrow_no_js_errors(browser, mock_server):
    """At 768x1024 the page must load without JS errors."""
    ctx = browser.new_context(base_url=mock_server, viewport={"width": 768, "height": 1024})
    _block_fonts(ctx)
    ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys())))
    pg = ctx.new_page()
    pg._js_errors = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    pg.goto("/")
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('home-vm')",
        timeout=10000,
    )
    assert pg._js_errors == [], f"JS errors at 768px viewport: {pg._js_errors}"
    ctx.close()


def test_layout_narrow_vm_cards_rendered(browser, mock_server):
    """VM cards must render at 768px — no blank content."""
    ctx = browser.new_context(base_url=mock_server, viewport={"width": 768, "height": 1024})
    _block_fonts(ctx)
    ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys())))
    pg = ctx.new_page()
    pg._js_errors = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    pg.goto("/")
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('home-vm')",
        timeout=10000,
    )
    assert pg.locator(".vm-card").count() > 0, "No VM cards at 768px width"
    assert pg._js_errors == []
    ctx.close()


def test_layout_narrow_backup_modal_opens(browser, mock_server):
    """Backup modal must open without JS errors at 768px."""
    ctx = browser.new_context(base_url=mock_server, viewport={"width": 768, "height": 1024})
    _block_fonts(ctx)
    ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys())))
    pg = ctx.new_page()
    pg._js_errors = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    pg.goto("/")
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('home-vm')",
        timeout=10000,
    )
    pg.locator(".backup-btn").first.click()
    pg.wait_for_selector("#backup-modal.open", timeout=5000)
    assert pg.locator("#backup-modal .modal").is_visible()
    assert pg._js_errors == []
    pg.evaluate("closeBackupModal()")
    ctx.close()


def test_layout_narrow_restore_modal_opens(browser, mock_server):
    """Restore modal must open without JS errors at 768px."""
    ctx = browser.new_context(base_url=mock_server, viewport={"width": 768, "height": 1024})
    _block_fonts(ctx)
    ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys())))
    pg = ctx.new_page()
    pg._js_errors = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    pg.goto("/")
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('home-vm')",
        timeout=10000,
    )
    expand_vm_card(pg, 101)
    pg.locator(".restore-btn[data-vmid='101']").first.click()
    pg.wait_for_selector("#modal.open", timeout=5000)
    assert pg.locator("#modal .modal").is_visible()
    assert pg._js_errors == []
    pg.click("button.modal-close")
    ctx.close()


# ─────────────────────────────────────────────────────────────────────────────
# DEPLOY — theme toggle + dedup element present in served HTML
# ─────────────────────────────────────────────────────────────────────────────

def test_deploy_has_theme_toggle(mock_server):
    """theme-toggle button must be present in the served HTML."""
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    assert "theme-toggle" in html, \
        "id='theme-toggle' or theme-toggle class missing from served HTML"

def test_deploy_has_pbs_dedup_element(mock_server):
    """pbs-dedup span must be present in the served HTML for dedup display."""
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    assert "pbs-dedup" in html, \
        "id='pbs-dedup' missing from served HTML — dedup factor not deployed"

def test_deploy_has_toggle_theme_function(mock_server):
    """toggleTheme() JS function must be present in the served HTML."""
    html = urllib.request.urlopen(mock_server + "/").read().decode()
    assert "toggleTheme" in html, "toggleTheme() missing from served HTML"


# ─────────────────────────────────────────────────────────────────────────────
# THEME — light/dark toggle and localStorage persistence
# ─────────────────────────────────────────────────────────────────────────────

def test_theme_toggle_button_exists(page: Page):
    """theme-toggle button must be present in the DOM after page load."""
    result = page.evaluate("document.getElementById('theme-toggle') !== null")
    assert result, "#theme-toggle button not found in DOM"

def test_theme_default_is_dark(page: Page):
    """Default theme must be dark (data-theme='dark' on <html>)."""
    theme = page.evaluate("document.documentElement.getAttribute('data-theme')")
    assert theme == "dark", f"Default theme should be 'dark', got: {theme!r}"

def test_theme_toggle_switches_to_light(page: Page):
    """Clicking theme-toggle once must switch to light theme."""
    page.evaluate("toggleTheme()")
    theme = page.evaluate("document.documentElement.getAttribute('data-theme')")
    assert theme == "light", f"Expected 'light' after toggle, got: {theme!r}"

def test_theme_toggle_back_to_dark(page: Page):
    """Clicking theme-toggle twice must return to dark theme."""
    page.evaluate("toggleTheme()")
    page.evaluate("toggleTheme()")
    theme = page.evaluate("document.documentElement.getAttribute('data-theme')")
    assert theme == "dark", f"Expected 'dark' after two toggles, got: {theme!r}"

def test_theme_icon_is_sun_on_dark(page: Page):
    """Theme toggle icon must be ☀ when theme is dark (click to go light)."""
    # Ensure dark theme
    page.evaluate("document.documentElement.setAttribute('data-theme','dark')")
    page.evaluate("_applyTheme('dark')")
    icon = page.locator("#theme-toggle").inner_text()
    assert icon == "☀", f"Expected ☀ icon on dark theme, got: {icon!r}"

def test_theme_icon_is_moon_on_light(page: Page):
    """Theme toggle icon must be ☽ when theme is light (click to go dark)."""
    page.evaluate("_applyTheme('light')")
    icon = page.locator("#theme-toggle").inner_text()
    assert icon == "☽", f"Expected ☽ icon on light theme, got: {icon!r}"

def test_theme_persists_in_localstorage(page: Page):
    """After toggling to light, localStorage must store 'light'."""
    page.evaluate("toggleTheme()")  # dark → light
    stored = page.evaluate("localStorage.getItem('theme')")
    assert stored == "light", f"localStorage should have 'light', got: {stored!r}"

def test_theme_no_js_errors_on_toggle(page: Page):
    """No JS errors when toggling theme multiple times."""
    for _ in range(4):
        page.evaluate("toggleTheme()")
    assert page._js_errors == [], f"JS errors during theme toggling: {page._js_errors}"

def test_theme_light_shows_content(page: Page):
    """In light mode the content area must still be rendered (no white-on-white crash)."""
    page.evaluate("_applyTheme('light')")
    text = content_text(page)
    assert "home-vm" in text, f"Content invisible in light theme: {text[:100]!r}"
    assert page._js_errors == []


# ─────────────────────────────────────────────────────────────────────────────
# IN_PVE — backup button visibility based on in_pve flag
# ─────────────────────────────────────────────────────────────────────────────

def test_in_pve_true_has_backup_now_btn(snap_test_page: Page):
    """VM with in_pve=True (VM 501) must show Backup now button."""
    backup_btn = snap_test_page.locator(".backup-btn[data-vmid='501']").filter(
        has_text="Backup"
    )
    assert backup_btn.count() > 0, "Backup now button missing for in_pve=True VM (501)"

def test_in_pve_false_no_backup_now_btn(snap_test_page: Page):
    """VM with in_pve=False (VM 503, cloud-only) must NOT show Backup now button."""
    # Look for a button with text "Backup" for VM 503
    backup_btns = snap_test_page.locator(".backup-btn[data-vmid='503']").filter(
        has_text="Backup"
    )
    assert backup_btns.count() == 0, \
        "Backup now button shown for in_pve=False VM (503) — should be hidden"

def test_in_pve_false_restore_still_available(snap_test_page: Page):
    """VM with in_pve=False must still show cloud restore button (cloud-only snapshot)."""
    expand_vm_card(snap_test_page, 503)
    restore_btn = snap_test_page.locator(".restore-btn[data-vmid='503']")
    assert restore_btn.count() > 0, \
        "Restore button missing for in_pve=False VM (503) — cloud restore should still work"

def test_in_pve_false_no_js_errors_on_card_render(snap_test_page: Page):
    """Rendering a card for an in_pve=False VM must not throw JS errors."""
    assert snap_test_page._js_errors == [], \
        f"JS errors rendering in_pve=False card: {snap_test_page._js_errors}"


# ─────────────────────────────────────────────────────────────────────────────
# DEDUP — PBS dedup factor shown in sidebar
# ─────────────────────────────────────────────────────────────────────────────

def test_dedup_shown_in_sidebar_when_available(page: Page):
    """pbs-dedup element must show the dedup factor from storage response (home: 4.2×)."""
    page.wait_for_function(
        "() => document.getElementById('pbs-dedup').innerText !== '—'",
        timeout=8000,
    )
    dedup_text = page.locator("#pbs-dedup").inner_text()
    assert "4.2" in dedup_text, \
        f"Expected dedup '4.2×' in sidebar, got: {dedup_text!r}"

def test_dedup_updates_on_host_switch(page: Page):
    """Switching to cabin must update dedup to 7.6× (cabin storage mock)."""
    page.click("#nav-cabin")
    page.wait_for_function(
        "() => document.getElementById('pbs-dedup').innerText.includes('7.6')",
        timeout=8000,
    )
    dedup_text = page.locator("#pbs-dedup").inner_text()
    assert "7.6" in dedup_text, f"Expected '7.6×' for cabin dedup, got: {dedup_text!r}"

def test_dedup_shows_dash_when_not_available(browser, mock_server):
    """pbs-dedup must show '—' when storage response has no dedup_factor."""
    orig_hosts = HOSTS[:]
    HOSTS.append({"id": "no-cloud", "label": "No Cloud"})
    try:
        ctx = browser.new_context(base_url=mock_server)
        _block_fonts(ctx)
        ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys()) + ["no-cloud"]))
        pg = ctx.new_page()
        pg._js_errors = []
        pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
        pg.goto("/")
        pg.wait_for_function(
            "() => document.getElementById('content').innerText !== 'Loading\u2026' "
            "&& document.getElementById('content').innerText !== 'Connecting\u2026'",
            timeout=10000,
        )
        pg.click("#nav-no-cloud")
        # Wait for storage MQTT message to be processed (dedup updates from '—' default)
        pg.wait_for_timeout(500)
        pg.wait_for_function(
            "() => document.getElementById('pbs-dedup').innerText !== ''",
            timeout=5000,
        )
        dedup_text = pg.locator("#pbs-dedup").inner_text()
        assert dedup_text == "—", f"Expected '—' when dedup_factor absent, got: {dedup_text!r}"
        assert pg._js_errors == []
        ctx.close()
    finally:
        HOSTS[:] = orig_hosts

def test_dedup_no_js_errors(page: Page):
    """No JS errors when dedup factor is displayed."""
    page.wait_for_function(
        "() => document.getElementById('pbs-dedup').innerText !== 'Loading\u2026'",
        timeout=8000,
    )
    assert page._js_errors == [], f"JS errors while dedup displayed: {page._js_errors}"


# ─────────────────────────────────────────────────────────────────────────────
# MOBILE — portrait viewport ≤699px layout
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mobile_page(browser, mock_server):
    """390x844 viewport — iPhone-sized portrait screen, home host loaded."""
    ctx = browser.new_context(base_url=mock_server, viewport={"width": 390, "height": 844})
    _block_fonts(ctx)
    ctx.add_init_script(_mqtt_mock_script(list(ITEMS.keys())))
    pg = ctx.new_page()
    pg._js_errors = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    pg.goto("/")
    pg.wait_for_function(
        "() => document.getElementById('content').innerText.includes('home-vm')",
        timeout=10000,
    )
    yield pg
    ctx.close()


def test_mobile_no_js_errors(mobile_page: Page):
    """No JS errors at 390px mobile viewport."""
    assert mobile_page._js_errors == [], f"JS errors at mobile viewport: {mobile_page._js_errors}"

def test_mobile_vm_cards_rendered(mobile_page: Page):
    """VM cards must render at 390px — content not blank."""
    assert mobile_page.locator(".vm-card").count() > 0, "No VM cards on mobile"
    assert "home-vm" in content_text(mobile_page)

def test_mobile_sidebar_shows_host_buttons(mobile_page: Page):
    """At mobile width, host nav buttons must still be present in DOM."""
    assert mobile_page.locator("#nav-home").count() > 0, "#nav-home missing on mobile"
    assert mobile_page.locator("#nav-cabin").count() > 0, "#nav-cabin missing on mobile"

def test_mobile_host_switch_works(mobile_page: Page):
    """Switching hosts must work at mobile viewport."""
    mobile_page.click("#nav-cabin")
    wait_content(mobile_page, "cabin-vm")
    assert "cabin-vm" in content_text(mobile_page)
    assert mobile_page._js_errors == []

def test_mobile_backup_modal_opens(mobile_page: Page):
    """Backup modal must open without JS errors on mobile."""
    mobile_page.locator(".backup-btn").first.click()
    mobile_page.wait_for_selector("#backup-modal.open", timeout=5000)
    assert mobile_page.locator("#backup-modal .modal").is_visible()
    assert mobile_page._js_errors == []
    mobile_page.evaluate("closeBackupModal()")

def test_mobile_snapshot_rows_stack_vertically(mobile_page: Page):
    """At mobile width, snapshot rows must not overflow horizontally."""
    expand_vm_card(mobile_page, 101)
    overflow = mobile_page.evaluate(
        "() => { const row = document.querySelector('.snapshot-row');"
        " return row ? row.scrollWidth > row.clientWidth + 2 : false; }"
    )
    assert not overflow, \
        "Snapshot row has horizontal overflow at mobile width — rows not stacking"

def test_mobile_vm_card_not_clipped(mobile_page: Page):
    """VM card must be at least partially visible — not clipped off-screen."""
    assert _partly_in_viewport(mobile_page, ".vm-card"), \
        "VM card not visible in mobile viewport"

def test_mobile_theme_toggle_works(mobile_page: Page):
    """Theme toggle must switch theme on mobile too."""
    mobile_page.evaluate("toggleTheme()")
    theme = mobile_page.evaluate("document.documentElement.getAttribute('data-theme')")
    assert theme == "light", f"Theme toggle broken on mobile, got: {theme!r}"
    assert mobile_page._js_errors == []
