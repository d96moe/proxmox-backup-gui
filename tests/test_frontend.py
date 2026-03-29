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
from http.server import BaseHTTPRequestHandler, HTTPServer

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
             "snapshots": [{"backup_time": 1700000000, "date": "2023-11-14 22:13:20",
                            "age": "1h ago", "local": True, "cloud": True,
                            "incremental": True, "size": "1.2 GB"}]}
        ],
        "lxcs": [
            {"id": 104, "name": "home-lxc", "type": "lxc", "os": "linux", "status": "running",
             "snapshots": [{"backup_time": 1700000010, "date": "2023-11-14 22:13:30",
                            "age": "1h ago", "local": True, "cloud": True,
                            "incremental": True, "size": "500 MB"}]}
        ],
    },
    "cabin": {
        "storage": {"local_used": 200, "local_total": 800, "cloud_used": 0},
        "vms": [
            {"id": 201, "name": "cabin-vm", "type": "vm", "os": "linux", "status": "running",
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
             "snapshots": []}
        ],
    },
}

STORAGE = {
    "home":  {"local_used": 100, "local_total": 500, "cloud_used": 50,
              "cloud_total": 2000, "cloud_quota_used": 738},
    "cabin": {"local_used": 200, "local_total": 800, "cloud_used": 50,
              "cloud_total": 2000, "cloud_quota_used": 738},
    # no-cloud: cloud_total is absent/null
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

        for host_id in ("home", "cabin", "empty", "lxc-only", "no-cloud"):
            if path == f"/api/host/{host_id}/items":
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

        # Serve frontend
        import os
        frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
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


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def mock_server():
    server = HTTPServer(("127.0.0.1", 0), MockHandler)
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
    yield


@pytest.fixture
def page(browser, mock_server):
    """Fresh page, loaded and ready (home host rendered)."""
    ctx = browser.new_context(base_url=mock_server)
    pg = ctx.new_page()
    # Capture JS console errors so tests can assert on them
    pg._js_errors: list[str] = []
    pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
    pg.goto("/")
    pg.wait_for_function(
        "() => document.getElementById('content').innerText !== 'Loading…'",
        timeout=10000,
    )
    yield pg
    ctx.close()


@pytest.fixture
def blank_page(browser, mock_server):
    """Page that has NOT loaded yet — lets tests control timing."""
    ctx = browser.new_context(base_url=mock_server)
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


# ─────────────────────────────────────────────────────────────────────────────
# DEPLOY — the tests that would have caught the actual prod bug
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# DOM — required elements exist, no JS errors thrown during page lifecycle
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_IDS = [
    "content", "hostname-label", "page-subtitle",
    "local-usage", "local-bar", "cloud-usage", "cloud-bar",
    "refresh-btn", "hosts-section",
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
    """If /items returns 500, UI must show error message — not stay frozen on Loading…"""
    cfg.items_status = 500
    pg, server = blank_page
    pg.goto(server)
    # Must NOT stay on Loading… indefinitely
    pg.wait_for_function(
        "() => !document.getElementById('content').innerText.includes('Loading') || "
        "      document.getElementById('content').innerText.includes('Error')",
        timeout=8000,
    )
    text = content_text(pg)
    assert "Loading" not in text, f"Still showing Loading… after backend 500: {text!r}"
    assert "Error" in text or "error" in text.lower(), \
        f"Expected error message after 500, got: {text!r}"

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
    """Content element must show Loading… synchronously on host click,
    before any fetch response arrives."""
    cfg.items_delay = 2.0  # slow all /items
    page.evaluate("document.getElementById('nav-cabin').click()")
    # Immediately after click (same JS tick), content should be Loading…
    text = content_text(page)
    assert "Loading" in text or "cabin" in text, \
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
        pg = ctx.new_page()
        pg._js_errors = []
        pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
        pg.goto("/")
        pg.wait_for_function(
            "() => document.getElementById('content').innerText !== 'Loading…'",
            timeout=10000,
        )
        pg.click("#nav-empty")
        pg.wait_for_function(
            "() => !document.getElementById('content').innerText.includes('Loading')",
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
        pg = ctx.new_page()
        pg._js_errors = []
        pg.on("pageerror", lambda e: pg._js_errors.append(str(e)))
        pg.goto("/")
        pg.wait_for_function(
            "() => document.getElementById('content').innerText !== 'Loading…'",
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
