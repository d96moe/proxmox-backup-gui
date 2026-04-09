"""Shared pytest fixtures for all test modules."""
import pytest

try:
    from playwright.sync_api import sync_playwright
    _playwright_available = True
except ImportError:
    _playwright_available = False


@pytest.fixture(scope="session")
def browser():
    """Session-scoped Chromium browser — shared by test_frontend and test_restore.
    A single sync_playwright() instance avoids the 'Sync API inside asyncio loop'
    error that occurs when a second instance is created while the first is active."""
    if not _playwright_available:
        pytest.skip("playwright not installed")
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        yield b
        b.close()
