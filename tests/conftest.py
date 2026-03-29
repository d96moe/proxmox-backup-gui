"""Shared pytest fixtures for all test modules."""
import pytest
from playwright.sync_api import sync_playwright


@pytest.fixture(scope="session")
def browser():
    """Session-scoped Chromium browser — shared by test_frontend and test_restore.
    A single sync_playwright() instance avoids the 'Sync API inside asyncio loop'
    error that occurs when a second instance is created while the first is active."""
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        yield b
        b.close()
