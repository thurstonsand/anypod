"""Shared fixtures for integration tests."""

from pathlib import Path

import pytest


@pytest.fixture
def cookies_path() -> Path | None:
    """Provide cookies.txt path if it exists, otherwise None.

    Integration tests can use this fixture to conditionally authenticate
    with YouTube to avoid rate limiting during testing.

    Returns:
        Path to cookies.txt file if it exists, None otherwise.
    """
    cookies_file = Path(__file__).parent / "cookies.txt"
    return cookies_file if cookies_file.exists() else None
