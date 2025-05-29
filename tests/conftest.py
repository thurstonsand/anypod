"""Pytest configuration and hooks for the Anypod test suite."""

from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.nodes import Item
import pytest

from anypod.logging_config import setup_logging


def pytest_addoption(parser: Parser) -> None:
    """Add custom command line options for pytest."""
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run integration tests",
    )


def pytest_configure() -> None:
    """Configure pytest setup including logging configuration."""
    setup_logging(
        log_format_type="human", app_log_level_name="INFO", include_stacktrace=False
    )


def pytest_collection_modifyitems(config: Config, items: list[Item]) -> None:
    """Skip integration tests unless --integration flag is provided."""
    if config.getoption("--integration"):
        return
    skip_integration = pytest.mark.skip(reason="need --integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
