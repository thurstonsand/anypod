# pyright: reportPrivateUsage=false
"""Global pytest configuration for the test suite."""

from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.nodes import Item
import lxml.etree  # noqa: F401  # type: ignore[reportUnusedImport]  # Workaround for feedgen bug (see below)
import pytest

from anypod.logging_config import setup_logging

# The lxml.etree import above is a workaround for a feedgen bug: feedgen/util.py
# does `import lxml` then accesses `lxml.etree` without properly importing the
# submodule. In pytest-xdist workers, feedgen may be imported before any code
# triggers the proper lxml.etree import, causing AttributeError. This pre-import
# ensures lxml.etree is in sys.modules.


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
