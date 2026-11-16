# pyright: reportPrivateUsage=false
"""Global pytest configuration for the test suite."""

from pathlib import Path

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


INTEGRATION_DIR = Path(__file__).parent / "integration"


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


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config: Config, items: list[Item]) -> None:
    """Mark each test by its directory and skip integration tests by default.

    Tests under ``tests/integration/`` get the ``integration`` marker and every
    other test gets ``unit``. Running first lets ``-m`` select on these markers.
    Integration tests are skipped unless the ``--integration`` flag is provided.
    """
    run_integration = config.getoption("--integration")
    skip_integration = pytest.mark.skip(reason="need --integration option to run")
    for item in items:
        if item.path.is_relative_to(INTEGRATION_DIR):
            item.add_marker(pytest.mark.integration)
            if not run_integration:
                item.add_marker(skip_integration)
        else:
            item.add_marker(pytest.mark.unit)
