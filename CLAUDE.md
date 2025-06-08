# CLAUDE.md

## Project Overview

Anypod converts yt-dlp-supported sources (YouTube channels, playlists) into RSS podcast feeds. It runs as a long-lived service that periodically fetches metadata, downloads media files, and generates podcast-consumable RSS feeds.

see @DESIGN_DOC.md for in depth design doc.

## Commands

### Development Commands
```bash
# Install dependencies and setup
uv sync

# Run application
uv run anypod --config-file example_feeds.yaml

# Debug modes for testing components
DEBUG_MODE=ytdlp uv run anypod --config-file debug.yaml               # Test yt-dlp operations
DEBUG_MODE=enqueuer uv run anypod --config-file example_feeds.yaml    # Test metadata fetching
DEBUG_MODE=downloader uv run anypod --config-file example_feeds.yaml  # Test download operations

# Linting and type checking
uv run ruff check                               # Lint code
uv run ruff format                              # Format code
uv run pyright                                  # Type checking
uv run pre-commit run --all-files               # All of the above, prefer to use this one when confirming your code is good
```

## Architecture

### Core Components
- **Configuration** (`config/`): Pydantic-based multi-source config (env vars → CLI → YAML)
- **Database** (`db/`): SQLite with sqlite-utils wrapper `SqliteUtilsCore`, manages download state machine
- **Data Coordinator** (`data_coordinator/`): Orchestrates the download lifecycle
  - Enqueuer: Fetches metadata, identifies new items
  - Downloader: Downloads media files, manages success/failure
  - Pruner: Implements retention policies, archives old items
- **yt-dlp Wrapper** (`ytdlp_wrapper/`): Handler-based system for different sources
- **File Manager**: Abstracts filesystem operations for future cloud storage
- **RSS Generation** (`rss/`): feedgen-based RSS feed creation

### Database Schema
Single `downloads` table with status lifecycle: `UPCOMING → QUEUED → DOWNLOADED/ERROR → ARCHIVED`
- Composite primary key: `(feed, id)`
- Critical indexes: `idx_feed_status`, `idx_feed_published`

### State Management
Download status transitions are implemented as explicit methods, not generic updates. Always use proper state transition methods in `db/db.py`.

## Development Notes

### Requirements
- Python 3.13+ (uses modern match/case syntax)
- Package manager: `uv` (not pip/poetry) - use modern `uv add`, `uv sync` commands, not legacy `uv pip`

### Code Patterns
- Error handling: Structured exceptions with context (e.g. feed_id, download_id)
- Logging: Structured JSON logging with proper context propagation
- Database: Uses sqlite-utils wrapped in `SqliteUtilsCore` class, not raw SQLite
- Currently synchronous but designed for future async conversion

### Configuration Example
```yaml
feeds:
  channel:
    url: https://www.youtube.com/@example
    yt_args: "-f worst[ext=mp4] --playlist-items 1-3"
    schedule: "0 3 * * *"
    since: "2022-01-01T00:00:00Z"
```

## Code Style Guidelines

### General Principles
- **Follow existing codebase style above all else**
- Focus on requested functionality - avoid unnecessary refactoring of existing code
- Use type hints consistently with `<type> | None` over `Optional[<type>]`
- Keep functions under 50 lines and focused on single responsibilities
- Scope `try` blocks tightly around specific statements that may raise exceptions
- Bias towards wrapping and re-raising exceptions to the highest possible location

### Documentation Requirements
All functions, methods, classes, and files require Google-style docstrings:

```python
def fetch_metadata(
    feed_id: str,
    url: str,
    yt_cli_args: list[str] | None = None,
) -> list[Download]:
    """Return metadata for *url* using **yt‑dlp**.

    Args:
        feed_id: Unique identifier of the parent feed.
        url: Media or playlist URL to inspect.
        yt_cli_args: Extra flags forwarded verbatim to ``yt‑dlp``.

    Returns:
        List of populated :class:`Download` instances.

    Raises:
        YtdlpApiError: When no extractable media is found.
    """
```

## Testing Guidelines

### Test Structure and Organization
- Tests mirror source structure in `/tests/anypod/`
- Integration tests in `/tests/integration/` with `integration_test_` prefix - run with `--integration` flag
- Add `# pyright: reportPrivateUsage=false` to test files for protected method access
- Use pytest markers: either `@pytest.mark.unit` or `@pytest.mark.integration` for all tests
- Import packages directly (e.g., `from anypod.db import Database`) not `from src.anypod`

### Test Writing Patterns
- Use Arrange-Act-Assert pattern without explicit comments
- Descriptive test names that describe behavior
- One assertion per test when possible
- Test both success and failure paths
- Add descriptive messages to non-obvious assertions
- Use `# type: ignore` for private method access complaints

### Test Execution
```bash
# Unit tests only
uv run pytest

# Include integration tests
uv run pytest --integration

# With coverage
uv run pytest --cov=src --cov-report=html
```

### Key Testing Rules
- Mock at appropriate levels using `pytest-mock`
- Don't mock external libraries you don't own
- Avoid string checks on error messages
- Keep tests fast and independent
- Use fixtures in `conftest.py` for shared setup
- Aim for meaningful coverage, not 100% blindly