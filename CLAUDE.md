# CLAUDE.md

## Project Overview

Anypod converts yt-dlp-supported sources (YouTube channels, playlists) into RSS podcast feeds. It runs as a long-lived service that periodically fetches metadata, downloads media files, and generates podcast-consumable RSS feeds.

### Project Scope & Intent
This is a **self-hosted, small-scale solution** designed for personal use or small groups (typically 1-5 users). Key characteristics:

- **Self-hosted only**: Not intended for public cloud deployment or multi-tenant use
- **Limited scale**: Optimized for dozens of feeds, not hundreds or thousands
- **Private admin**: Configuration and management interfaces are not designed for public web exposure
- **RSS-only public access**: Only the RSS feeds and media files are intended to be publicly accessible
- **No authentication**: RSS feeds inherently cannot use authentication, and admin functions assume trusted local access

This design prioritizes simplicity, reliability, and ease of self-hosting over scalability or multi-tenancy.

see @DESIGN_DOC.md for in depth design doc.

## Commands

### Development Commands

**NOTE**: All commands should be run from the project root folder -- never use `cd`

```bash
# Install dependencies and setup
uv sync

# Running the application (dev)
timeout 30 ./scripts/run_dev.sh [--keep] # Run full anypod service (scheduler + HTTP server)
# - Default: Creates fresh database and cleans all downloaded files
# - --keep: Preserves existing database and downloaded files
# - Uses local_feeds.yaml config, tmpdata/ directory, cookies.txt file, US/Eastern timezone
# - This is a long-running process that will not end automatically

# Debug modes for testing individual components
./scripts/run_debug.sh <debug_mode> [--keep]
# - Same --keep and config/directory behavior as run_dev.sh
# - Runs single component in isolation instead of full service

# debug_mode options: ytdlp, enqueuer, or downloader
# See src/anypod/cli/debug_<debug_mode>.py for implementation details


# Linting and type checking
uv run ruff check                               # Lint code
uv run ruff format                              # Format code
uv run pyright                                  # Type checking
uv run pre-commit run --all-files               # All of the above, prefer to use this one when confirming your code is good

# Testing

# Run tests (fast, no coverage)
uv run pytest
# Run integration tests (use sparingly, or target individual tests/files, as these hit real youtube endpoints)
uv run pytest --integration
# Run tests with coverage reporting, including integration tests; use sparingly, and only when running ALL tests
uv run pytest --integration --cov=src --cov-report=html --cov-report=term-missing

# tool use
uvx yt-dlp # Can research/view real data from youtube videos for research. see @example_feeds.yaml for real links

# Key patterns for codebase exploration:
# - Function calls: `sg -p 'function_name($$$ARGS)' -l python src/`
# - Class instantiations: `sg -p 'ClassName($$$ARGS)' -l python src/`
# - Method calls: `sg -p '$OBJ.method_name($$$ARGS)' -l python src/`
# - Import statements: `sg -p 'from $MODULE import $ITEM' -l python src/`
# - Exception handling: `sg -p 'raise $EXCEPTION($$$ARGS)' -l python src/`

# Pattern syntax: `$VAR` = single node, `$$$ARGS` = multiple nodes
# Use for EXPLORATION of code structure, not changes. Prefer over text tools for finding call sites, instantiations, and syntax patterns.
uvx --from ast-grep-cli sg # Syntax-aware code search for exploring codebase structure
```

### Docker Deployment Commands
```bash
# Build Docker image locally
docker build -t anypod:local .

# Run with docker-compose (recommended for production)
docker compose up -d

# Run standalone container
docker run -d -p 8024:8024 -v ./config:/config -v ./data:/data -v ./cookies:/cookies ghcr.io/thurstonsand/anypod:nightly
```

## Architecture

### Core Components
- **Configuration** (`config/`): Pydantic-based multi-source config (env vars → CLI → YAML)
- **Database** (`db/`): SQLite with `SQLModel` and `SQLAlchemy`, manages download state machine
- **Data Coordinator** (`data_coordinator/`): Orchestrates the download lifecycle
  - Enqueuer: Fetches metadata, identifies new items
  - Downloader: Downloads media files, manages success/failure
  - Pruner: Implements retention policies, archives old items
- **yt-dlp Wrapper** (`ytdlp_wrapper/`): Handler-based system for different sources
- **File Manager**: Abstracts filesystem operations for future cloud storage
- **RSS Generation** (`rss/`): feedgen-based RSS feed creation

### Database Schema
- **Tables**:
  - `feed`: Stores feed metadata
  - `download`: Stores download information and status
- Single `downloads` table with status lifecycle: `UPCOMING → QUEUED → DOWNLOADED/ERROR → ARCHIVED`
- Composite primary key: `(feed, id)`
- Critical indexes: `idx_feed_status`, `idx_feed_published`

### State Management
Download status transitions are implemented as explicit methods, not generic updates. Always use proper state transition methods in `db/download_db.py` and `db/feed_db.py`.

## Development Notes

### Requirements
- Python 3.13+ (uses modern match/case syntax)
- Package manager: `uv` (not pip/poetry) - use modern `uv add`, `uv sync` commands, not legacy `uv pip`

### Code Patterns
- Error handling: Structured exceptions with context (e.g. feed_id, download_id)
- Logging: Structured JSON logging with proper context propagation
- Database: Uses `SQLModel` and `SQLAlchemy` with a custom `SqlalchemyCore` class, not raw SQLite.
- Currently synchronous but designed for future async conversion

### Tool Configuration
- Linting and formatting: see @.ruff.toml for ruff configuration
- Type checking: see @pyrightconfig.json for pyright configuration
- Testing: pytest configuration in @pyproject.toml

## File Structure

Whenever you add a new file, or notice a file that is missing from this list, proactively document it here.

> [!note]: ignores `__init__.py` files

```
anypod/
├── src/anypod/
│   ├── __main__.py              # Entry point
│   ├── cli/                     # CLI interface and debug modes
│   │   ├── cli.py               # Main CLI handler
│   │   ├── default.py           # Default CLI mode
│   │   ├── debug_downloader.py  # Debug mode for download testing
│   │   ├── debug_enqueuer.py    # Debug mode for metadata fetching
│   │   └── debug_ytdlp.py       # Debug mode for yt-dlp testing
│   ├── config/                  # Configuration management
│   │   ├── config.py            # Main config loader
│   │   ├── feed_config.py       # Feed-specific configuration
│   │   └── types/               # Config type definitions
│   │       ├── cron_expression.py         # Cron expression type
│   │       ├── feed_metadata_overrides.py # Feed metadata overrides
│   │       ├── podcast_categories.py      # Podcast categories
│   │       └── podcast_explicit.py        # Podcast explicit flag
│   ├── data_coordinator/        # Core orchestration logic
│   │   ├── coordinator.py       # Main coordinator
│   │   ├── downloader.py        # Download logic
│   │   ├── enqueuer.py          # Metadata fetch & enqueue
│   │   ├── pruner.py            # Retention policy implementation
│   │   └── types/               # Coordinator types
│   │       ├── phase_result.py       # Result of a single phase
│   │       └── processing_results.py # Overall processing results
│   ├── db/                      # Database layer
│   │   ├── decorators.py        # DB error handling decorators
│   │   ├── download_db.py       # Download-specific operations
│   │   ├── feed_db.py           # Feed-specific operations
│   │   ├── sqlalchemy_core.py   # SQLAlchemy core
│   │   └── types/               # Database types
│   │       ├── download.py                # Download model
│   │       ├── download_status.py         # Download status enum
│   │       ├── feed.py                    # Feed model
│   │       ├── source_type.py             # Source type enum
│   │       └── timezone_aware_datetime.py # Timezone-aware datetime type
│   ├── rss/                     # RSS feed generation
│   │   ├── feedgen_core.py      # Feed generation logic
│   │   └── rss_feed.py          # `feedgen` wrapper
│   ├── schedule/                # Scheduled feed processing
│   │   ├── apscheduler_core.py  # Type-safe APScheduler wrapper
│   │   └── scheduler.py         # Main feed scheduler using APScheduler
│   ├── server/                  # FastAPI HTTP server
│   │   ├── app.py               # FastAPI app factory
│   │   ├── dependencies.py      # FastAPI dependency providers
│   │   ├── server.py            # HTTP server configuration
│   │   ├── validation.py        # Input validation utilities for endpoints
│   │   └── routers/             # API routers
│   │       ├── health.py        # Health check endpoint
│   │       └── static.py        # Static file serving and directory browsing
│   ├── ytdlp_wrapper/           # `yt-dlp` integration
│   │   ├── base_handler.py      # Base handler interface for different source types
│   │   ├── youtube_handler.py   # YouTube source handler
│   │   ├── ytdlp_wrapper.py     # High-level wrapper
│   │   └── core/                # Core yt-dlp wrapper
│   │       ├── args.py          # yt-dlp argument builder
│   │       ├── core.py          # yt-dlp core runner
│   │       ├── info.py          # yt-dlp info parser
│   │       └── thumbnails.py    # yt-dlp thumbnail parser
│   ├── exceptions.py            # Custom exceptions
│   ├── file_manager.py          # File operations abstraction
│   ├── logging_config.py        # Logging configuration
│   ├── metadata.py              # Metadata utility functions
│   ├── path_manager.py          # Path resolution logic
│   └── state_reconciler.py      # State reconciliation between config and database
├── pyproject.toml               # Package configuration
├── uv.lock                      # Dependency lock file
├── example_feeds.yaml           # Example configuration
├── DESIGN_DOC.md                # High level design of system
├── TASK_LIST.md                 # Work items to build the full system
└── README.md                    # Public documentation
```

### Configuration Example

Full YAML file describing podcasts:

```yaml
feeds:
  channel:
    url: https://www.youtube.com/@example
    yt_args: "-f worst[ext=mp4] --playlist-items 1-3"
    yt_channel: "stable"
    schedule: "0 3 * * *"
    since: "20220101"

  # Feed with full metadata overrides
  premium_podcast:
    url: https://www.youtube.com/@premium/videos
    schedule: "0 6 * * *"
    metadata:
      title: "My Premium Podcast"                                 # Override feed title
      subtitle: "Daily insights and discussions"                  # Feed subtitle
      description: "A daily podcast about technology and culture" # Feed description
      language: "en"                                              # Language code (e.g., 'en', 'es', 'fr')
      author: "John Doe"                                          # Podcast author
      author_email: "john@example.com"                            # Podcast author email
      image_url: "https://example.com/podcast-art.jpg"            # Original podcast artwork URL (min 1400x1400px, will be downloaded and hosted locally)
      podcast_type: "episodic"                                    # Podcast type: "episodic" or "serial"
      explicit: "no"                                              # Explicit content: "yes", "no", or "clean"
      category:                                                   # Apple Podcasts categories (max 2)
        - "Technology"                                            # Main category only
        - "Business > Entrepreneurship"                           # Main > Sub category
        # Alternative formats:
        # - {"main": "Technology"}
        # - {"main": "Business", "sub": "Entrepreneurship"}
        # Or as comma-separated string: "Technology, Business > Entrepreneurship"
```

#### Environment Variables
Configure global application settings via environment variables:
```bash
DEBUG_MODE=enqueuer                    # Debug mode: ytdlp, enqueuer, downloader
LOG_FORMAT=json                        # Log format: human, json (default: human)
LOG_LEVEL=DEBUG                        # Log level: DEBUG, INFO, WARNING, ERROR (default: INFO)
LOG_INCLUDE_STACKTRACE=true           # Include stack traces in logs (default: false)
BASE_URL=https://podcasts.example.com  # Base URL for feeds/media (default: http://localhost:8024)
DATA_DIR=/path/to/data                # Root directory for all application data (default: /data)
CONFIG_FILE=/path/to/feeds.yaml       # Config file path (default: /config/feeds.yaml)
COOKIES_PATH=/path/to/cookies.txt     # Optional cookies.txt file for yt-dlp authentication (default: /cookies/cookies.txt)
SERVER_HOST=0.0.0.0                   # HTTP server host (default: 0.0.0.0)
SERVER_PORT=8024                      # HTTP server port (default: 8024)
TRUSTED_PROXIES=["192.168.1.0/24"]    # Trusted proxy IPs/networks for reverse proxy support (default: None)
```

## Code Style Guidelines

### General Principles
- **Follow existing codebase style above all else**
- **Following style applies even when editing non-code as well. In all scenarios, follow existing style**
- Focus on requested functionality - avoid unnecessary refactoring of existing code
- Use type hints consistently with `<type> | None` over `Optional[<type>]`
- Keep functions under 50 lines and focused on single responsibilities
- Scope `try` blocks tightly around specific statements that may raise exceptions
- Bias towards wrapping and re-raising exceptions to the highest possible location
- Exception messages should not include variable data - use existing exception parameters/attributes instead
- When catching and re-raising exceptions, use `raise ... from e` syntax to preserve stack traces
- Catch specific exceptions rather than broad `Exception` catches
- When adding new code/features, don't reference them with "new logic", "new field", etc. It is going to live in the code base for a long time past "new"
- Order functions so each is defined before being referenced within the same file
- Imports should always be at the top of the file, unless you are specifically trying to lazy load

### Docstring Guidelines
- All functions, methods, classes, and tops of files require Google-style docstrings:

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

- When writing docstrings, never write one for __init__ fns, since this should be covered by the class level docstring
