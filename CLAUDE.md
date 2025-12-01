# CLAUDE.md

## Project Overview

Anypod converts yt-dlp-supported sources (YouTube channels/playlists, Patreon posts, X/Twitter video statuses) into RSS podcast feeds. It runs as a long-lived service that periodically fetches metadata, downloads media files, and generates podcast-consumable RSS feeds.

**Scope**: Self-hosted, small-scale (dozens of feeds), no authentication. RSS feeds and media are publicly accessible; admin interfaces assume trusted local access.

For architecture-heavy changes, consult `DESIGN_DOC.md` after reading this file.

## Behavior

1. **Prioritize focus**: Limit context gathering to the immediate set of files relevant to the requested command or question. Avoid scanning the whole codebase unless explicitly instructed.
2. **Start with direct matches**: First look for direct function/class/file name matches related to the command or topic. Only expand the scope if those are insufficient.
3. **Avoid excessive global context**: Do not recursively walk or analyze all files "just in case"—prefer to incrementally expand as more information is required.
4. **Be cautious of insufficient context**: Do not be too eager to start without at least the core, directly related files loaded. If unsure, ask for clarification or explicit file references rather than assuming broad context.
5. **Balance context size**: Start narrowly (using cues such as file names, directory hints, or explicit function/class usages) but do not limit so much that reasoning/implementation is disconnected from the intended change.

**Example workflow:**

- User asks about a function: Load the file(s) where that function/class is implemented; load related imports/types only if clearly required.
- If asked about architecture: Prefer documentation files (README, DESIGN_DOC.md) and top-level source structure, not all code files.
- When a question seems ambiguous, request the user to specify file or module names before loading the entire codebase.

## Commands

**NOTE**: All commands should be run from the project root folder—never use `cd`.

```bash
# Linting, formatting, type checking (preferred all-in-one)
uv run pre-commit run --all-files

# Individual checks
uv run ruff check        # Lint
uv run ruff format       # Format
uv run basedpyright      # Type checking

# Testing
uv run pytest                  # Fast tests
uv run pytest --integration    # Integration tests (hits real YouTube endpoints, use sparingly)

# yt-dlp research (see example_feeds.yaml for real links)
uvx yt-dlp <url>
```

Use `uv run pre-commit run --all-files` before finalizing work. Use `uv run pytest` (and `--integration` when relevant) to validate behavior.

For dev server/debug scripts, coverage runs, Docker workflows, and code-exploration tips, see `docs/development.md`.

## Architecture

### Core Components

- **Configuration** (`config/`): Pydantic-based multi-source config (env vars → CLI → YAML)
- **Database** (`db/`): Manages download state machine
- **Data Coordinator** (`data_coordinator/`): Orchestrates download lifecycle (Enqueuer → Downloader → Pruner)
- **yt-dlp Wrapper** (`ytdlp_wrapper/`): Handler-based system for different sources
- **RSS Generation** (`rss/`): feedgen-based RSS feed creation
- **Server** (`server/`): FastAPI app serving RSS/media endpoints
- **Scheduler** (`schedule/`): APScheduler-based feed scheduling

### State Management

Download status transitions are implemented as explicit methods in `db/download_db.py` and `db/feed_db.py`. For table layout, indexes, and lifecycle details, see `docs/database.md`.

## Development Notes

### Requirements

- Python 3.14+
- Package manager: `uv` (not pip/poetry)—use `uv add`, `uv sync`, not legacy `uv pip`

### Code Patterns

- Error handling: Structured exceptions with context (e.g. feed_id, download_id)
- Logging: Structured JSON logging with proper context propagation
- Database: Uses SQLModel and SQLAlchemy with a custom `SqlalchemyCore` class
- Currently synchronous but designed for future async conversion
- Function signatures: Default to required parameters; avoid `<type> | None` unless `None` is a real, supported input path

## Key Directories

- `src/anypod/` – Application code (CLI, config, orchestration, server, yt-dlp integration)
- `src/anypod/db/` – DB models and access layer
- `src/anypod/data_coordinator/` – Scheduler phases (enqueuer, downloader, pruner)
- `src/anypod/server/` – FastAPI app, routers, validation
- `src/anypod/rss/` – RSS generation
- `alembic/` – Database migrations

## Configuration

Feeds are defined under `feeds:` with `url`, `schedule` (cron or `"manual"`), optional `yt_args`, retention rules (`since`, `keep_last`), and optional `metadata` overrides.

For full configuration examples, manual feed workflows, and environment variables, see `docs/configuration.md`.

## Code Style Guidelines

### General Principles

- **Follow existing codebase style above all else**
- **Following style applies even when editing non-code as well. In all scenarios, follow existing style**
- Focus on requested functionality—avoid unnecessary refactoring of existing code
- Use type hints consistently with `<type> | None` over `Optional[<type>]`
- Keep functions under 50 lines and focused on single responsibilities
- Scope `try` blocks tightly around specific statements that may raise exceptions
- Bias towards wrapping and re-raising exceptions to the highest possible location
- Exception messages should not include variable data—use existing exception parameters/attributes instead
- When catching and re-raising exceptions, use `raise ... from e` syntax to preserve stack traces
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

- When writing docstrings, never write one for `__init__` fns, since this should be covered by the class level docstring

## Boundaries

### Never

- Commit secrets, API keys, credentials, or `cookies.txt` content
- Modify `alembic/versions/` without reading `docs/database.md` first
- Bypass database state transition methods with raw SQL or generic ORM updates
- Use broad `Exception` catches
- Suppress type errors with `# type: ignore` (unless integrating with an untyped dependency, which we don't have control of)

### Ask First

- Database schema changes (require Alembic migrations)
- Changes to `pyproject.toml` dependencies
- Modifying `alembic/env.py` or migration infrastructure
- Architecture changes spanning multiple components

## Git Workflow

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/) with scope:

```
<type>(<scope>): <imperative summary>

<body explaining what and why>

Closes #<issue> (if exists)
```

**Types**: `feat`, `fix`, `refactor`, `docs`, `chore`, `test`
**Scopes**: `admin`, `api`, `db`, `rss`, `ci`, `docker`, `deps`, etc.

Example:

```
feat(admin): add reset-sync endpoint to adjust feed last_successful_sync

Add POST /admin/feeds/{feed_id}/reset-sync endpoint that allows operators
to manually set a feed's last_successful_sync timestamp...

Closes #79
```

### Branches

- `feat/descriptive-title` — new features
- `fix/descriptive-title` — bug fixes
- `docs/descriptive-title`, `refactor/descriptive-title`, etc.

### Pre-commit

Pre-commit runs automatically as a git hook. **Commits will not succeed if pre-commit fails.** Run manually with:

```bash
uv run pre-commit run --all-files
```
