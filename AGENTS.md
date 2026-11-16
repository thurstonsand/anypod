# AGENTS.md

## Project Overview

Anypod converts yt-dlp-supported sources (YouTube channels/playlists, Patreon posts, X/Twitter video statuses) into RSS podcast feeds. It runs as a long-lived service that periodically fetches metadata, downloads media files, and generates podcast-consumable RSS feeds.

**Scope**: Self-hosted, small-scale (dozens of feeds), no authentication. RSS feeds and media are publicly accessible; admin interfaces assume trusted local access.

For architecture-heavy changes, consult `DESIGN_DOC.md` after reading this file.

## Commands

```bash
# Linting, formatting, type checking (preferred all-in-one)
uv run pre-commit run --all-files

# Testing
uv run pytest                  # Fast tests
uv run pytest --integration    # Integration tests (hits real YouTube endpoints, use sparingly)
```

Use `uv run pre-commit run --all-files` before finalizing work. Use `uv run pytest` (and `--integration` when relevant) to validate behavior.

For individual lint/type tools, dev server/debug scripts, coverage runs, Docker workflows, and yt-dlp research, see `docs/development.md`.

## Architecture

### Core Components

- **Configuration** (`config/`): Pydantic-based multi-source config (env vars → CLI → YAML)
- **Database** (`db/`): Manages download state machine
- **Data Coordinator** (`data_coordinator/`): Orchestrates download lifecycle (Enqueuer → Downloader → Pruner)
- **yt-dlp Wrapper** (`ytdlp_wrapper/`): Handler-based system for different sources
- **RSS Generation** (`rss/`): feedgen-based RSS feed creation
- **Server** (`server/`): FastAPI app serving RSS/media endpoints; see `README.md` for admin API endpoint reference
- **Scheduler** (`schedule/`): APScheduler-based feed scheduling

## Development Notes

### Requirements

- Python 3.14+
- Package manager: `uv` (not pip/poetry)—use `uv add`, `uv sync`, not legacy `uv pip`

### Code Patterns

- Error handling: Structured exceptions with context (e.g. feed_id, download_id)
- Logging: Structured JSON logging with proper context propagation
- Function signatures: Default to required parameters; avoid `<type> | None` unless `None` is a real, supported input path

## Configuration

For full configuration examples, manual feed workflows, and environment variables, see `docs/configuration.md`.

## Code Style Guidelines

### General Principles

- Bias towards wrapping and re-raising exceptions to the highest possible location
- Order functions so each is defined before being referenced within the same file

### Docstring Guidelines

- All functions, methods, classes, and tops of files require Google-style docstrings.
- When writing docstrings, never write one for `__init__` fns, since this should be covered by the class level docstring

## Testing Guidelines

### Test Structure and Organization

- Unit tests should mirror source structure (found in `src/anypod`) in `/tests/anypod/`
- Integration tests go in `/tests/integration/` with `test_integration_` prefix - run with `--integration` flag
- Add `# pyright: reportPrivateUsage=false` to test files for protected method access
- When adding new tests to an existing file:
  - function/method tests should be ordered in the same order as the actual code file
  - if there are already tests for the function you are adding a test for, add the new test next to the others

### Test Writing Patterns

- Use Arrange-Act-Assert pattern
- Do not assert check for the string content of an exception, as this is fragile

### Key Testing Rules

- Mock at appropriate levels using `pytest-mock`
- Don't mock external libraries you don't own
- Keep tests fast and independent
- Aim for meaningful coverage, not 100% blindly
- Include security tests for input validation when testing HTTP endpoints

## Boundaries

### Never

- Bypass database state transition methods with raw SQL or generic ORM updates
- Suppress type errors with `# type: ignore` (unless integrating with an untyped dependency, which we don't have control of)
- Add test-only methods or flags to production code

### Ask First

- Database schema changes
- Changes to `pyproject.toml` dependencies