# Development Guide

Development workflows, scripts, and tooling for Anypod.

## Requirements

- Python 3.14+
- [`uv`](https://docs.astral.sh/uv/) package manager (not pip/poetry)
- ffmpeg and ffprobe

## Running the Application (Dev)

### Full Service

```bash
./scripts/run_dev.sh [--keep]
```

Runs the full Anypod service (scheduler + HTTP server).

- **Default**: Creates fresh database and cleans all downloaded files
- **`--keep`**: Preserves existing database and downloaded files
- Prefer the default behavior of creating a fresh db each time; only use `--keep` if you need to preserve state between runs
- Uses `local_feeds.yaml` config, `tmpdata/` directory, `cookies.txt` file, `US/Eastern` timezone
- This is a long-running process that will not end automatically

### Debug Modes

```bash
./scripts/run_debug.sh <debug_mode> [--keep]
```

Same `--keep` and config/directory behavior as `run_dev.sh`. Runs a single component in isolation instead of the full service.

| Mode         | Description            | Implementation                       |
| ------------ | ---------------------- | ------------------------------------ |
| `ytdlp`      | Test yt-dlp directly   | `src/anypod/cli/debug_ytdlp.py`      |
| `enqueuer`   | Test metadata fetching | `src/anypod/cli/debug_enqueuer.py`   |
| `downloader` | Test download logic    | `src/anypod/cli/debug_downloader.py` |

## Linting, Formatting, and Type Checking

```bash
# All checks (preferred)
uv run pre-commit run --all-files

# Individual tools
uv run ruff check      # Lint
uv run ruff format     # Format
uv run basedpyright    # Type checking
```

All tools are configured in `pyproject.toml`.

## Database Migrations

```bash
bash scripts/check_migrations.sh    # Check for schema drift between models and migrations
```

Validates that SQLAlchemy models and Alembic migrations are in sync.

## Testing

```bash
# Fast tests (no coverage)
uv run pytest

# Integration tests (hits real YouTube endpoints - use sparingly)
uv run pytest --integration

# Full coverage report (use sparingly, runs all tests including integration)
uv run pytest --integration --cov=src --cov-report=html --cov-report=term-missing
```

Coverage reports are generated in `htmlcov/`.

## Docker

### Build Locally

```bash
docker build -t anypod:local .
```

### Run with Docker Compose (recommended)

```bash
docker compose up -d
```

### Run Standalone Container

```bash
docker run -d \
  -p 8024:8024 \
  -v ./config:/config \
  -v ./data:/data \
  -v ./cookies:/cookies \
  ghcr.io/thurstonsand/anypod:nightly
```

## Tool Usage

### yt-dlp Research

```bash
# View real data from YouTube videos for research
uvx yt-dlp <url>
```

See `example_feeds.yaml` for real links to test with.
