# Anypod

[![License](https://img.shields.io/github/license/thurstonsand/anypod)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/thurstonsand/anypod/ci.yml?branch=main)](https://github.com/thurstonsand/anypod/actions/workflows/ci.yml)

Your self-hosted, YAML-driven bridge from yt-dlp–supported sources (YouTube channels and playlists) to podcast‑consumable RSS feeds. Runs as a long‑lived service that periodically downloads media and serves RSS and media over HTTP.

> Designed for small, self‑hosted deployments. Admin is private/trusted; only RSS and media endpoints are for public access.

## Table of contents

- [High Level](#high-level)
- [Features](#features)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Environment variables](#environment-variables)
- [HTTP endpoints](#http-endpoints)
- [Reverse proxies](#reverse-proxies)
- [Limitations and notes](#limitations-and-notes)
- [Development](#development)
- [Architecture](#architecture)
- [Roadmap](#roadmap)
- [FAQ / Troubleshooting](#faq--troubleshooting)
- [License](#license)
- [Acknowledgements](#acknowledgements)


## High Level

Anypod is a thin Python wrapper around `yt‑dlp` that turns any `yt‑dlp`–supported source into an RSS feed consumable by podcast players. You declare feeds in YAML with a cron schedule and optional `yt‑dlp` format rules; Anypod periodically:

1. Discovers new items from channels or playlists
2. Enqueues and downloads media files
3. Prunes any out-of-scope media according to retention rules
4. Regenerates RSS feeds and serves both the feeds and the media over HTTP

## Features

- Simple YAML config with per‑feed schedules
- Works with YouTube channels and playlists
- Feed metadata overrides (title, description, artwork, categories, explicit, etc.)
- Retention policies: keep the last N items and/or only since YYYYMMDD
- Docker image with non‑root (PUID/PGID) support

## Quick start

### Using Docker Compose (recommended)

```yaml
services:
  anypod:
    image: ghcr.io/thurstonsand/anypod:latest # or 'nightly' for tracking main branch
    container_name: anypod
    restart: unless-stopped
    ports:
      - "8024:8024"
    volumes:
      - ./example_feeds.yaml:/config/feeds.yaml
      - ./data:/data
      - ./cookies.txt:/cookies/cookies.txt # optional
      - /etc/localtime:/etc/localtime:ro
    environment:
      PUID: 1000
      PGID: 1000
      LOG_FORMAT: json
      LOG_LEVEL: INFO
      LOG_INCLUDE_STACKTRACE: "false"
      BASE_URL: https://reverseproxy.example
      SERVER_PORT: 8024
```

Start it:

```bash
docker compose up -d
```

- See all available feeds at `https://reverseproxy.example/feeds`
- Subscribe in your podcast app to `https://reverseproxy.example/feeds/<feed_id>.xml`


### Using Docker directly

```bash
docker run -d \
  --name anypod \
  -p 8024:8024 \
  -v ./example_feeds.yaml:/config/feeds.yaml \
  -v ./data:/data \
  -v ./cookies.txt:/cookies/cookies.txt \
  ghcr.io/thurstonsand/anypod:latest # or nightly
```


## Configuration

Put your feeds in a YAML file. Example:

```yaml
feeds:
  channel:
    url: https://www.youtube.com/@example
    yt_args: "-f best[ext=mp4]"
    schedule: "0 3 * * *"
    since: "20220101"
  favorite_podcast:
    url: https://www.youtube.com/@favorite_podcast # will default to the 'videos' feed/playlist
    schedule: "0 6 * * *"
    yt_args: "-f best[ext=mp4]"
    metadata:
      title: "My Premium Podcast"
      subtitle: "Daily insights and discussions"
      description: "A daily podcast about technology and culture"
      language: "en"
      author: "John Doe"
      image_url: "https://example.com/podcast-art.jpg"
      explicit: "no"
      category:
        - "Technology"
        - "Business > Entrepreneurship"
```

Notes:

- `schedule` accepts a [cron expression](https://crontab.cronhub.io/)
- `since` must be in the format `YYYYMMDD` (day‑precision; see Limitation below).
- `yt_args` are passed directly to the [`yt-dlp` program](https://github.com/yt-dlp/yt-dlp); see their docs for full options, keeping note of the options below you cannot use (or risk breaking Anypod)

Reserved/managed `yt‑dlp` options (set by Anypod, do not override):

- Metadata: `--dump-json`, `--dump-single-json`, `--flat-playlist`, `--skip-download`, `--quiet`, `--no-warnings`
- Filtering/iteration: `--break-match-filters`, `--lazy-playlist`, playlist limits derived from `keep_last` and `since`
- Paths/output: `--paths`, `--output "<download_id>.%(ext)s"`
- Thumbnails: conversion to `jpg` is enforced

## Environment variables

All can be provided via env or CLI flags (kebab‑case). Common ones:

| Name | Default | Description |
| ---- | ------- | ----------- |
| `BASE_URL` | `http://reverseproxy.example:8024` | Public base URL for feed/media links (set this behind a reverse proxy) |
| `SERVER_PORT` | `8024` | Bind port |
| `TRUSTED_PROXIES` | unset | List of local IPs or networks allowed to access the server, mainly for reverse proxy use (e.g. `["192.168.1.0/24"]`) |
| `TZ` | unset | Your timezone (set if you don't want to mount `/etc/localtime`) |
| `LOG_FORMAT` | `json` | `human` or `json` |
| `LOG_LEVEL` | `INFO` | Log level |
| `LOG_INCLUDE_STACKTRACE` | `false` | Include stack traces in error logs |
| `PUID` | `1000` | Container user |
| `PGID` | `1000` | Container group |


## HTTP endpoints

- `GET /feeds` – directory listing of feeds
- `GET /feeds/{feed_id}.xml` – podcast RSS
- `GET /media` – directory listing of feeds with media
- `GET /media/{feed_id}` – directory listing of media files for a feed
- `GET /media/{feed_id}/{filename}.{ext}` – media file download
- `GET /api/health` – health check

No authentication is implemented. Only expose `/feeds` and `/media` publicly (they're the only ones available for now).

## Reverse proxies

Set `BASE_URL` to your public URL and configure `TRUSTED_PROXIES` if running behind a reverse proxy so that link generation and client IP handling are correct.

## Limitations and Notes

### Scheduling and Rate Limiting

Even if you set 2 feeds to have the same schedule, it will only ever run one at a time; this is a simple way to ensure we stay under rate limits.

On the subject of rate limiting, Youtube can be fairly aggressive, and you may find your downloads failing. You can get much higher rate limits [using cookies](#cookies) with a logged in account, but Youtube does reserve the right to ban your account if it detects excessive bot activity, so I would recommend using a burner account. Reports I've seen online say that you can download pretty aggressively, like hundreds of videos an hour, and still not get flagged, but better safe than sorry.

### Cookies

In order to get cookies, I have successfully followed these instructions:

- [Exporting Youtube cookies](https://github.com/yt-dlp/yt-dlp/wiki/Extractors#exporting-youtube-cookies)
- [How to pass cookies to yt-dlp](https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp)
- [Error 429: Too many requests](https://github.com/yt-dlp/yt-dlp/wiki/FAQ#http-error-429-too-many-requests-or-402-payment-required)
- a couple comments:
- specifically, I've used the [Get cookies.txt LOCALLY](https://chromewebstore.google.com/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdbgdldlbecc) Chrome extension to retrieve them in a file
- if you are on Windows, watch out for the newlines. The Docker container will expect `LF`, and Windows might default to `CRLF`
- With youtube cookies, I have seen that this actually blocks you from even seeing "Premium" (enhanced bitrate) videos; this is a known problem
    - There might be a way around it if you use a PO Token Provider, but it is nontrivial; see [yt-dlp docs](https://github.com/yt-dlp/yt-dlp/wiki/PO-Token-Guide) and [recommended provider](https://github.com/Brainicism/bgutil-ytdlp-pot-provider)
    - Probably best to just choose from whatever options you do get -- even without cookies, I mostly just got 403's when trying to download Premium anyway

### Pocket Casts

For users of Pocket Casts, I would not test out feeds in the app, since they permanently cache on their end. If you get any configuration wrong, you'll be stuck with it until you change the feed's id, which will generate a new url.

If you do want to test you have everything configured correctly (which I recommend), I have found Apple Podcasts to be just fine.

### Filtering

I recommend using a filter (either `since` or `keep_last`) when setting up your feed, otherwise Anypod will download EVERY video in the playlist. On that note, `yt-dlp` only allows for day precision filtering (YYYYMMDD), tho this should be sufficient for most people.

## Development

Requirements: Python 3.13+, [`uv`](https://docs.astral.sh/uv/) package manager.

```bash
# Install deps
uv sync

# Run full service (dev)
timeout 30 ./scripts/run_dev.sh [--keep]

# Run debug component
./scripts/run_debug.sh <enqueuer|downloader|ytdlp> [--keep]

# Lint/format/type-check/tests
uv run pre-commit run --all-files
uv run ruff check && uv run ruff format
uv run pyright
uv run pytest
uv run pytest --integration
```

Local defaults in dev scripts:

- Database and media under `tmpdata/`
- Config from `local_feeds.yaml`
- Cookies from `cookies.txt`
- Timezone `US/Eastern`

## Architecture

High‑level components:

- Configuration: Pydantic settings; YAML + env + CLI
- Database: SQLite via SQLModel/SQLAlchemy (async) with Alembic migrations
- Data Coordinator: Enqueuer → Downloader → Pruner → RSS generation
- yt‑dlp wrapper: async subprocess invocations with typed parsing
- File/Path management: consistent on‑disk and URL mapping
- HTTP server: FastAPI + Uvicorn serving RSS/media endpoints

For details, see `DESIGN_DOC.md`.

## Roadmap

High‑level upcoming work. See `TASK_LIST.md` for the full checklist.

- Admin Dashboard
- Advanced video conversion
- Grab transcripts, potentially add in-app transcription when not provided externally
- Grab timestamps and convert to chapters
- Include global size limit such that all podcasts across all feeds don't exceed a certain size
- Integrate sponsorblock to automatically cut out or add chapter markers for ads
- Podcast feed with an endpoint you can send videos to, to dynamically create your own playlist
  - You can recreate this functionality now by creating an unlisted youtube playlist and add videos to it
- Download and then self-host feed/podcast images
- Support for other sources (e.g. Patreon)

## FAQ / Troubleshooting

- 429/403 from YouTube: back off your schedule or use cookies. Some content (e.g., YouTube Premium) may 403; cookies can also reduce available qualities.
- Certain yt‑dlp flags get ignored: see the Reserved options list above.
- Where are files and DB?
  - DB: `${DATA_DIR}/db/anypod.db`
  - Media: `${DATA_DIR}/media/<feed_id>/<download_id>.<ext>`