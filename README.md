# Anypod

[![License](https://img.shields.io/github/license/thurstonsand/anypod)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/thurstonsand/anypod/ci.yml?branch=main)](https://github.com/thurstonsand/anypod/actions/workflows/ci.yml)

Your self-hosted, YAML-driven bridge from `yt-dlp`–supported sources (YouTube channels/playlists, Patreon creators/posts, X/Twitter video statuses, and arbitrary `yt-dlp` sites) to podcast‑consumable RSS feeds. Runs as a long‑lived service that periodically fetches metadata, downloads and normalizes media, and serves RSS and media over HTTP.

> Designed for small, self‑hosted deployments. Admin endpoints assume trusted local access; only RSS and media endpoints are meant for public access.

## Table of contents

- [Anypod](#anypod)
  - [Table of contents](#table-of-contents)
  - [High Level](#high-level)
  - [Features](#features)
  - [Quick start](#quick-start)
    - [Using Docker Compose (recommended)](#using-docker-compose-recommended)
    - [Using Docker directly](#using-docker-directly)
  - [Configuration](#configuration)
    - [Media conversion presets](#media-conversion-presets)
    - [Transcript configuration](#transcript-configuration)
  - [Environment variables](#environment-variables)
  - [HTTP endpoints](#http-endpoints)
    - [Public endpoints](#public-endpoints)
    - [Admin endpoints (trusted/local access only)](#admin-endpoints-trustedlocal-access-only)
  - [Reverse proxies](#reverse-proxies)
  - [Limitations and Notes](#limitations-and-notes)
    - [Scheduling and Rate Limiting](#scheduling-and-rate-limiting)
    - [Cookies](#cookies)
    - [PO Tokens (YouTube)](#po-tokens-youtube)
    - [Pocket Casts](#pocket-casts)
    - [Filtering](#filtering)
    - [X / Twitter](#x--twitter)
    - [Patreon](#patreon)
    - [Other yt-dlp Sources](#other-yt-dlp-sources)
    - [Error Handling and Logging](#error-handling-and-logging)
  - [Development](#development)
  - [Architecture](#architecture)
  - [Roadmap](#roadmap)
  - [FAQ / Troubleshooting](#faq--troubleshooting)

## High Level

Anypod is a thin Python wrapper around `yt-dlp` and `ffmpeg` that turns `yt-dlp`–supported sources into RSS feeds consumable by podcast players. You declare feeds in YAML with a cron schedule (or `manual`) and optional global `defaults:`; Anypod periodically:

1. Discovers new items from channels, playlists, or creator pages
2. Enqueues, downloads, and converts media into podcast-friendly MP4/M4A files (with embedded episode artwork and fast-start moov atoms)
3. Prunes out-of-scope media according to retention rules (`keep_last`, `since`)
4. Regenerates RSS feeds and serves feeds, media, artwork, and transcripts over HTTP

## Features

- Simple YAML config with top-level `defaults:` and per-feed overrides
- First-class support for YouTube channels/playlists, Patreon creator pages/posts (video + audio-only fallback + `prefer_audio`), and X/Twitter video posts, plus best-effort support for arbitrary `yt-dlp` sites
- First-class media conversion presets (`light`, `optimized`, `off`, `custom`) with fast-start MP4 remuxing, embedded episode artwork, and custom post-processing hook support
- Feed metadata overrides (title, description, artwork, categories, explicit, etc.)
- Thumbnail hosting: Downloads and serves feed artwork and episode thumbnails locally
- Transcript/subtitle support: Downloads and serves transcripts via `<podcast:transcript>` tags (VTT format), and soft-embeds them as `mov_text` subtitle tracks in MP4 video containers
- Retention policies: keep the last N items (`keep_last`) and/or only items since a cutoff date (`since`)
- Manual submission feeds: declare `schedule: "manual"` and push ad-hoc URLs via the admin API
- Non-blocking startup: HTTP server and health checks come up immediately while upstream feed reconciliation runs in the background
- Docker image with non-root (`PUID`/`PGID`) support

## Quick start

### Using Docker Compose (recommended)

Copy `example_feeds.yaml` to `feeds.yaml`, edit it with your channels, and start Anypod:

```bash
cp example_feeds.yaml feeds.yaml
```

```yaml
services:
  anypod:
    image: ghcr.io/thurstonsand/anypod:latest # or 'nightly' to track main
    container_name: anypod
    restart: unless-stopped
    ports:
      - "8024:8024" # Public RSS/media server
      - "127.0.0.1:8025:8025" # Admin API (bound to localhost only)
    volumes:
      - ./feeds.yaml:/config/feeds.yaml:ro
      - ./data:/data
      # - ./cookies.txt:/cookies/cookies.txt:ro # optional; mount only if you need cookies
      - /etc/localtime:/etc/localtime:ro
    environment:
      PUID: 1000
      PGID: 1000
      SERVER_PORT: 8024
      ADMIN_SERVER_PORT: 8025

      # Set to the URL your podcast app reaches (required for external apps)
      BASE_URL: http://localhost:8024

      # Optional: mount cookies.txt above and uncomment:
      # COOKIES_PATH: /cookies/cookies.txt

      # Optional: reverse proxy trusted IPs/CIDRs (comma-separated or JSON array)
      # TRUSTED_PROXIES: "192.168.1.0/24"

      # Optional: timezone for cron schedules and `since` dates
      # TZ: "America/New_York"

      # Optional: YouTube PO Token provider
      POT_PROVIDER_URL: http://bgutil-provider:4416
    depends_on:
      bgutil-provider:
        condition: service_healthy

  bgutil-provider:
    image: brainicism/bgutil-ytdlp-pot-provider:latest
    container_name: bgutil-provider
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:4416/ping || exit 1"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 10s
```

Start it:

```bash
docker compose up -d
```

- Browse available feeds at `http://localhost:8024/feeds` (or your `BASE_URL`)
- Subscribe in your podcast app to `<BASE_URL>/feeds/<feed_id>.xml`
- Explore the interactive Admin API docs at `http://127.0.0.1:8025/docs`

### Using Docker directly

```bash
docker run -d \
  --name anypod \
  -p 8024:8024 \
  -p 127.0.0.1:8025:8025 \
  -v ./feeds.yaml:/config/feeds.yaml:ro \
  -v ./data:/data \
  -e BASE_URL=http://localhost:8024 \
  ghcr.io/thurstonsand/anypod:latest
```

To use cookies with `docker run`, add `-e COOKIES_PATH=/cookies/cookies.txt -v ./cookies.txt:/cookies/cookies.txt:ro`.

## Configuration

Put your feeds in `/config/feeds.yaml`. Shared settings can be defined once under top-level `defaults:` and overridden per feed:

```yaml
defaults:
  media_conversion: light # off | light (default) | optimized | custom
  keep_last: 20
  transcript_lang: en

feeds:
  my_channel:
    url: https://www.youtube.com/@example
    schedule: "@daily"
    since: "2024-01-01"

  favorite_podcast:
    url: https://www.youtube.com/@favorite_podcast
    schedule: "0 6 * * *"
    media_conversion: optimized # HEVC hvc1 + AAC for smaller files
    metadata:
      title: "My Favorite Podcast"
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

- `schedule` accepts standard 5-field [cron expressions](https://crontab.cronhub.io/) (with POSIX Sunday `0`/`7` handled accurately in your configured `TZ`), `@hourly`/`@daily`/`@weekly`/`@monthly`, or `"manual"` for ad-hoc feeds
- `since` accepts `YYYY-MM-DD`, `YYYYMMDD`, or unquoted YAML dates (day precision)
- `image_url` overrides feed artwork (downloaded and hosted locally)
- `yt_args` passes extra options directly to [`yt-dlp`](https://github.com/yt-dlp/yt-dlp)

### Media conversion presets

Anypod converts downloaded media into podcast-player-compatible files automatically (default: `light`):

- **`light` (default)**: Prefers H.264/AAC streams from `yt-dlp`, stream-copies H.264/HEVC/AV1 video without re-encoding (only transcoding VP9/incompatible codecs), encodes non-AAC audio to 128k AAC, remuxes into MP4 with `-movflags +faststart`, and embeds episode artwork. Audio-only files are preserved as `.m4a`.
- **`optimized`**: Re-encodes video to compact HEVC (`libx265`, `-tag:v:0 hvc1`, CRF 28) + 128k AAC + `+faststart` + embedded artwork for Apple Podcasts / Pocket Casts / mobile devices.
- **`off`**: Keeps raw `yt-dlp` output untouched.
- **`custom`**: Runs your own executable script (default `/config/post_process.sh`, or `MEDIA_CONVERSION_SCRIPT`) in an isolated per-download staging directory.

See [docs/configuration.md](docs/configuration.md#media-conversion) for full preset details, `embed_thumbnail`, `prefer_audio`, and the custom hook contract.

### Transcript configuration

Add `transcript_lang` to a feed (or `defaults:`) to fetch subtitles/transcripts (VTT format) and emit `<podcast:transcript>` tags. Transcripts are served from `/transcripts/{feed_id}/{download_id}.{lang}.vtt`. For MP4-family video files (`.mp4`, `.m4v`, `.mov`), Anypod also soft-embeds the transcript as a `mov_text` subtitle track with the matching ISO 639-2/T language tag.

Control preference with `transcript_source_priority` (defaults to `['creator', 'auto']`).

For full configuration details including `defaults:`, manual feeds, metadata overrides, and reserved `yt-dlp` options, see [docs/configuration.md](docs/configuration.md).

## Environment variables

All settings can be provided via environment variables or CLI flags (kebab-case). Common ones:

| Name                      | Default                   | Description                                                            |
| ------------------------- | ------------------------- | ---------------------------------------------------------------------- |
| `BASE_URL`                | `http://localhost:8024`   | Public base URL for feed/media links (supports subpath prefixes)       |
| `SERVER_PORT`             | `8024`                    | Bind port for public server                                            |
| `ADMIN_SERVER_PORT`       | `8025`                    | Bind port for admin server (keep private)                              |
| `SINGLE_SERVER_MODE`      | `false`                   | Mount admin routes on main server (requires external path gating)      |
| `TRUSTED_PROXIES`         | unset                     | Trusted proxy IPs/CIDRs (comma-separated, `*`, or JSON array)          |
| `COOKIES_PATH`            | unset                     | Path to `cookies.txt` file for authenticated sources                   |
| `POT_PROVIDER_URL`        | unset                     | `bgutil` POT provider URL for YouTube PO Tokens                        |
| `MEDIA_CONVERSION_SCRIPT` | `<config>/post_process.sh`| Custom executable hook path for `media_conversion: custom`             |
| `LOG_FORMAT`              | `human`                   | Log output format (`human` or `json`)                                  |
| `TZ`                      | system local / `UTC`      | Timezone for cron schedules and `since` dates                          |
| `PUID` / `PGID`           | `1000`                    | Container user/group                                                   |

For the complete list of environment variables, see [docs/configuration.md](docs/configuration.md#environment-variables).

## HTTP endpoints

### Public endpoints

- `GET /` – redirects (`307`) to `/feeds`
- `GET /feeds` – directory listing of enabled feeds
- `GET /feeds/{feed_id}.xml` – podcast RSS
- `GET /media` – directory listing of feeds with media
- `GET /media/{feed_id}` – directory listing of media files for a feed
- `GET /media/{feed_id}/{filename}.{ext}` – media file download
- `GET /images/{feed_id}.{ext}` – feed artwork/thumbnail
- `GET /images/{feed_id}/{download_id}.{ext}` – episode thumbnail
- `GET /transcripts/{feed_id}/{download_id}.{lang}.{ext}` – episode transcript file
- `GET /api/health` – health check (verifies SQLite database reachability)

### Admin endpoints (trusted/local access only)

Interactive Swagger UI is available at `http://127.0.0.1:8025/docs`.

- `GET /admin/feeds` – list all feeds (enabled and disabled) with sync timestamps, failure counts, and per-status download counts
- `GET /admin/feeds/{feed_id}/downloads` – list downloads for a feed (supports `?status=`, `?limit=`, `?offset=`)
- `GET /admin/feeds/{feed_id}/downloads/{download_id}` – retrieve selected fields for a download record (supports `?fields=`)
- `POST /admin/feeds/{feed_id}/refresh` – trigger immediate feed processing (enqueue + download cycle)
- `POST /admin/feeds/{feed_id}/requeue` – requeue all `ERROR` downloads for a feed to `QUEUED` and trigger processing
- `POST /admin/feeds/{feed_id}/downloads/{download_id}/requeue` – requeue a single `ERROR` download to `QUEUED`
- `POST /admin/feeds/{feed_id}/reset-sync` – reset `last_successful_sync` (`{"sync_time": "2025-01-01T00:00:00Z"}`)
- `POST /admin/feeds/{feed_id}/downloads` – queue a single URL for manual feeds (`schedule: "manual"`)
- `POST /admin/feeds/{feed_id}/downloads/{download_id}/refresh-metadata` – re-fetch metadata from `yt-dlp` (body optional; pass `{"refresh_transcript": true}` to force transcript refresh)
- `DELETE /admin/feeds/{feed_id}/downloads/{download_id}` – delete a download from a manual feed (removes media, thumbnail, transcript, and regenerates RSS)
- `GET /api/health` – health check

Quick `curl` examples:

```bash
# Inspect all feeds and download counts
curl -s http://127.0.0.1:8025/admin/feeds | jq

# Trigger an immediate refresh
curl -X POST http://127.0.0.1:8025/admin/feeds/my_channel/refresh

# Requeue failed episodes
curl -X POST http://127.0.0.1:8025/admin/feeds/my_channel/requeue

# Submit a URL to a manual feed
curl -X POST http://127.0.0.1:8025/admin/feeds/manual_drop/downloads \
  -H 'Content-Type: application/json' \
  -d '{"url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"}'
```

Admin endpoints run on a separate server by default with no authentication. Only expose the public server publicly.

When `SINGLE_SERVER_MODE=true`, admin routes are mounted on the main server under `/admin/`. Use this only when external access control (e.g., Cloudflare Access, Nginx/Caddy auth) protects `/admin/`. See [docs/configuration.md](docs/configuration.md#single-server-mode) for details.

## Reverse proxies

Set `BASE_URL` to your external URL (including any subpath prefix, e.g., `https://podcasts.example.com` or `https://home.example.com/anypod`) and set `TRUSTED_PROXIES` to your proxy's IP/CIDR so forwarded headers are trusted.

**Caddy:**

```caddyfile
podcasts.example.com {
    reverse_proxy anypod:8024
}
```

**Nginx:**

```nginx
server {
    server_name podcasts.example.com;
    location / {
        proxy_pass http://127.0.0.1:8024;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

**Cloudflare Tunnel:** Point your public hostname (`podcasts.example.com`) to `http://anypod:8024`, set `BASE_URL=https://podcasts.example.com`, and keep port `8025` unbound from the tunnel (or protect `/admin/*` with Cloudflare Access if using `SINGLE_SERVER_MODE=true`).

## Limitations and Notes

### Scheduling and Rate Limiting

Even if multiple feeds share the same cron schedule, Anypod processes one feed at a time to reduce rate-limiting pressure.

YouTube can aggressively rate-limit unauthenticated requests. Using [cookies](#cookies) (preferably on a dedicated account) and/or a [PO Token provider](#po-tokens-youtube) significantly improves reliability.

### Cookies

Helpful references for exporting cookies:

- [Exporting YouTube cookies](https://github.com/yt-dlp/yt-dlp/wiki/Extractors#exporting-youtube-cookies)
- [How to pass cookies to yt-dlp](https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp)
- [Error 429: Too many requests](https://github.com/yt-dlp/yt-dlp/wiki/FAQ#http-error-429-too-many-requests-or-402-payment-required)

Notes:

- Extensions such as [Get cookies.txt LOCALLY](https://chromewebstore.google.com/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdbgdldlbecc) work well
- On Windows, ensure the file uses `LF` line endings rather than `CRLF`
- Make sure `cookies.txt` exists as a regular file on the host before starting Docker so Docker does not auto-create an empty directory
- When YouTube runs with cookies, Anypod automatically passes `--extractor-args youtubetab:skip=authcheck` unless you specify your own `youtubetab:` extractor arg in `yt_args`

### PO Tokens (YouTube)

Some YouTube endpoints and qualities require short-lived Proof-of-Origin (PO) tokens. Anypod defaults to POT disabled; to enable it, run a PO Token provider such as [bgutil-ytdlp-pot-provider](https://github.com/Brainicism/bgutil-ytdlp-pot-provider) and set `POT_PROVIDER_URL` (included in the Docker Compose quick start above).

### Pocket Casts

Pocket Casts caches feed URLs and metadata aggressively. Test new feeds in Apple Podcasts or Overcast first (and make sure `BASE_URL` points to your final public URL) before subscribing in Pocket Casts.

### Filtering

Set `since` or `keep_last` on every new feed (or in `defaults:`), otherwise Anypod will enqueue every item in the channel/playlist. Anypod logs a startup warning if a scheduled feed has neither set. Note that `yt-dlp`'s date filtering is day-precision (`YYYYMMDD`).

### X / Twitter

Public X/Twitter video status URLs (`https://x.com/<handle>/status/<id>` or `https://twitter.com/<handle>/status/<id>`) are supported. Profile/timeline URLs are not supported. Supply `cookies.txt` for private or member-only posts.

### Patreon

Patreon creator pages and individual posts are supported:

- **Cookies required**: Paywalled content requires `cookies.txt`
- **Video + audio support**: Posts with both video and audio attachments pick video by default, or audio when `prefer_audio: true` is configured on the feed. Audio-only posts automatically fall back to their audio attachment instead of being dropped.

### Other yt-dlp Sources

URLs outside YouTube, Patreon, and X/Twitter (such as Vimeo, Twitch, SoundCloud, Nebula, etc.) are routed through a best-effort generic `yt-dlp` handler. Because third-party extractors vary widely, set `metadata.title` and `since`/`keep_last` explicitly if needed.

### Error Handling and Logging

Anypod tolerates partial failures during feed runs:

- Individual inaccessible videos/posts are retried up to `max_errors` times before moving to `ERROR` status (requeueable via `POST /admin/feeds/{feed_id}/requeue`)
- After each download, Anypod validates the media with `ffprobe` and deletes unreadable/corrupt files so broken media never reaches RSS subscribers
- Startup state reconciliation runs asynchronously after the HTTP server starts, so transient upstream errors never block health checks or crash-loop the container

## Development

Requirements: Python 3.14+, [`uv`](https://docs.astral.sh/uv/), `ffmpeg`, and `ffprobe`.

```bash
uv run pre-commit run --all-files # Lint/format/type-check
uv run pytest                     # Run tests
uv run pytest --integration       # Integration tests (hits YouTube)
```

For dev server scripts, debug modes, Docker workflows, and coverage, see [docs/development.md](docs/development.md).

## Architecture

High-level components:

- Configuration: Pydantic settings (`defaults:` + per-feed YAML + env + CLI)
- Database: SQLite via SQLModel/SQLAlchemy (async) with automatic startup Alembic migrations
- Data Coordinator: Enqueuer → Downloader (+ MediaProcessor) → Pruner → RSS generation
- `yt-dlp` wrapper: async subprocess invocations with source-specific + generic handlers
- File/Path management: consistent on-disk and URL mapping (including subpath `BASE_URL`)
- HTTP server: FastAPI + Uvicorn serving public RSS/media and private Admin endpoints

For details, see `DESIGN_DOC.md`.

## Roadmap

Tracked in [GitHub Issues](https://github.com/thurstonsand/anypod/issues). Planned areas include:

- Web Admin Dashboard
- AI transcription fallback (Whisper) when a source has no subtitles
- Chapter marker generation from video descriptions/timestamps
- Global disk-usage cap across all feeds
- SponsorBlock integration (cut segments or add chapter markers)

## FAQ / Troubleshooting

- **429 / 403 from YouTube**: Back off your cron schedule, enable `POT_PROVIDER_URL`, or provide `cookies.txt`.
- **Certain `yt-dlp` flags are managed by Anypod**: See [yt-dlp Arguments Caveats](docs/configuration.md#yt-dlp-arguments-caveats).
- **Where are files and the database stored?**
  - Database: `${DATA_DIR}/db/anypod.db`
  - Media: `${DATA_DIR}/media/<feed_id>/<download_id>.<ext>`
  - Feed XML: `${DATA_DIR}/feeds/<feed_id>.xml`
  - Transcripts: `${DATA_DIR}/transcripts/<feed_id>/<download_id>.<lang>.vtt`
