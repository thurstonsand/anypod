# Configuration Guide

Feed configuration, media conversion presets, and environment variables for Anypod.

## Feed Configuration

Feeds are defined in a YAML file (default: `/config/feeds.yaml`). Unknown keys in `feeds.yaml` are ignored so upgrades remain backwards-compatible, and Anypod logs a startup `WARNING` for every unknown key so typos are caught quickly.

### Basic Structure

```yaml
defaults: # optional global defaults inherited by every feed
  media_conversion: light
  keep_last: 20

feeds:
  <feed_id>:
    url: <source_url>
    schedule: <cron_expression> # or "@hourly" / "@daily" / "@weekly" / "@monthly" / "manual"
    enabled: true # optional (default: true)
    media_conversion: light # optional: off | light | optimized | custom
    embed_thumbnail: true # optional (defaults to true for light/optimized, false for off/custom)
    prefer_audio: false # optional (Patreon: pick audio attachment when post has both video & audio)
    yt_args: <yt-dlp_arguments> # optional
    since: <YYYY-MM-DD or YYYYMMDD> # optional
    keep_last: <number> # optional
    max_errors: 3 # optional (default: 3)
    metadata: # optional overrides
      title: <string>
      # ... other fields
```

`feed_id` values must contain only letters, digits, underscores, and hyphens (`^[a-zA-Z0-9_-]+$`, max 64 chars).

### Global Defaults (`defaults:`)

Any per-feed option except `url` can be placed in the top-level `defaults:` block. Per-feed values override `defaults:`, and nested `metadata:` blocks are deep-merged so you can set shared metadata (like `author_email` or `language`) globally while overriding `title` per feed:

```yaml
defaults:
  schedule: "0 3 * * *"
  media_conversion: light
  keep_last: 25
  transcript_lang: en
  metadata:
    language: en

feeds:
  tech_channel:
    url: https://www.youtube.com/@example
    since: "2024-06-01"
  audio_patreon:
    url: https://www.patreon.com/creator
    prefer_audio: true
    media_conversion: optimized
    metadata:
      title: "Creator Podcast (Audio)"
```

### Full Example

```yaml
defaults:
  media_conversion: light
  keep_last: 30

feeds:
  # Basic feed with schedule, date filter, and transcripts
  channel:
    url: https://www.youtube.com/@example
    schedule: "@daily"
    since: "2024-01-01"
    transcript_lang: en
    transcript_source_priority:
      - creator
      - auto

  # Feed with optimized HEVC conversion and full metadata overrides
  premium_podcast:
    url: https://www.youtube.com/@premium/videos
    schedule: "0 6 * * *"
    media_conversion: optimized
    metadata:
      title: "My Premium Podcast"
      subtitle: "Daily insights and discussions"
      description: "A daily podcast about technology and culture"
      language: "en"
      author: "John Doe"
      author_email: "john@example.com"
      image_url: "https://example.com/podcast-art.jpg"
      podcast_type: "episodic" # or "serial"
      explicit: "no" # "yes", "no", or "clean"
      category:
        - "Technology"
        - "Business > Entrepreneurship"

  # Manual feed (no automatic scheduling)
  manual_drop:
    schedule: "manual"
    metadata:
      title: "Manual Drops"
      description: "Episodes arrive when submitted via the Admin API"
```

### Field Reference

| Field                        | Required | Default                          | Description                                                                        |
| ---------------------------- | -------- | -------------------------------- | ---------------------------------------------------------------------------------- |
| `url`                        | Yes\*    | —                                | Source URL (YouTube, Patreon, X/Twitter, or any yt-dlp-supported site)             |
| `schedule`                   | Yes      | —                                | 5-field cron expression, `@hourly`/`@daily`/`@weekly`/`@monthly`, or `"manual"`    |
| `enabled`                    | No       | `true`                           | Set `false` to pause syncing without deleting feed data                            |
| `media_conversion`           | No       | `light`                          | Post-download conversion preset: `off`, `light`, `optimized`, or `custom`          |
| `embed_thumbnail`            | No       | `true` (`light`/`optimized`), `false` (`off`/`custom`) | Embed episode artwork directly into the media file (for Pocket Casts / Apple Podcasts) |
| `prefer_audio`               | No       | `false`                          | On multi-attachment Patreon posts, download the audio attachment instead of video  |
| `yt_args`                    | No       | `""`                             | Extra `yt-dlp` arguments (see caveats below)                                       |
| `since`                      | No       | unset                            | Only include items published on/after this date (`YYYY-MM-DD`, `YYYYMMDD`, or YAML date) |
| `keep_last`                  | No       | unset                            | Retain only the N most recent items                                                |
| `download_delay`             | No       | unset                            | Delay downloads after publication (e.g., `24h`, `3d`, `1w`) for metadata to settle |
| `max_errors`                 | No       | `3`                              | Consecutive failure attempts before transitioning a download to `ERROR`            |
| `transcript_lang`            | No       | unset                            | ISO 639-1 language code for subtitles/transcripts (e.g., `en`)                     |
| `transcript_source_priority` | No       | `['creator', 'auto']` (when `transcript_lang` set) | Ordered list of transcript sources to try (`creator`, `auto`)                      |
| `metadata`                   | No       | unset                            | Override feed metadata (see below)                                                 |

\* `url` is optional for manual feeds (`schedule: "manual"`).

### Media Conversion

Anypod includes a built-in `ffmpeg`-powered media processor so episodes play reliably across Apple Podcasts, Pocket Casts, Overcast, and web browsers without needing external `--exec` scripts.

| Preset      | Default `yt-dlp` format (when `-f` not in `yt_args`) | Video handling | Audio handling | Container / tags |
| ----------- | ---------------------------------------------------- | -------------- | -------------- | ---------------- |
| `off`       | `yt-dlp` default                                     | Untouched      | Untouched      | Untouched        |
| `light`     | `bestvideo[vcodec^=avc1]+bestaudio[acodec^=mp4a]/bestvideo+bestaudio/best` | Stream-copies `h264`, `hevc`, `av1`; transcodes other codecs (e.g. VP9) to fast H.264 | Stream-copies `aac`; encodes others to 128k AAC | `.mp4` with `-movflags +faststart` (`.m4a` for audio-only) |
| `optimized` | `bestvideo[height<=720]+bestaudio/best[height<=720]/best` | Re-encodes to HEVC (`libx265`, CRF 28, `-tag:v:0 hvc1`, `yuv420p`) | Encodes to 128k AAC | `.mp4` with `-movflags +faststart` (`.m4a` for audio-only) |
| `custom`    | `yt-dlp` default                                     | Delegated to custom script | Delegated to custom script | Determined by script output |

Notes:

- If you pass your own `-f` / `--format` flag in `yt_args`, Anypod respects it and does not inject a preset format.
- Audio-only downloads (e.g. `-f bestaudio` or Patreon audio-only posts) stay audio-only in `light` and `optimized` modes (remuxed/transcoded to `.m4a` with `+faststart`, preserving embedded artwork).
- `embed_thumbnail` defaults to `true` for `light` and `optimized` and `false` for `off` and `custom`. When enabled, Anypod embeds the episode thumbnail as an attached picture stream (`attached_pic`) and preserves it through conversion.
- Conversions run atomically in `tmp/` before swapping into `media/` so podcast clients never see half-written files.

#### Custom Hook Script (`media_conversion: custom`)

For full control over encoding (e.g., hardware acceleration via VAAPI/NVENC/VideoToolbox), set `media_conversion: custom` and place an executable script at `<config_dir>/post_process.sh` (or set `MEDIA_CONVERSION_SCRIPT`).

Anypod validates at startup that the script exists and is executable (`chmod +x`), then invokes it per download inside an isolated staging directory under `tmp/` with:

- **Positional argument `$1`**: Path to the input media file inside the staging directory
- **Environment variables**:
  - `ANYPOD_INPUT`: Full path to the input media file
  - `ANYPOD_OUTPUT`: Suggested output path (defaulting to `<staging>/<download_id>.mp4`)
  - `ANYPOD_FEED_ID`: Feed identifier
  - `ANYPOD_DOWNLOAD_ID`: Download identifier
  - `ANYPOD_THUMBNAIL`: Path to the episode thumbnail JPG (empty string if unavailable)
  - `ANYPOD_EMBED_THUMBNAIL`: `"true"` or `"false"`

Contract:

- Exit `0` on success; non-zero fails the download and cleans up staging files automatically.
- Write the final file to `ANYPOD_OUTPUT` (or overwrite `ANYPOD_INPUT` in place). Anypod detects the resulting file extension, atomically moves it into `media/<feed_id>/<download_id>.<ext>`, and updates SQLite metadata (`ext`, `mime_type`, `filesize`, `duration`).

Minimal example (`/config/post_process.sh`):

```bash
#!/usr/bin/env bash
set -euo pipefail
ffmpeg -y -i "$ANYPOD_INPUT" -c:v copy -c:a aac -b:a 128k -movflags +faststart "$ANYPOD_OUTPUT"
```

### Transcript Settings

- `transcript_lang` accepts ISO 639-1 two-letter codes (e.g., `en`, `es`). When set, Anypod downloads subtitles in VTT format, stores them under `/transcripts/{feed_id}`, emits `<podcast:transcript>` tags so podcast players surface captions, and soft-embeds a `mov_text` subtitle track into MP4-family video files (`.mp4`, `.m4v`, `.mov`).
- `transcript_source_priority` is an ordered list containing `creator` and/or `auto`. The first available source wins. When omitted but `transcript_lang` is set, Anypod defaults to `['creator', 'auto']`.

### Metadata Overrides

| Field          | Description                                                                        |
| -------------- | ---------------------------------------------------------------------------------- |
| `title`        | Feed title                                                                         |
| `subtitle`     | Feed subtitle                                                                      |
| `description`  | Feed description                                                                   |
| `language`     | Language code (e.g., `en`, `es`, `fr`)                                             |
| `author`       | Podcast author name                                                                |
| `author_email` | Podcast owner email (when unset, `<itunes:owner>` is omitted from RSS)             |
| `image_url`    | Original artwork URL (min 1400x1400px, downloaded and hosted locally)              |
| `podcast_type` | `"episodic"` or `"serial"`                                                         |
| `explicit`     | `"yes"`, `"no"`, or `"clean"`                                                      |
| `category`     | Apple Podcasts categories (max 2)                                                  |

### Category Formats

Categories can be specified in multiple formats:

```yaml
# Simple string list
category:
  - "Technology"
  - "Business > Entrepreneurship"

# Object format
category:
  - main: "Technology"
  - main: "Business"
    sub: "Entrepreneurship"

# Comma-separated string
category: "Technology, Business > Entrepreneurship"
```

### Manual Feeds

For ad-hoc content, set `schedule: "manual"`:

1. Configure the feed with `schedule: "manual"` and at least `metadata.title`
2. POST each URL to `POST /admin/feeds/{feed_id}/downloads` with `{"url": "<video url>"}`
3. Anypod fetches metadata, queues, downloads, converts, and updates RSS automatically

If the video was already downloaded, the endpoint responds with `new: false` and skips re-downloading.

### yt-dlp Arguments Caveats

The following `yt-dlp` options are managed by Anypod and should not be overridden in `yt_args`:

- **Metadata**: `--dump-json`, `--dump-single-json`, `--flat-playlist`, `--skip-download`, `--quiet`, `--no-warnings`
- **Filtering/iteration**: `--break-match-filters`, `--lazy-playlist`, playlist limits derived from `keep_last` and `since`
- **Paths/output**: `--paths`, `--output "<download_id>.%(ext)s"`
- **Thumbnails**: conversion to `jpg` is enforced
- **Updates**: `-U`/`--update-to` is handled via a standalone scheduled maintenance job controlled by `YT_CHANNEL` and `YT_DLP_UPDATE_FREQ`

When YouTube runs with `COOKIES_PATH` set, Anypod automatically passes `--extractor-args youtubetab:skip=authcheck` unless you supply your own `youtubetab:` extractor arg in `yt_args`.

## Environment Variables

Configure global application settings via environment variables. All can also be provided as CLI flags (kebab-case).

### Core Settings

| Variable                  | Default                        | Description                                                    |
| ------------------------- | ------------------------------ | -------------------------------------------------------------- |
| `BASE_URL`                | `http://localhost:8024`        | Public base URL for feed/media links (supports subpath prefix) |
| `DATA_DIR`                | `/data`                        | Root directory for all application data                        |
| `CONFIG_FILE`             | `/config/feeds.yaml`           | Config file path                                               |
| `COOKIES_PATH`            | unset                          | Optional `cookies.txt` file for `yt-dlp` authentication        |
| `MEDIA_CONVERSION_SCRIPT` | `<config_dir>/post_process.sh` | Executable hook script used by `media_conversion: custom`      |

### Server Settings

| Variable             | Default   | Description                                                                                            |
| -------------------- | --------- | ------------------------------------------------------------------------------------------------------ |
| `SERVER_HOST`        | `0.0.0.0` | HTTP server bind address                                                                               |
| `SERVER_PORT`        | `8024`    | Public HTTP server port                                                                                |
| `ADMIN_SERVER_PORT`  | `8025`    | Admin HTTP server port (keep private)                                                                  |
| `SINGLE_SERVER_MODE` | `false`   | Mount admin routes on main server (see [Single-Server Mode](#single-server-mode) for security details) |
| `TRUSTED_PROXIES`    | unset     | Trusted proxy IPs/CIDRs: comma-separated (`192.168.1.0/24,10.0.0.1`), `*`, or JSON array               |

### Logging Settings

| Variable                 | Default | Description                                    |
| ------------------------ | ------- | ---------------------------------------------- |
| `LOG_FORMAT`             | `human` | Log format: `human` or `json`                  |
| `LOG_LEVEL`              | `INFO`  | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_INCLUDE_STACKTRACE` | `false` | Include stack traces in error logs             |

### yt-dlp Settings

| Variable             | Default  | Description                                                                             |
| -------------------- | -------- | --------------------------------------------------------------------------------------- |
| `YT_CHANNEL`         | `stable` | `yt-dlp` self-update channel: `stable`, `nightly`, `master`, version, or `disabled`     |
| `YT_DLP_UPDATE_FREQ` | `12h`    | Minimum interval between `yt-dlp` updates (`12h`, `1d`, `PT12H`, or `0` to disable)     |
| `POT_PROVIDER_URL`   | unset    | POT provider URL for YouTube PO tokens                                                  |

### Debug Settings

| Variable     | Default | Description                                   |
| ------------ | ------- | --------------------------------------------- |
| `DEBUG_MODE` | unset   | Debug mode: `ytdlp`, `enqueuer`, `downloader` |

### Docker Settings

| Variable | Default | Description                                         |
| -------- | ------- | --------------------------------------------------- |
| `PUID`   | `1000`  | Container user ID                                   |
| `PGID`   | `1000`  | Container group ID                                  |
| `TZ`     | unset   | Timezone (alternative to mounting `/etc/localtime`) |

## Single-Server Mode

By default, Anypod runs two HTTP servers:

- **Public server** (port 8024): Serves RSS feeds, media files, images, and transcripts
- **Admin server** (port 8025): Exposes admin endpoints for feed management

When `SINGLE_SERVER_MODE=true`, Anypod mounts the admin router on the main server under `/admin/` instead of running a separate admin server. This simplifies deployment when admin access is protected at the infrastructure level.

### When to Use Single-Server Mode

Use single-server mode when you have external access control that can protect URL paths:

- **Cloudflare Access**: Gate `/admin/*` routes by identity
- **Nginx/Caddy auth**: Require authentication for `/admin/` prefix
- **VPN-only access**: Entire server behind VPN with no public exposure

### Configuration

```bash
SINGLE_SERVER_MODE=true
```

Or in Docker Compose:

```yaml
environment:
  SINGLE_SERVER_MODE: "true"
```

### Security Warning

When single-server mode is enabled, Anypod logs a WARNING at startup:

> Single-server mode enabled: admin APIs are exposed on the public server port. Ensure admin routes are protected at the infrastructure level.

**Do not enable single-server mode unless you have confirmed that `/admin/` routes are protected.** Admin endpoints allow triggering downloads, resetting feed state, and deleting content.

### Behavior Differences

| Aspect              | Dual-Server Mode (default)   | Single-Server Mode                 |
| ------------------- | ---------------------------- | ---------------------------------- |
| Ports               | 8024 (public) + 8025 (admin) | 8024 only                          |
| Admin routes        | Separate app on admin port   | Mounted at `/admin/` on main app   |
| `ADMIN_SERVER_PORT` | Used                         | Ignored                            |
| Proxy headers       | Public server only           | All routes honor `TRUSTED_PROXIES` |

## Where to Change Things

- **New feeds**: Edit your feeds YAML file (default `/config/feeds.yaml`)
- **Shared feed settings**: Put them in the top-level `defaults:` block of `feeds.yaml`
- **Server / deployment settings**: Use environment variables
- **Per-feed overrides**: Use feed-level fields or the `metadata:` block in YAML
