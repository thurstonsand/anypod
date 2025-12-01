# Configuration Guide

Feed configuration and environment variables for Anypod.

## Feed Configuration

Feeds are defined in a YAML file (default: `/config/feeds.yaml`).

### Basic Structure

```yaml
feeds:
  <feed_id>:
    url: <source_url>
    schedule: <cron_expression> # or "manual"
    yt_args: <yt-dlp_arguments> # optional
    since: <YYYYMMDD> # optional
    keep_last: <number> # optional
    metadata: # optional overrides
      title: <string>
      # ... other fields
```

### Full Example

```yaml
feeds:
  # Basic feed with schedule and date filter
  channel:
    url: https://www.youtube.com/@example
    yt_args: "-f worst[ext=mp4] --playlist-items 1-3"
    schedule: "0 3 * * *"
    since: "20220101"
    transcript_lang: en # Download English transcripts when available
    transcript_source_priority:
      - creator # Prefer creator subtitles first
      - auto # Fall back to auto-generated captions if needed

  # Feed with full metadata overrides
  premium_podcast:
    url: https://www.youtube.com/@premium/videos
    schedule: "0 6 * * *"
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
      description: "Episodes arrive when we say so"
```

### Field Reference

| Field                        | Required | Description                                                   |
| ---------------------------- | -------- | ------------------------------------------------------------- |
| `url`                        | Yes\*    | Source URL (YouTube channel/playlist, Patreon, X/Twitter)     |
| `schedule`                   | Yes      | Cron expression or `"manual"`                                 |
| `yt_args`                    | No       | Extra yt-dlp arguments (see caveats below)                    |
| `since`                      | No       | Only include items after this date (`YYYYMMDD`)               |
| `keep_last`                  | No       | Retain only the N most recent items                           |
| `transcript_lang`            | No       | Language code for subtitles/transcripts (e.g., `en`)          |
| `transcript_source_priority` | No       | Ordered list of transcript sources to try (`creator`, `auto`) |
| `metadata`                   | No       | Override feed metadata (see below)                            |

- `url` is optional for manual feeds.

### Transcript Settings

- `transcript_lang` accepts ISO 639-1 two-letter codes (e.g., `en`, `es`). When set, Anypod downloads subtitles in VTT format, stores them under `/transcripts/{feed_id}`, and emits `<podcast:transcript>` tags so podcast players surface captions.
- `transcript_source_priority` is an ordered list containing `creator` and/or `auto`. The first available source wins. When omitted but `transcript_lang` is set, Anypod defaults to `['creator', 'auto']`.

```yaml
feeds:
  channel:
    transcript_lang: en
    transcript_source_priority:
      - creator
      - auto
```

### Metadata Overrides

| Field          | Description                                                           |
| -------------- | --------------------------------------------------------------------- |
| `title`        | Feed title                                                            |
| `subtitle`     | Feed subtitle                                                         |
| `description`  | Feed description                                                      |
| `language`     | Language code (e.g., `en`, `es`, `fr`)                                |
| `author`       | Podcast author name                                                   |
| `author_email` | Podcast author email                                                  |
| `image_url`    | Original artwork URL (min 1400x1400px, downloaded and hosted locally) |
| `podcast_type` | `"episodic"` or `"serial"`                                            |
| `explicit`     | `"yes"`, `"no"`, or `"clean"`                                         |
| `category`     | Apple Podcasts categories (max 2)                                     |

### Category Formats

Categories can be specified in multiple formats:

```yaml
# Simple string
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

1. Configure feed with `schedule: "manual"` and at least `metadata.title`
2. POST each URL to `POST /admin/feeds/{feed_id}/downloads` with `{"url": "<video url>"}`
3. The handler stores and queues the download via the normal pipeline

If the video was already downloaded, the endpoint responds with `new: false` and skips scheduling.

### yt-dlp Arguments Caveats

The following yt-dlp options are managed by Anypod and should not be overridden:

- **Metadata**: `--dump-json`, `--dump-single-json`, `--flat-playlist`, `--skip-download`, `--quiet`, `--no-warnings`
- **Filtering/iteration**: `--break-match-filters`, `--lazy-playlist`, playlist limits derived from `keep_last` and `since`
- **Paths/output**: `--paths`, `--output "<download_id>.%(ext)s"`
- **Thumbnails**: conversion to `jpg` is enforced
- **Updates**: `-U`/`--update-to` controlled by `yt_channel` configuration

## Environment Variables

Configure global application settings via environment variables. All can also be provided as CLI flags (kebab-case).

### Core Settings

| Variable       | Default                 | Description                                         |
| -------------- | ----------------------- | --------------------------------------------------- |
| `BASE_URL`     | `http://localhost:8024` | Public base URL for feed/media links                |
| `DATA_DIR`     | `/data`                 | Root directory for all application data             |
| `CONFIG_FILE`  | `/config/feeds.yaml`    | Config file path                                    |
| `COOKIES_PATH` | unset                   | Optional cookies.txt file for yt-dlp authentication |

### Server Settings

| Variable            | Default   | Description                                             |
| ------------------- | --------- | ------------------------------------------------------- |
| `SERVER_HOST`       | `0.0.0.0` | HTTP server bind address                                |
| `SERVER_PORT`       | `8024`    | Public HTTP server port                                 |
| `ADMIN_SERVER_PORT` | `8025`    | Admin HTTP server port (keep private)                   |
| `TRUSTED_PROXIES`   | unset     | Trusted proxy IPs/networks (e.g., `["192.168.1.0/24"]`) |

### Logging Settings

| Variable                 | Default | Description                                    |
| ------------------------ | ------- | ---------------------------------------------- |
| `LOG_FORMAT`             | `human` | Log format: `human` or `json`                  |
| `LOG_LEVEL`              | `INFO`  | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_INCLUDE_STACKTRACE` | `false` | Include stack traces in error logs             |

### yt-dlp Settings

| Variable             | Default  | Description                                                      |
| -------------------- | -------- | ---------------------------------------------------------------- |
| `YT_CHANNEL`         | `stable` | yt-dlp update channel: `stable`, `nightly`, `master`, or version |
| `YT_DLP_UPDATE_FREQ` | `12h`    | Minimum interval between yt-dlp updates                          |
| `POT_PROVIDER_URL`   | unset    | POT provider URL for YouTube PO tokens                           |

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

## Where to Change Things

- **New feeds**: Edit your feeds YAML file (default `/config/feeds.yaml`)
- **Global defaults**: Prefer environment variables over hardcoding
- **Per-feed settings**: Use the feed-level fields in YAML
- **Metadata overrides**: Use the `metadata` block in each feed
