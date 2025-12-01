# Transcription Support Implementation Plan

## Overview

This document outlines the implementation plan for adding podcast transcription support to Anypod. The goal is to extract subtitles/captions from source videos via yt-dlp and serve them as podcast transcripts through the RSS feed using the `<podcast:transcript>` tag from the Podcasting 2.0 namespace.

**GitHub Issue:** [#95 - Add podcast transcription support with AI fallback](https://github.com/thurstonsand/anypod/issues/95)

**Scope:** Phase 1 focuses on extracting existing transcriptions from sources via yt-dlp. AI-generated transcription fallback (Whisper) is deferred to a future phase.

## Current State Analysis

### yt-dlp Subtitle Capabilities

yt-dlp provides comprehensive subtitle extraction:

```bash
--write-subs              # Write creator-provided subtitles
--write-auto-subs         # Write auto-generated subtitles (YouTube)
--sub-format FORMAT       # Format preference: "vtt", "srt/vtt/best"
--sub-langs LANGS         # Languages: "en", "en.*", "all", etc.
--convert-subs FORMAT     # Convert to: srt, vtt, ass, lrc
--skip-download           # Get subs without video (metadata phase)
```

**Key behaviors:**
- Creator subtitles take precedence over auto-generated when both exist
- Auto-generated subtitles are YouTube-specific
- VTT format preserves speaker labels via `<v>` tags
- SRT format has wider player compatibility

### Podcast Namespace Transcript Specification

The `<podcast:transcript>` tag is part of the Podcasting 2.0 namespace:

```xml
<rss xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <item>
      <podcast:transcript 
          url="https://example.com/episode1/transcript.vtt" 
          type="text/vtt" 
          language="en"
          rel="captions" />
    </item>
  </channel>
</rss>
```

**Attributes:**

| Attribute  | Required | Description |
|------------|----------|-------------|
| `url`      | Yes      | Full URL to transcript file |
| `type`     | Yes      | MIME type: `text/vtt`, `application/x-subrip`, `application/json`, `text/plain`, `text/html` |
| `language` | No       | ISO language code (defaults to feed's `<language>`) |
| `rel`      | No       | `"captions"` indicates timed captions; omit for plain transcript |

### Podcast Player Support

| Player | VTT | SRT | JSON | Notes |
|--------|-----|-----|------|-------|
| Apple Podcasts | ✅ Preferred | ✅ Fallback | ❌ | VTT supports speaker names via `<v>` tags |
| Pocket Casts | ✅ | ✅ | ✅ | All Podcasting 2.0 formats |
| Podcast Addict | ❌ | ✅ Live captions | ❌ | HTML/text for transcript button |
| Podverse | ✅ | ✅ | ✅ | Full Podcasting 2.0 support |
| AntennaPod | ✅ | ✅ | ✅ | Full Podcasting 2.0 support |

**Recommendation:** Provide VTT as primary format (best Apple support, preserves timing and speakers).

## Implementation Plan

### Feed Configuration Additions

Add per-feed settings so operators decide when transcripts are downloaded and which subtitle sources to prefer:

- `transcript_lang`: ISO 639-1 language string passed to yt-dlp's `--sub-langs` option. When set, Anypod downloads subtitles in that language (if available) and emits `<podcast:transcript>` tags pointing at the hosted `.vtt` files.
- `transcript_source_priority`: Ordered list containing `creator` and/or `auto`. The list defines which subtitles to attempt first; omit to use the default `['creator', 'auto']` ordering.

```yaml
feeds:
  sample_channel:
    transcript_lang: en
    transcript_source_priority:
      - creator
      - auto
```

### 1. Database Schema Changes

Add transcript fields to the `Download` model:

```python
# In src/anypod/db/types/download.py
class Download(SQLModel, table=True):
    # ... existing fields ...

    # Transcript fields
    transcript_ext: str | None = None       # "vtt", "srt", or None
    transcript_lang: str | None = None      # "en", "es", etc.
    transcript_source: str | None = None    # "creator", "auto", or "ai" (future)
```

**Field descriptions:**
- `transcript_ext`: File extension indicating format and presence of transcript
- `transcript_lang`: Language code of the transcript (may differ from feed language)
- `transcript_source`: Origin of transcript for quality indication in UI

#### Alembic Migration

```python
# alembic/versions/XXXX_add_transcript_fields.py
"""Add transcript fields to download table."""

def upgrade() -> None:
    op.add_column("download", sa.Column("transcript_ext", sa.String(), nullable=True))
    op.add_column("download", sa.Column("transcript_lang", sa.String(), nullable=True))
    op.add_column("download", sa.Column("transcript_source", sa.String(), nullable=True))

def downgrade() -> None:
    op.drop_column("download", "transcript_source")
    op.drop_column("download", "transcript_lang")
    op.drop_column("download", "transcript_ext")
```

### 2. Transcript Storage Structure

Store transcripts in a dedicated directory alongside media:

```
/data/{feed_id}/
├── {download_id}.m4a              # Media file
├── transcripts/
│   └── {download_id}.{lang}.vtt   # Transcript file (e.g., "video123.en.vtt")
└── ...
```

**Rationale:**
- Dedicated subdirectory keeps media directory clean
- Easy to identify transcript files for cleanup
- Consistent with existing image storage pattern (`images/{feed_id}/downloads/`)

### 3. PathManager Extensions

Add transcript path and URL methods:

```python
# In src/anypod/path_manager.py
class PathManager:
    # ... existing methods ...

    async def feed_transcripts_dir(self, feed_id: str) -> Path:
        """Return the directory for a feed's transcript files."""
        self._validate_safe_path_component(feed_id)
        path = self._base_data_dir / feed_id / "transcripts"
        await aiofiles.os.makedirs(path, exist_ok=True)
        return path

    async def transcript_path(self, feed_id: str, download_id: str, lang: str, ext: str) -> Path:
        """Return the full file system path for a transcript file."""
        self._validate_safe_path_component(feed_id)
        self._validate_safe_path_component(download_id)
        self._validate_safe_path_component(lang)
        self._validate_safe_path_component(ext)
        transcripts_dir = await self.feed_transcripts_dir(feed_id)
        return transcripts_dir / f"{download_id}.{lang}.{ext}"

    def transcript_url(self, feed_id: str, download_id: str, lang: str, ext: str) -> str:
        """Return the HTTP URL for a transcript file."""
        self._validate_safe_path_component(feed_id)
        self._validate_safe_path_component(download_id)
        self._validate_safe_path_component(lang)
        self._validate_safe_path_component(ext)
        return urljoin(self._base_url, f"/transcripts/{feed_id}/{download_id}.{lang}.{ext}")
```

### 4. YtdlpArgs Extensions

Add subtitle/transcript CLI options:

```python
# In src/anypod/ytdlp_wrapper/core/args.py
class YtdlpArgs:
    def __init__(self, user_args: list[str] | None = None):
        # ... existing initialization ...
        self._write_subs = False
        self._write_auto_subs = False
        self._sub_format: str | None = None
        self._sub_langs: str | None = None
        self._convert_subs: str | None = None
        self._paths_subtitle: Path | None = None
        self._output_subtitle: str | None = None

    def write_subs(self) -> "YtdlpArgs":
        """Enable creator-provided subtitle downloading."""
        self._write_subs = True
        return self

    def write_auto_subs(self) -> "YtdlpArgs":
        """Enable auto-generated subtitle downloading (YouTube)."""
        self._write_auto_subs = True
        return self

    def sub_format(self, fmt: str) -> "YtdlpArgs":
        """Set subtitle format preference (e.g., 'vtt', 'srt/vtt/best')."""
        self._sub_format = fmt
        return self

    def sub_langs(self, langs: str) -> "YtdlpArgs":
        """Set subtitle languages to download (e.g., 'en', 'en.*', 'all')."""
        self._sub_langs = langs
        return self

    def convert_subs(self, fmt: str) -> "YtdlpArgs":
        """Convert subtitles to specified format (srt, vtt, ass, lrc)."""
        self._convert_subs = fmt
        return self

    def paths_subtitle(self, path: Path) -> "YtdlpArgs":
        """Set the directory where subtitles will be saved."""
        self._paths_subtitle = path
        return self

    def output_subtitle(self, template: str) -> "YtdlpArgs":
        """Set the output template for subtitle files."""
        self._output_subtitle = template
        return self

    def to_list(self) -> list[str]:
        cmd = self._build_base_cmd()
        # ... existing command building ...

        if self._write_subs:
            cmd.append("--write-subs")
        if self._write_auto_subs:
            cmd.append("--write-auto-subs")
        if self._sub_format:
            cmd.extend(["--sub-format", self._sub_format])
        if self._sub_langs:
            cmd.extend(["--sub-langs", self._sub_langs])
        if self._convert_subs:
            cmd.extend(["--convert-subs", self._convert_subs])
        if self._paths_subtitle:
            cmd.extend(["--paths", f"subtitle:{self._paths_subtitle}"])
        if self._output_subtitle:
            cmd.extend(["-o", f"subtitle:{self._output_subtitle}"])

        return cmd
```

### 5. Handler Metadata Extraction

Update handlers to extract subtitle information from yt-dlp info dict:

```python
# In handler base class or utility
def extract_subtitle_info(
    info_dict: dict[str, Any],
    preferred_langs: list[str] = ["en"],
) -> tuple[str | None, str | None, str | None]:
    """Extract subtitle metadata from yt-dlp info dict.

    Args:
        info_dict: yt-dlp extracted info dictionary.
        preferred_langs: Ordered list of preferred language codes.

    Returns:
        Tuple of (ext, lang, source) or (None, None, None) if no subtitles.
    """
    # Check for creator subtitles first
    subtitles = info_dict.get("subtitles", {})
    auto_subs = info_dict.get("automatic_captions", {})

    for lang in preferred_langs:
        # Prefer creator subtitles
        if lang in subtitles:
            return ("vtt", lang, "creator")
        # Fall back to auto-generated
        if lang in auto_subs:
            return ("vtt", lang, "auto")

    # Check for any available subtitle
    if subtitles:
        lang = next(iter(subtitles))
        return ("vtt", lang, "creator")
    if auto_subs:
        lang = next(iter(auto_subs))
        return ("vtt", lang, "auto")

    return (None, None, None)
```

### 6. Download Workflow Integration

Update `download_media_to_file` in `ytdlp_wrapper.py`:

```python
async def download_media_to_file(
    self,
    download: Download,
    user_yt_cli_args: list[str],
    cookies_path: Path | None = None,
) -> tuple[Path, str]:
    """Download the media and transcript for a given Download."""
    # ... existing setup ...

    transcripts_dir = await self._paths.feed_transcripts_dir(download.feed_id)

    download_args = (
        YtdlpArgs(user_yt_cli_args)
        # ... existing media args ...
        # Add subtitle downloading
        .write_subs()
        .write_auto_subs()
        .sub_format("vtt")
        .sub_langs("en")  # TODO: Make configurable per-feed
        .convert_subs("vtt")
        .paths_subtitle(transcripts_dir)
        .output_subtitle(f"{download.id}.%(ext)s")
    )

    # ... rest of download logic ...
```

### 7. FileManager Extensions

Add transcript file operations:

```python
# In src/anypod/file_manager.py
class FileManager:
    async def get_transcript_path(
        self, feed_id: str, download_id: str, lang: str, ext: str
    ) -> Path:
        """Get the file path for a transcript file."""
        try:
            file_path = await self._paths.transcript_path(feed_id, download_id, lang, ext)
        except ValueError as e:
            raise FileOperationError(
                "Invalid feed or download identifier.",
                feed_id=feed_id,
                download_id=download_id,
            ) from e

        if not await aiofiles.os.path.isfile(file_path):
            raise FileNotFoundError(f"Transcript file not found: {file_path}")
        return file_path

    async def transcript_exists(
        self, feed_id: str, download_id: str, lang: str, ext: str
    ) -> bool:
        """Check if a transcript file exists."""
        try:
            await self.get_transcript_path(feed_id, download_id, lang, ext)
            return True
        except FileNotFoundError:
            return False

    async def delete_transcript(
        self, feed_id: str, download_id: str, lang: str, ext: str
    ) -> None:
        """Delete a transcript file from the filesystem."""
        try:
            file_path = await self.get_transcript_path(feed_id, download_id, lang, ext)
            await aiofiles.os.remove(file_path)
            logger.debug("Transcript file deleted", extra={"path": str(file_path)})
        except FileNotFoundError:
            logger.debug(
                "Transcript file not found for deletion",
                extra={"feed_id": feed_id, "download_id": download_id, "lang": lang, "ext": ext},
            )
```

### 8. Static Router Extensions

Add transcript serving route:

```python
# In src/anypod/server/routers/static.py

@router.api_route("/transcripts/{feed_id}/{filename}.{lang}.{ext}", methods=["GET", "HEAD"])
async def serve_download_transcript(
    feed_id: ValidatedFeedId,
    filename: ValidatedFilename,
    lang: ValidatedFilename,
    ext: ValidatedExtension,
    file_manager: FileManagerDep,
) -> FileResponse:
    """Serve transcript file for a specific download."""
    logger.debug(
        "Serving transcript",
        extra={"feed_id": feed_id, "download_id": filename, "lang": lang, "ext": ext},
    )

    try:
        file_path = await file_manager.get_transcript_path(feed_id, filename, lang, ext)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="Transcript not found") from e
    except FileOperationError as e:
        raise HTTPException(status_code=500, detail="Internal server error") from e

    media_type = mimetypes.guess_type(f"file.{ext}")[0]

    return FileResponse(
        path=file_path,
        media_type=media_type or "application/octet-stream",
        headers={"Cache-Control": "public, max-age=86400"},
    )
```

### 9. RSS Feed Generator Updates

Update `feedgen_core.py` to emit `<podcast:transcript>` tags:

```python
# In src/anypod/rss/feedgen_core.py

PODCAST_NS = "https://podcastindex.org/namespace/1.0"

class FeedgenCore:
    def __init__(self, paths: PathManager, feed_id: str, feed: Feed):
        fg = FeedGenerator()

        # Register podcast namespace
        fg.register_extension("podcast", Podcast, PodcastEntryExtension)

        # Add podcast namespace declaration
        fg.load_extension("podcast", rss=True)

        # ... existing initialization ...

    def with_downloads(self, downloads: list[Download]) -> FeedgenCore:
        for download in downloads:
            fe = self._fg.add_entry(order="append")

            # ... existing episode setup ...

            # Add transcript tag if available
            if download.transcript_lang and download.transcript_ext:
                try:
                    transcript_url = self._paths.transcript_url(
                        download.feed_id,
                        download.id,
                        download.transcript_lang,
                        download.transcript_ext,
                    )
                    # Add podcast:transcript element
                    self._add_transcript_element(
                        fe,
                        url=transcript_url,
                        ext=download.transcript_ext,
                        lang=download.transcript_lang,
                    )
                except ValueError as e:
                    logger.warning(
                        "Failed to generate transcript URL",
                        extra={
                            "feed_id": download.feed_id,
                            "download_id": download.id,
                        },
                        exc_info=e,
                    )

        return self

    def _add_transcript_element(
        self,
        entry: Any,
        url: str,
        ext: str,
        lang: str | None = None,
    ) -> None:
        """Add podcast:transcript element to feed entry.

        Args:
            entry: feedgen FeedEntry object.
            url: URL to the transcript file.
            ext: File extension (vtt, srt, json, txt).
            lang: Optional language code.
        """
        mime_types = {
            "vtt": "text/vtt",
            "srt": "application/x-subrip",
            "json": "application/json",
            "txt": "text/plain",
        }
        mime_type = mime_types.get(ext, "text/plain")

        # Access the underlying lxml element
        item = entry._FeedEntry__rss_entry  # type: ignore

        # Create transcript element
        transcript = etree.SubElement(
            item,
            f"{{{PODCAST_NS}}}transcript",
            url=url,
            type=mime_type,
        )
        if lang:
            transcript.set("language", lang)
        # VTT and SRT are timed captions
        if ext in ("vtt", "srt"):
            transcript.set("rel", "captions")
```

### 10. Pruner Integration

Update pruner to clean up transcripts:

```python
# In src/anypod/data_coordinator/pruner.py
class Pruner:
    async def _handle_transcript_deletion(
        self, download: Download, feed_id: str
    ) -> None:
        """Handle transcript deletion for a download being pruned."""
        if download.transcript_ext:
            log_params = {
                "feed_id": feed_id,
                "download_id": download.id,
                "transcript_ext": download.transcript_ext,
            }
            logger.debug(
                "Attempting to delete transcript for downloaded item being pruned.",
                extra=log_params,
            )

            try:
                await self._file_manager.delete_transcript(
                    feed_id, download.id, download.transcript_lang, download.transcript_ext
                )
            except FileOperationError as e:
                raise PruneError(
                    message="Failed to delete transcript during pruning.",
                    feed_id=feed_id,
                    download_id=download.id,
                ) from e
            logger.debug(
                "Transcript deleted successfully during pruning.",
                extra=log_params,
            )
```

### 11. Migration Script for Existing Downloads

Create a script to download transcripts for existing media:

```python
#!/usr/bin/env python3
# scripts/migrate_transcripts.py
"""Download transcripts for existing downloads that don't have them.

This script iterates through all DOWNLOADED items in the database and
attempts to fetch subtitles/captions for each using yt-dlp.

Usage:
    uv run python scripts/migrate_transcripts.py [--dry-run] [--feed-id FEED_ID]

Options:
    --dry-run       Show what would be downloaded without actually downloading
    --feed-id       Only process downloads from a specific feed
    --lang          Subtitle language to fetch (default: en)
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from anypod.config import Config
from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import Download, DownloadStatus
from anypod.path_manager import PathManager
from anypod.ytdlp_wrapper.core import YtdlpArgs, YtdlpCore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def fetch_transcript_for_download(
    download: Download,
    paths: PathManager,
    lang: str = "en",
    dry_run: bool = False,
) -> bool:
    """Attempt to download transcript for a single download.

    Args:
        download: The Download object to fetch transcript for.
        paths: PathManager instance.
        lang: Language code for subtitles.
        dry_run: If True, only log what would be done.

    Returns:
        True if transcript was downloaded (or would be in dry-run), False otherwise.
    """
    if download.transcript_ext:
        logger.debug(
            "Skipping download with existing transcript",
            extra={"download_id": download.id, "feed_id": download.feed_id},
        )
        return False

    logger.info(
        "Processing download",
        extra={
            "download_id": download.id,
            "feed_id": download.feed_id,
            "source_url": download.source_url,
        },
    )

    if dry_run:
        logger.info(
            "[DRY RUN] Would attempt to download transcript",
            extra={"download_id": download.id},
        )
        return True

    try:
        transcripts_dir = await paths.feed_transcripts_dir(download.feed_id)

        args = (
            YtdlpArgs()
            .quiet()
            .no_warnings()
            .skip_download()
            .write_subs()
            .write_auto_subs()
            .sub_format("vtt")
            .sub_langs(lang)
            .convert_subs("vtt")
            .paths_subtitle(transcripts_dir)
            .output_subtitle(f"{download.id}.%(ext)s")
        )

        await YtdlpCore.download(args, download.source_url)

        # Check if transcript was downloaded
        transcript_path = transcripts_dir / f"{download.id}.vtt"
        if transcript_path.exists():
            logger.info(
                "Transcript downloaded successfully",
                extra={"download_id": download.id, "path": str(transcript_path)},
            )
            return True
        else:
            logger.info(
                "No transcript available for download",
                extra={"download_id": download.id},
            )
            return False

    except Exception as e:
        logger.warning(
            "Failed to download transcript",
            extra={"download_id": download.id, "error": str(e)},
        )
        return False


async def update_download_transcript_fields(
    download_db: DownloadDatabase,
    download: Download,
    lang: str,
) -> None:
    """Update download record with transcript metadata.

    Args:
        download_db: Database interface.
        download: The Download to update.
        lang: Language code of the transcript.
    """
    # TODO: Implement proper database update method
    # This will need a new method in DownloadDatabase to update transcript fields
    pass


async def main(
    dry_run: bool = False,
    feed_id: str | None = None,
    lang: str = "en",
) -> None:
    """Main migration function.

    Args:
        dry_run: If True, only show what would be done.
        feed_id: If provided, only process this feed.
        lang: Language code for subtitles.
    """
    config = Config()
    paths = PathManager(
        base_data_dir=config.data_dir,
        base_tmp_dir=config.tmp_dir,
        base_url=config.base_url,
    )

    # Initialize database
    download_db = DownloadDatabase(config.db_path)
    await download_db.initialize()

    # Get all downloaded items
    downloads = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.DOWNLOADED,
        feed_id=feed_id,
    )

    logger.info(
        "Found downloads to process",
        extra={"count": len(downloads), "feed_id": feed_id or "all"},
    )

    success_count = 0
    skip_count = 0
    fail_count = 0

    for download in downloads:
        if download.transcript_ext:
            skip_count += 1
            continue

        success = await fetch_transcript_for_download(
            download=download,
            paths=paths,
            lang=lang,
            dry_run=dry_run,
        )

        if success:
            success_count += 1
            if not dry_run:
                await update_download_transcript_fields(
                    download_db, download, lang
                )
        else:
            fail_count += 1

    logger.info(
        "Migration complete",
        extra={
            "success": success_count,
            "skipped": skip_count,
            "failed": fail_count,
            "dry_run": dry_run,
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download transcripts for existing media files."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be downloaded without actually downloading",
    )
    parser.add_argument(
        "--feed-id",
        type=str,
        help="Only process downloads from a specific feed",
    )
    parser.add_argument(
        "--lang",
        type=str,
        default="en",
        help="Subtitle language to fetch (default: en)",
    )

    args = parser.parse_args()

    asyncio.run(
        main(
            dry_run=args.dry_run,
            feed_id=args.feed_id,
            lang=args.lang,
        )
    )
```

## Implementation Phases

### Phase 1: Core Infrastructure ✅
- [x] Create Alembic migration for `transcript_ext`, `transcript_lang`, `transcript_source` fields
- [x] Extend `PathManager` with `transcript_path()` and `transcript_url()` methods
- [x] Extend `FileManager` with transcript file operations
- [x] Add `/transcripts/{feed_id}/{download_id}.{ext}` route to static router

### Phase 2: yt-dlp Integration ✅
- [x] Extend `YtdlpArgs` with subtitle CLI options
- [x] Update handler metadata extraction to capture subtitle availability
- [x] Update `download_media_to_file` to download subtitles alongside media
- [x] Update database operations to store transcript metadata

### Phase 3: RSS Integration ✅
- [x] Register podcast namespace in feedgen
- [x] Implement `PodcastEntryExtension.transcript()` method
- [x] Update `with_downloads()` to emit `<podcast:transcript>` tags
- [x] Test RSS validation with podcast validators

### Phase 4: Cleanup & Migration ✅
- [x] Extend Pruner to handle transcript cleanup
- [x] Create `scripts/migrate_transcripts.py` migration script
- [x] Add database update method for transcript fields
- [x] Test end-to-end with real feeds

### Phase 5: Configuration (Future)
- [ ] Support multiple languages per feed
- [ ] AI transcription fallback (Whisper integration)

## Architecture Diagram

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Feed Config   │────▶│  Enqueuer Phase  │────▶│  yt-dlp Info    │
│ transcription:  │     │  fetch metadata  │     │  - subtitles    │
│   enabled: true │     │                  │     │  - auto_captions│
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                          │
                        ┌─────────────────────────────────┘
                        ▼
              ┌───────────────────┐
              │ Download Model    │
              │ - transcript_ext  │
              │ - transcript_lang │
              │ - transcript_src  │
              └─────────┬─────────┘
                        │
                        ▼
              ┌───────────────────┐     ┌──────────────────┐
              │ Downloader Phase  │────▶│  yt-dlp Download │
              │ --write-subs      │     │  media + .vtt    │
              │ --write-auto-subs │     └──────────────────┘
              └─────────┬─────────┘
                        │
                        ▼
              ┌───────────────────┐
              │  File Storage     │
              │  /data/{feed}/    │
              │  ├── {id}.m4a     │
              │  └── transcripts/ │
              │      └── {id}.vtt │
              └─────────┬─────────┘
                        │
                        ▼
              ┌───────────────────┐     ┌──────────────────┐
              │  RSS Generation   │────▶│  <item>          │
              │  FeedgenCore      │     │    <podcast:     │
              │                   │     │     transcript/> │
              └───────────────────┘     └──────────────────┘
                        │
                        ▼
              ┌───────────────────┐
              │  HTTP Server      │
              │  /transcripts/    │
              │    {feed}/{id}.vtt│
              └───────────────────┘
```

## Considerations

1. **Storage Impact**: VTT files are typically small (10-100KB) but will accumulate
2. **Download Time**: Subtitle download adds ~1-2s per video
3. **Language Limitations**: Initial implementation supports single language per feed
4. **Format Limitations**: VTT-only initially; SRT conversion possible via yt-dlp
5. **Availability**: Not all videos have subtitles; graceful handling required
6. **YouTube-Specific**: Auto-generated subtitles only available on YouTube
7. **Quality Variance**: Auto-generated subtitles have varying accuracy

## Future Enhancements

1. **AI Transcription Fallback**: Whisper integration when no subtitles exist
2. **Multi-Language Support**: Multiple transcript files per episode
3. **Speaker Diarization**: Enhanced VTT with speaker labels
4. **Transcript Search**: Full-text search across episode transcripts
5. **Format Options**: Support SRT, JSON (PodcastIndex), plain text outputs
