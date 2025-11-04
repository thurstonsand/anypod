# X / Twitter Handler

## Overview

Extend Anypod's yt-dlp wrapper so feeds can ingest X (Twitter) video posts the same way we support YouTube and Patreon sources. The new handler must plug into the existing handler-selection model, reuse the current database types, and follow the same logging, error, and testing conventions established for other platforms.

## Research Summary

### Data collection

- Pulled metadata for four different X posts (10 sec, 3:39, 14:49, and 73:39) using `uvx` with yt-dlp's master/nightly build. Each run succeeded without additional headers or cookies, confirming baseline extraction works for public VOD posts.【1edc11†L1-L2】【18f5b7†L1-L2】【be5949†L1-L2】【12cc73†L1-L2】
- Verified that yt-dlp resolves both `x.com/.../status/...` and `twitter.com/.../status/...` URLs; both map to the same extractor (`Twitter`).【f42f39†L1-L4】【af3f65†L1-L2】【40cadc†L1-L1】
- Attempted to fetch metadata for the provided YouTube sample, but the environment blocks YouTube requests with repeated `Tunnel connection failed: 403 Forbidden` responses. Further comparison with live YouTube metadata must be done locally where YouTube is reachable.【87c0f0†L1-L13】

### Observed X metadata patterns

| Sample | Duration(s) | Duration String | Like Count | View Count | Filesize Approx | Ext | Top-level vcodec | Top-level acodec | Channel ID | Uploader ID | Domain |
|---|---|---|---|---|---|---|---|---|---|---|---|
| x1 | 11.666 | 11 | 25632 |  | 3173152 | mp4 |  |  | 1485068728412913666 | interesting_aIl | x.com |
| x2 | 219.752 | 3:39 | 917 |  | 59772544 | mp4 |  |  | 291797158 | ThePrimeagen | x.com |
| x3 | 889.725 | 14:49 | 813 |  | 2794626225 | mp4 |  |  | 12819682 | mitchellh | x.com |
| x4 | 4419.915 | 1:13:39 | 3040 |  | 1202216880 | mp4 |  |  | 1881451105454002176 | RapidResponse47 | x.com |

Additional notes from the JSON dumps:

- `extractor`/`extractor_key` is always `twitter`, matching yt-dlp's naming rather than "x".
- Every sample returns exactly one `requested_downloads` entry pointing at a direct `video.twimg.com` MP4 URL (no manifest). Formats additionally expose HLS variants via `m3u8_native`, but direct HTTPS MP4 renditions exist for 270p/360p/720p tiers.【00bfef†L1-L5】【badc63†L1-L10】
- `filesize` is absent while `filesize_approx` is populated both at the root and inside `requested_downloads` entries.【81a3fa†L1-L9】
- `duration` is provided as a float, even for long videos; `duration_string` follows `H:MM:SS` formatting where applicable. Published timestamps are exposed via `timestamp` (Unix epoch) and `upload_date` (YYYYMMDD string).【ae83b3†L1-L3】
- Engagement metrics include `like_count`, `comment_count`, and `repost_count`, but `view_count` is consistently null.【5a2c26†L8-L12】【ccc3f7†L1-L5】
- Thumbnails point at `pbs.twimg.com` JPG URLs; uploader metadata (`uploader`, `uploader_id`, `uploader_url`) remains on the legacy `twitter.com` domain.【23425d†L1-L9】【e1d7b9†L1-L1】

### Comparison with existing handlers

- The YouTube handler expects populated `ext`, `duration`, MIME lookup from extension, direct thumbnail, and `webpage_url`. These fields appear in the minimal fixture used by current unit tests, which we can rely on as the canonical contract in this codebase.【8df9b6†L23-L67】
- Patreon logic demonstrates how we handle absent durations (`FFProbe` fallback), rely on `filesize_approx`, apply platform-specific referers, and use `requested_downloads` to pick a concrete attachment when playlist entries contain multiple assets.【1d80f9†L1-L36】【b29373†L1-L4】【ca41ad†L1-L23】

Compared to both platforms, X posts:

- Provide a single video asset per status, so playlist handling can stay simple (no multi-attachment indexes like Patreon).
- Expose direct MP4 URLs via `requested_downloads` and `formats`, so we can avoid HLS downloads without extra arguments.
- Lack reliable view counts and top-level codec metadata, so we must derive MIME types purely from file extensions and fall back to known defaults when fields are missing.

## Existing Architecture Touchpoints

- `HandlerSelector` dispatches URLs to source-specific handlers, defaulting to YouTube. Patreon registers `patreon.com` suffixes and injects `FFProbe` for duration probing.【c1164d†L1-L24】
- `YtdlpWrapper` orchestrates discovery, metadata enumeration, thumbnail downloads, and media downloads by delegating to handler hooks for argument shaping (`prepare_*_args`) and parsing (`extract_*`).【4105a3†L1-L87】
- Handlers wrap yt-dlp dictionaries with typed helpers (`YoutubeEntry`, `PatreonEntry`) to centralize error handling, field coercion, and defaults before instantiating `Download` rows.【852c83†L1-L120】【6b406a†L1-L45】

## Proposed Implementation

### Handler selection

- Add an `XHandler` (likely named `TwitterHandler` to align with yt-dlp's extractor naming) registered for both `x.com` and `twitter.com` hostnames, keeping YouTube as the default. Update `HandlerSelector` and `handlers/__init__.py` to expose the new type and reuse the injected `FFProbe` when constructing the handler.【c1164d†L9-L24】

### Handler responsibilities

Implement `TwitterHandler` mirroring the structure of `PatreonHandler` / `YoutubeHandler`:

- **determine_fetch_strategy**
  - Run `yt-dlp` with `skip_download + flat_playlist` to classify the URL. Status URLs should return `_type = video` and extractor `twitter`. Treat all supported URLs as `SourceType.SINGLE_VIDEO`. For now we will not support list/timeline URLs because yt-dlp rejected simple profile URLs in research; document this limitation and raise `SourceType.UNKNOWN` if yt-dlp reports a playlist-like type.【778ebb†L1-L2】

- **Argument preparation**
  - `prepare_playlist_info_args` / `prepare_downloads_info_args` / `prepare_media_download_args` can remain no-ops unless we observe auth requirements. Keep placeholders to add referer or headers later without rewriting call sites.
  - Ensure download args favor MP4 renditions: explicitly set `args.format("bv+ba/b")` is unnecessary because direct MP4s surface as default, but we may want to select `bestvideo*+bestaudio/best` to guard against extractor regressions. Include this in design as a TODO for implementation validation.

- **Metadata parsing**
  - Create a `TwitterEntry` helper that exposes `download_id`, `title`, `timestamp`, `duration`, `thumbnail`, `description`, `filesize`, `quality_info`, and `requested_download_urls`. Follow the same exception-wrapping approach as other handlers to produce `YtdlpTwitterDataError` / `YtdlpTwitterPostFilteredOutError` when required fields are missing or the post lacks video evidence.
  - Published datetime: prefer `timestamp`; if absent, fall back to `upload_date` (parse `%Y%m%d`). If both missing, raise a parse error.
  - Duration: cast the float to `int`, treating `None` or invalid values as errors (Twitter posts always returned a value in research). Keep a fallback to call `FFProbe` on the direct MP4 when duration is zero to future-proof against live clips.
  - Filesize: use `filesize` or `filesize_approx` (required by `Download`). Raise when both are missing because the DB column requires `gt=0`.
  - MIME type / extension: top-level `ext` is `mp4`, so default to `.mp4` and `video/mp4` when empty. If future formats emit audio-only or other containers, consider inspecting the first requested download URL for file extension before raising.
  - Thumbnail: prefer `thumbnail` and fall back to the best JPG/PNG entry from `thumbnails` similar to Patreon.
  - Source URL: use `webpage_url`, falling back to `original_url`.
  - Quality info: derive from `resolution` or `height` in `requested_downloads[0]` so downstream RSS descriptions stay informative.

- **Download metadata assembly**
  - Populate `Download` with `status=DownloadStatus.QUEUED`, remote thumbnail URL, description, and optional engagement metrics if we decide to surface them later (non-blocking). Playlist index should remain `None`.

### Wrapper integration

- Update `YtdlpWrapper.discover_feed_properties`, `fetch_playlist_metadata`, `fetch_new_downloads_metadata`, and `download_media_to_file` to rely on the new handler through the existing selector—no direct code changes beyond ensuring we pass through Twitter-specific args.
- Confirm manual submission flows inherit support automatically once the handler is registered (the manual design doc expects handler reuse). Document this in release notes.

### Testing

- Mirror existing handler tests: unit tests for discovery classification, feed metadata extraction, download parsing success/failure, duration fallback, missing required fields, and handler selector routing.
- Add regression tests verifying we use `filesize_approx`, convert `duration` floats correctly, and respect fallback URLs when `requested_downloads` is empty.
- Extend `tests/anypod/ytdlp_wrapper/test_ytdlp_wrapper.py` to ensure `HandlerSelector` picks `TwitterHandler` for `x.com` and `twitter.com` hosts, keeping YouTube/Patreon unaffected.

### Documentation & configuration

- Update `README.md` / `DESIGN_DOC.md` to list X/Twitter support and call out limitations (public posts only, timelines unsupported without additional auth).
- Mention in configuration docs that cookies may be required for private posts, though this is unverified.

## Open Questions / Risks

- Authentication: yt-dlp handled public posts without cookies, but we do not know how subscriber-only or age-restricted posts behave. Need real-world tests before promising support.
- Long-form reliability: The 73-minute sample reports `filesize_approx` but not actual `filesize`. We should confirm yt-dlp's download pipeline honors the approximate size and that the direct MP4 URL remains stable across large files.
- Timeline/list URLs: Research indicates a simple profile URL is rejected. If we want to support creator-wide feeds, we must investigate yt-dlp's `twitter:user` extractor and whether it requires `--referer` or cookies. Design assumes status URLs only for now.【778ebb†L1-L2】
- HLS vs MP4: Although current posts expose direct MP4s, we need to verify this holds for all content types (e.g., streamed spaces, vertical clips). Implementation should gracefully fallback to HLS if direct MP4 links disappear.
- Engagement metadata: `view_count` is null, so we cannot expose reliable play counts. Decide whether to surface likes/comments in RSS descriptions or ignore them.

## Implementation Addendum - Playlist Investigation

During implementation testing, I investigated whether X/Twitter URLs could provide playlist-like behavior. Here are the findings:

### Playlist Testing Results

**No playlist support discovered** - After extensive testing with various X/Twitter URL patterns:
- Profile URLs (`https://x.com/username`) are not supported by yt-dlp's Twitter extractor
- User timeline URLs fall back to generic extractor with errors
- Only individual status URLs (`x.com/.../status/...` or `twitter.com/.../status/...`) work reliably

**Confirmed working patterns:**
- `https://x.com/username/status/123456789` ✅
- `https://twitter.com/username/status/123456789` ✅

**Tested patterns that do NOT work:**
- `https://x.com/username` ❌ (profile page)
- `https://x.com/username/with_replies` ❌
- `https://x.com/username/media` ❌
- `https://x.com/username/timeline` ❌

### Implementation Decision

Based on this investigation, the implementation correctly assumes **single video only** for X/Twitter URLs. The `determine_fetch_strategy` method in `TwitterHandler` only returns `SourceType.SINGLE_VIDEO` for the `twitter` extractor, and any other extractor types fall back to `SourceType.UNKNOWN`.

This aligns with yt-dlp's behavior where the Twitter extractor is designed specifically for individual status posts, not user timelines or collections. If playlist-like behavior is needed in the future, it would require either:
1. Changes to yt-dlp's Twitter extractor to support user timelines
2. Custom logic to handle multiple status URLs (outside the scope of this handler)

The current implementation provides reliable support for the use case that works consistently: individual X/Twitter video status posts.

