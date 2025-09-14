# Patreon Handler

## Overview

Add first-class support for Patreon sources alongside YouTube. Users can subscribe to Patreon creator pages or individual posts and have them converted into RSS podcast feeds. This integrates with the existing handler-based yt-dlp wrapper, reusing the database, coordinator, and RSS systems without schema changes.

Goals:
- Support Patreon creator pages (playlist-like) and single posts.
- Respect Patreon-specific requirements: cookies and referer header.
- Default to video-focused feeds by filtering out audio-only posts (configurable later).
- Keep changes minimal, aligned with existing handler/wrapper patterns.

## Problem / Goal

- Enable Patreon as a supported source in `ytdlp_wrapper` so feeds can be configured with Patreon URLs.
- Ensure authenticated access via `cookies.txt` is respected when needed.
- Prevent 403s by setting `--referer https://www.patreon.com`.
- Avoid audio-only items by default using `--match-filter vcodec`.

## Context / Findings

Patreon behaves similarly to YouTube for yt-dlp extraction:
- Creator pages: `extractor` = `patreon:campaign`, `_type = playlist` with many entries (URLs to posts).
- Posts: `extractor` = `patreon`, `_type = video` (or flat `_type = url`).
- Common fields align with YouTube (title, description, timestamp, uploader, etc.). Differences observed:
  - Audio-only posts frequently have `ext=mp3`, `vcodec` unset/null, often missing `duration` and `thumbnail`.
  - Video posts (e.g., via Mux) include vcodec/acodec, thumbnails, and formats.
  - Some posts require higher tier access; yt-dlp may emit partial results with a non-zero exit code for inaccessible entries.
    - In this case, we can add these posts to the list in case the user may get access in the future, but it should otherwise not block execution

Operational caveats:
- Cookies are typically required for paywalled content.
- Setting the referer header helps avoid 403s.
- `--match-filter vcodec` reliably filters out audio-only posts.

## Assumptions

- Cookies file is available at the configured path when needed for Patreon.
- No DB or RSS schema changes are necessary; existing types/models suffice.
- Existing `SourceType` enum is sufficient (SINGLE_VIDEO, PLAYLIST, CHANNEL, UNKNOWN). Patreon creator pages map best to PLAYLIST; single posts map to SINGLE_VIDEO.
- We will add an option in the future to switch between video or audio only -- for now, we will start with defaulting to video

## Constraints

- Follow `SourceHandlerBase` protocol; do not refactor unrelated systems.
- Keep YouTube behavior unchanged and backward compatible.
- Reuse `YtdlpInfo`, `Download`, `Feed`, and the current orchestration model.
- Minimize surface area: handler selection + small arg builder additions.

## Impacted Modules

- `src/anypod/ytdlp_wrapper/core/args.py`: Add `referer(url: str)` and `match_filter(expr: str)` builder methods.
- `src/anypod/ytdlp_wrapper/patreon_handler.py` (new): Implement `SourceHandlerBase` for Patreon.
- `src/anypod/ytdlp_wrapper/patreon_handler.py`: Add typed `PatreonEntry` helper using `YtdlpInfo`.
- `src/anypod/ytdlp_wrapper/patreon_handler.py`: Add Patreon-specific error classes.
- `src/anypod/ytdlp_wrapper/ytdlp_wrapper.py`: Select handler by URL host; apply referer/match-filter for Patreon.
- `src/anypod/ytdlp_wrapper/core/core.py`: Tolerate partial success in `extract_downloads_info` when some JSON lines parsed.

## Design Details

### Handler Selection

- Add `_select_handler(url: str) -> SourceHandlerBase` in `YtdlpWrapper`.
  - `patreon.com` → `PatreonHandler`
  - otherwise → `YoutubeHandler`
- Use fresh handler selection in each high-level call (no shared mutation).

### Patreon Source Type Mapping

- Map `patreon:campaign` (creator pages/collections) → `SourceType.PLAYLIST`.
- Map `patreon` (individual posts) → `SourceType.SINGLE_VIDEO`.

Rationale: Creator pages behave like playlists of posts; mapping to PLAYLIST avoids YouTube-specific channel tab logic while keeping behavior consistent for filtering/thumbnail flows.

### YtdlpArgs Enhancements

- Implement `referer(url: str)` → append `--referer <url>`.
- Implement `match_filter(expr: str)` → append `--match-filter <expr>`.

Usage policy for Patreon:
- Always apply `args.referer('https://www.patreon.com')` for discovery, playlist enumeration, thumbnails, and downloads.
- Apply `args.match_filter('vcodec')` for enumeration and single-post operations by default to skip audio-only posts.

### Text-only Posts Identification

- Confirm how yt-dlp represents text-only Patreon posts (no media). We don't know yet how, so this requiures furthe investigation.
- Ensure `match-filter vcodec` excludes such posts during enumeration; additionally, in single-post paths treat missing `ext` and no video evidence as filtered out.
- If the provided example channel https://patreon.com/LemonadeStand channel lacks text-only examples, request another Patreon channel from the reviewer to validate behavior.

### Error Handling

- Add `YtdlpPatreonDataError` mirroring YouTube’s data errors (preserve `feed_id`/`download_id`).
- Add `YtdlpPatreonPostFilteredOutError` for entries filtered out (e.g., no video evidence).
- Preserve structured logging and context (feed_id, download_id).

### Partial Success Tolerance (Enumeration)

- Adjust `YtdlpCore.extract_downloads_info` to parse stdout lines first; if non-zero exit code but at least one valid JSON line was parsed, return parsed entries without error. Only raise when there are zero entries and a non-zero exit.

### Metadata Parsing Behavior

- Discovery (`determine_fetch_strategy`):
  - Use `extract_playlist_info` with referer applied.
  - `_type == 'playlist'` → PLAYLIST; `_type in {'url','video'}` → SINGLE_VIDEO; else fall back to resolved URL with UNKNOWN.

- Feed metadata (`extract_feed_metadata`):
  - Populate `title`, `author` (`uploader` then `channel`), `description`, feed-level `thumbnail` if present, and `last_successful_sync` from `epoch`.
  - Respect `source_type` from discovery and include `source_url`.

- Download metadata (`extract_download_metadata`):
  - Require `id`, `title`, and published datetime (prefer `timestamp`; then `upload_date`; then `release_timestamp`).
  - Duration: Patreon often omits it; default to `0` without raising when not live.
  - Extension/MIME: If `ext` exists, map normally; if missing but `vcodec`/`formats` indicate video, default to `mp4`/`video/mp4`. If no video evidence (e.g., audio-only filtered), raise `YtdlpPatreonPostFilteredOutError`.
  - `source_url`: prefer `webpage_url`, fallback to `original_url`.
  - Thumbnails: may be absent for audio-only posts; tolerate gracefully.
  - No live/upcoming handling for Patreon posts.

### Documentation

- [ ] Update README.md with end-user guidance:
  - [ ] Note default behavior: downloads video content only (for now).
  - [ ] Mark Patreon support as beta due to limited channel coverage.
- [ ] Update DESIGN_DOC.md with implementation notes consistent with other sections.

## Step-by-Step Implementation Plan

- [x] Args builder: add `referer(url: str)` and `match_filter(expr: str)` to `YtdlpArgs`.
- [ ] Args builder tests: covered via wrapper-level tests (no standalone file).

- [x] PatreonHandler: implement `determine_fetch_strategy` (no channel-tabs logic), classify PLAYLIST vs SINGLE_VIDEO.
- [x] PatreonEntry: implement typed helper wrapping `YtdlpInfo` with required field checks.
- [x] PatreonHandler: implement `extract_feed_metadata`.
- [x] PatreonHandler: implement `extract_download_metadata` with extension/mime defaults and duration=0 fallback.
- [x] Patreon errors: implement `YtdlpPatreonDataError` and `YtdlpPatreonPostFilteredOutError`.

- [ ] Wrapper: add `_select_handler(url)` and use in all flows:
  - [ ] `discover_feed_properties`
  - [ ] `fetch_playlist_metadata`
  - [ ] `download_feed_thumbnail`
  - [ ] `fetch_new_downloads_metadata`
  - [ ] `download_media_to_file`
- [ ] Wrapper (Patreon): apply `referer('https://www.patreon.com')` universally.
- [ ] Wrapper (Patreon): apply `match_filter('vcodec')` in enumeration and single-post paths.
- [ ] Ensure YouTube behavior unchanged.

- [ ] Partial success tolerance: experiment with yt-dlp outputs to confirm stderr/exit-code patterns.
- [ ] Partial success tolerance: update `YtdlpCore.extract_downloads_info` to return parsed entries on non-zero exit if at least one JSON line valid; else raise.

- [ ] Text-only posts: confirm yt-dlp behavior for text-only entries (no media/formats).
- [ ] Text-only posts: ensure enumeration excludes them (via match-filter) and single-post parse treats them as filtered.
- [ ] Text-only posts: if no text-only example found on target channel, request alternative channel to test.

- [ ] Tests: Patreon handler discovery classification (single/playlist/unknown) with mocked `extract_playlist_info`.
- [ ] Tests: Patreon entry parsing (success, missing publish dt → error, missing ext + no video evidence → filtered, defaults when formats indicate video, text-only → filtered).
- [ ] Tests: Wrapper dispatch (URL → handler) and arg augmentation (referer/match-filter present for Patreon; YouTube unaffected).
- [ ] Tests: Core partial success tolerance in `extract_downloads_info`.

## Test Plan

- [ ] Unit: `test_patreon_determine_fetch_strategy_*` (single, playlist, unknown).
- [ ] Unit: `test_patreon_extract_download_metadata_*` (success, missing publish dt → error, audio-only → filtered, text-only → filtered).
- [ ] Unit: `test_wrapper_selects_patreon_handler_and_sets_referer_and_match_filter`.
- [ ] Unit: `test_extract_downloads_info_tolerates_partial_success`.
- [ ] Unit: Ensure existing YouTube tests continue to pass.

- [ ] Integration (manual/optional): With valid Patreon cookies, run targeted extraction against public/free posts to validate end-to-end behavior. Keep out of CI by default.
- [ ] Integration (manual/optional): Validate text-only post handling; if no example available, request alternate channel/sample.

## Backward Compatibility

- No config changes required. Existing YouTube feeds unaffected.
- Patreon without cookies may yield empty results or errors surfaced as `YtdlpApiError`.
  - [ ] Make sure there is an integration test that confirms behavior with no cookie
- No DB or RSS schema modifications.

## Open Questions / Future Work

- [ ] Audio-only Patreon posts: add opt-in config to include audio-only (skip `match-filter vcodec`).
- Tier-based filtering / error surfaces for inaccessible posts.
  - [ ] Is there any metadata that would inform whether the inaccessible post has video content, or is that blocked entirely
  - [ ] Behavior should be to simply queue the inaccessible post for download, but otherwise continue execution (unless we can ascertain that it DOESN'T have a video, in which case we skip)
- [ ] Centralize shared entry parsing across handlers to reduce duplication as more sources are added.
- [ ] Consider feed-level thumbnail behavior for Patreon if playlist thumbnails are inconsistently available.

## Review Gate (PRD)

Stop here for review/approval. After approval:
- [ ] Implement scoped changes and add unit tests per plan.
- [ ] Run `uv run pytest` and `uv run pre-commit run --all-files` to validate.
- [ ] Provide a brief summary with file references on handoff.
