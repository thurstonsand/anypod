# YouTube Transcript API Integration

## Problem Statement

YouTube's auto-generated VTT subtitles downloaded via yt-dlp contain overlapping cues designed for real-time karaoke-style display. Each cue repeats previous text and appends new words, resulting in transcripts where every line appears 2-3 times when displayed sequentially in podcast apps.

**Example of current (broken) output:**

```txt
Thank you for joining us on the lemonade Thank you for joining us on the lemonade
  Thank you for joining us on the lemonade
stand.
Uh this week you've joined us at stand.
```

This is a YouTube-specific problem. Other sources (Patreon, Twitter) produce clean VTT files without overlapping cues.

## Proposed Solution

Use the `youtube-transcript-api` Python library for YouTube transcripts instead of yt-dlp's subtitle download. This library:

1. Fetches transcripts directly from YouTube's transcript API (not subtitle streams)
2. Returns clean, non-overlapping segments with `start`, `duration`, and `text`
3. Includes a built-in `WebVTTFormatter` that outputs proper VTT files
4. Distinguishes between creator-provided and auto-generated transcripts

**Non-YouTube sources continue using yt-dlp's VTT download unchanged.**

## Design Decisions

### 1. Integration Point: New `BaseHandler` Method

Add a new method to `SourceHandlerBase` protocol for transcript downloads:

```python
async def download_transcript(
    self,
    download_id: str,
    source_url: str,
    transcript_lang: str,
    transcript_source: TranscriptSource,
    output_path: Path,
) -> bool:
    """Download transcript for a video.

    Args:
        download_id: The video/download identifier.
        source_url: The source URL for the video.
        transcript_lang: Language code for transcripts (e.g., "en").
        transcript_source: Source type (creator or auto-generated).
        output_path: Full path where the VTT file should be written.

    Returns:
        True if transcript was downloaded successfully, False otherwise.
    """
```

Each handler implements this method to download the transcript file directly:

- **YouTube handler**: Uses `youtube-transcript-api` to fetch and write VTT
- **Other handlers**: Use yt-dlp to download VTT

### 2. `YtdlpWrapper.download_transcript_only()` Orchestrates and Verifies

The wrapper method:

1. Constructs the expected output path: `{transcripts_dir}/{download_id}.{lang}.vtt`
2. Calls `handler.download_transcript()` to perform the download
3. If handler returns `True`, verifies the file exists at the expected path
4. Returns the extension ("vtt") if file exists, `None` otherwise

This keeps file verification logic in one place, agnostic to how the file was created.

### 3. Type-Safe Wrapper Module for `youtube-transcript-api`

Create `src/anypod/ytdlp_wrapper/youtube_transcript.py` following the pattern of `feedgen_core.py`:

- Encapsulates all `youtube-transcript-api` interactions
- Handles type-unsafe library operations internally
- Exposes a single type-safe async function: `download_transcript()`
- Maps library exceptions to domain-specific exceptions

The video ID is already available as `download_id` (extracted during metadata retrieval).

### 4. Domain-Specific Exceptions

Add `YouTubeTranscriptError` to `exceptions.py` following the pattern of other errors in the file.

### 5. Async Pattern

Use `asyncio.to_thread()` to run synchronous `youtube-transcript-api` calls without blocking:

```python
transcript_list = await asyncio.to_thread(api.list, video_id)
transcript = await asyncio.to_thread(
    transcript_list.find_manually_created_transcript, [lang]
)
```

> NOTE: the above is an example. Explore the library fully to understand the most efficient way to implement.

### 6. Specific Exception Handling

Catch specific exceptions from `youtube-transcript-api` rather than the base class:

| Library Exception            | Handling                                  |
| ---------------------------- | ----------------------------------------- |
| `NoTranscriptFound`          | Log warning, return `False`               |
| `TranscriptsDisabled`        | Log warning, return `False`               |
| `VideoUnavailable`           | Log warning, return `False`               |
| `AgeRestricted`              | Log warning, return `False`               |
| `IpBlocked`                  | Log error, raise `YouTubeTranscriptError` |
| `RequestBlocked`             | Log error, raise `YouTubeTranscriptError` |
| Network errors (`HTTPError`) | Log error, raise `YouTubeTranscriptError` |

### 7. Language and Source Priority Handling

The `youtube-transcript-api` supports the same concepts as yt-dlp:

- `find_manually_created_transcript(['en'])` → `TranscriptSource.CREATOR`
- `find_generated_transcript(['en'])` → `TranscriptSource.AUTO`

The `transcript_source` parameter determines which method to call.

## Edge Cases and Error Handling

| Scenario                       | Handling                                                   |
| ------------------------------ | ---------------------------------------------------------- |
| Video has no transcripts       | Return `False` (same as current behavior)                  |
| Requested language unavailable | Return `False`, log warning                                |
| Video is age-restricted        | Log warning, return `False`                                |
| Video is unavailable/private   | Log warning, return `False`                                |
| IP blocked by YouTube          | Log error, raise `YouTubeTranscriptError`                  |
| Network errors                 | Raise `YouTubeTranscriptError` (let caller handle retries) |

## Rejected Alternatives

### 1. Download json3 and Convert to VTT

**Rejected because:**

- Requires building custom json3 parser
- json3 format is undocumented and could change
- `youtube-transcript-api` already solves this problem with a maintained library

### 2. Post-process VTT to Deduplicate Overlapping Cues

**Rejected because:**

- Complex heuristics needed to detect and merge overlapping text
- Timestamp alignment is tricky when cues have sub-second overlaps
- More fragile than using a clean source

### 3. Apply to All Sources

**Rejected because:**

- Patreon/Twitter VTT files are already clean
- `youtube-transcript-api` only works with YouTube
- Adding unnecessary dependencies for non-YouTube sources

## Integration Points

### Files to Modify

1. **`src/anypod/exceptions.py`**

   - Add `YouTubeTranscriptError` exception class

2. **`src/anypod/ytdlp_wrapper/handlers/base_handler.py`**

   - Add `download_transcript()` method to `SourceHandlerBase` protocol

3. **`src/anypod/ytdlp_wrapper/handlers/youtube_handler.py`**

   - Implement `download_transcript()` using `youtube_transcript` module

4. **`src/anypod/ytdlp_wrapper/handlers/patreon_handler.py`**

   - Implement `download_transcript()` using yt-dlp

5. **`src/anypod/ytdlp_wrapper/handlers/twitter_handler.py`**

   - Implement `download_transcript()` using yt-dlp

6. **`src/anypod/ytdlp_wrapper/ytdlp_wrapper.py`**
   - Modify `download_transcript_only()` to delegate to handler, then verify file exists

### Files to Create

1. **`src/anypod/ytdlp_wrapper/youtube_transcript.py`**
   - Type-safe wrapper module for `youtube-transcript-api`
   - Single exposed function: `download_transcript()`

### Files Unchanged

- `src/anypod/data_coordinator/downloader.py` - calls `download_transcript_only()` unchanged
- `src/anypod/state_reconciler.py` - calls `download_transcript_only()` unchanged
- `src/anypod/server/routers/static.py` - serves VTT files unchanged
- `src/anypod/rss/feedgen_core.py` - references transcripts unchanged

## Implementation Plan

Each stage is a complete, committable unit of work. After completing each stage, run `uv run pre-commit run --all-files` and commit the changes.

---

### Stage 0: Add Dependency

Add the `youtube-transcript-api` package dependency.

- [ ] Run `uv add youtube-transcript-api`
- [ ] Verify `pyproject.toml` and `uv.lock` are updated

---

### Stage 1: Add Domain Exception

Add the `YouTubeTranscriptError` exception class.

- [ ] Add `YouTubeTranscriptError` to `src/anypod/exceptions.py`
  - Attributes: `video_id`, `lang`
  - Follow `FFProbeError` pattern

---

### Stage 2: Create YouTube Transcript Module

Create the type-safe wrapper module for `youtube-transcript-api`.

- [ ] Create `src/anypod/ytdlp_wrapper/youtube_transcript.py`:
  - [ ] Import and type-ignore the `youtube-transcript-api` library
  - [ ] Implement async `download_transcript(video_id, lang, source, output_path) -> bool`
    - Use `asyncio.to_thread()` for blocking API calls
    - List available transcripts via `api.list(video_id)`
    - Select transcript based on `source` parameter (CREATOR → `find_manually_created_transcript`, AUTO → `find_generated_transcript`)
    - Fetch transcript and format to VTT using `WebVTTFormatter`
    - Write VTT content to `output_path`
    - Return `True` on success
  - [ ] Handle specific exceptions:
    - `NoTranscriptFound`, `TranscriptsDisabled`, `VideoUnavailable`, `AgeRestricted` → log warning, return `False`
    - `IpBlocked`, `RequestBlocked`, `HTTPError` → raise `YouTubeTranscriptError`
- [ ] Add unit tests for `download_transcript()` behavior (mock the API)

---

### Stage 3: Add Handler Protocol Method

Add `download_transcript()` method to `SourceHandlerBase` protocol.

- [ ] Add method signature to `src/anypod/ytdlp_wrapper/handlers/base_handler.py`:
  - Parameters: `download_id`, `source_url`, `transcript_lang`, `transcript_source`, `output_path`
  - Returns: `bool`

---

### Stage 4: Implement Handler Methods

Implement `download_transcript()` in all handlers.

- [ ] **YouTubeHandler** (`youtube_handler.py`):
  - [ ] Call `youtube_transcript.download_transcript()` with `download_id` as `video_id`
  - [ ] Return result
- [ ] **PatreonHandler** (`patreon_handler.py`):
  - [ ] Build yt-dlp args: `sub_format("vtt")`, `sub_langs()`, `convert_subs("vtt")`, `write_subs()` or `write_auto_subs()`, paths, output template
  - [ ] Call `YtdlpCore.download()` with the args
  - [ ] Return `True` on success, `False` on failure
- [ ] **TwitterHandler** (`twitter_handler.py`):
  - [ ] Same implementation as PatreonHandler
- [ ] Add unit tests for handler `download_transcript()` methods

---

### Stage 5: Update YtdlpWrapper

Modify `download_transcript_only()` to use the new handler method.

- [ ] Refactor `download_transcript_only()` in `ytdlp_wrapper.py`:
  - [ ] Get handler via `_handler_selector.select(source_url)`
  - [ ] Construct expected output path: `{transcripts_dir}/{download_id}.{lang}.vtt`
  - [ ] Call `handler.download_transcript(download_id, source_url, transcript_lang, transcript_source, output_path)`
  - [ ] If returns `False`, return `None`
  - [ ] Verify file exists at output path (using `_find_and_normalize_transcript` or similar)
  - [ ] Return "vtt" if file exists, `None` otherwise
- [ ] Remove old yt-dlp args building logic from wrapper (now in handlers)
- [ ] Add/update tests for `download_transcript_only()`

---

### Stage 6: Integration Testing and Verification

Final verification with real YouTube content.

- [ ] Add integration test (mark with `--integration`):
  - [ ] Download transcript for a known YouTube video
  - [ ] Verify VTT file content is clean (no duplicate lines)
- [ ] Manual verification:
  - [ ] Run against a real feed with YouTube content
  - [ ] Verify transcript displays correctly in a podcast app

---

### Verification Commands

Run after each stage:

```bash
uv run pre-commit run --all-files
uv run pytest
```

Run for integration tests:

```bash
uv run pytest --integration
```

Run for type safety:

```bash
uv run pre-commit run --all-files
```
