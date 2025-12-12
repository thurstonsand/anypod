"""Integration tests for youtube_transcript module with real YouTube transcripts."""

# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from anypod.db.types import TranscriptSource
from anypod.ytdlp_wrapper.youtube_transcript import download_transcript

# Video with both creator subtitles (en, de) and auto-generated captions
# "Me at the zoo" - first video ever uploaded to YouTube
VIDEO_WITH_CREATOR_SUBS = "jNQXAC9IVRw"

# Video with auto-generated captions only (no creator subtitles)
# "VFX Artists React to Bad and Great CGi 173"
VIDEO_WITH_AUTO_SUBS_ONLY = "ZY6TS8Q4C8s"

# Video with no subtitles at all
# "Big Buck Bunny 60fps 4K"
VIDEO_WITH_NO_SUBS = "aqz-KE-bpKQ"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_creator_transcript_success(
    tmp_path: Path,
    cookies_path: Path | None,
):
    """Verifies downloading creator-provided subtitles returns valid VTT content."""
    output_path = tmp_path / "transcript.vtt"

    result = await download_transcript(
        video_id=VIDEO_WITH_CREATOR_SUBS,
        lang="en",
        source=TranscriptSource.CREATOR,
        output_path=output_path,
        cookies_path=cookies_path,
    )

    assert result is True
    assert output_path.exists()

    content = output_path.read_text(encoding="utf-8")
    assert content.startswith("WEBVTT")
    assert "-->" in content


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_auto_transcript_success(
    tmp_path: Path,
    cookies_path: Path | None,
):
    """Verifies downloading auto-generated subtitles returns valid VTT content."""
    output_path = tmp_path / "transcript.vtt"

    result = await download_transcript(
        video_id=VIDEO_WITH_AUTO_SUBS_ONLY,
        lang="en",
        source=TranscriptSource.AUTO,
        output_path=output_path,
        cookies_path=cookies_path,
    )

    assert result is True
    assert output_path.exists()

    content = output_path.read_text(encoding="utf-8")
    assert content.startswith("WEBVTT")
    assert "-->" in content


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_transcript_creates_parent_directories(
    tmp_path: Path,
    cookies_path: Path | None,
):
    """Verifies transcript download creates parent directories if needed."""
    output_path = tmp_path / "nested" / "dirs" / "transcript.vtt"

    result = await download_transcript(
        video_id=VIDEO_WITH_CREATOR_SUBS,
        lang="en",
        source=TranscriptSource.CREATOR,
        output_path=output_path,
        cookies_path=cookies_path,
    )

    assert result is True
    assert output_path.exists()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_transcript_unavailable_returns_false(
    tmp_path: Path,
    cookies_path: Path | None,
):
    """Verifies requesting unavailable transcripts returns False without raising."""
    output_path = tmp_path / "transcript.vtt"

    result = await download_transcript(
        video_id=VIDEO_WITH_NO_SUBS,
        lang="en",
        source=TranscriptSource.AUTO,
        output_path=output_path,
        cookies_path=cookies_path,
    )

    assert result is False
    assert not output_path.exists()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_creator_transcript_when_only_auto_available(
    tmp_path: Path,
    cookies_path: Path | None,
):
    """Verifies requesting creator subs when only auto exists returns False."""
    output_path = tmp_path / "transcript.vtt"

    result = await download_transcript(
        video_id=VIDEO_WITH_AUTO_SUBS_ONLY,
        lang="en",
        source=TranscriptSource.CREATOR,
        output_path=output_path,
        cookies_path=cookies_path,
    )

    assert result is False
    assert not output_path.exists()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_transcript_unsupported_language_returns_false(
    tmp_path: Path,
    cookies_path: Path | None,
):
    """Verifies requesting a non-existent language returns False."""
    output_path = tmp_path / "transcript.vtt"

    result = await download_transcript(
        video_id=VIDEO_WITH_CREATOR_SUBS,
        lang="xyz",
        source=TranscriptSource.CREATOR,
        output_path=output_path,
        cookies_path=cookies_path,
    )

    assert result is False
    assert not output_path.exists()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_transcript_not_available_source_returns_false(
    tmp_path: Path,
    cookies_path: Path | None,
):
    """Verifies NOT_AVAILABLE source type returns False."""
    output_path = tmp_path / "transcript.vtt"

    result = await download_transcript(
        video_id=VIDEO_WITH_CREATOR_SUBS,
        lang="en",
        source=TranscriptSource.NOT_AVAILABLE,
        output_path=output_path,
        cookies_path=cookies_path,
    )

    assert result is False
    assert not output_path.exists()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_download_transcript_different_language(
    tmp_path: Path,
    cookies_path: Path | None,
):
    """Verifies downloading non-English creator subtitles works correctly."""
    output_path = tmp_path / "transcript.vtt"

    result = await download_transcript(
        video_id=VIDEO_WITH_CREATOR_SUBS,
        lang="de",
        source=TranscriptSource.CREATOR,
        output_path=output_path,
        cookies_path=cookies_path,
    )

    assert result is True
    assert output_path.exists()

    content = output_path.read_text(encoding="utf-8")
    assert content.startswith("WEBVTT")
