"""Type-safe wrapper for youtube-transcript-api library.

This module encapsulates all youtube-transcript-api interactions, providing
a clean async interface for downloading YouTube transcripts as VTT files.
"""

import asyncio
from http.cookiejar import MozillaCookieJar
import logging
from pathlib import Path

import aiofiles
import aiofiles.os
import requests
from requests.exceptions import RequestException
from youtube_transcript_api import (
    AgeRestricted,
    IpBlocked,
    NoTranscriptFound,
    RequestBlocked,
    TranscriptsDisabled,
    VideoUnavailable,
    YouTubeTranscriptApi,
)
from youtube_transcript_api._errors import YouTubeRequestFailed
from youtube_transcript_api.formatters import WebVTTFormatter

from ..db.types import TranscriptSource
from ..exceptions import YouTubeTranscriptError, YouTubeTranscriptUnavailableError

logger = logging.getLogger(__name__)


def _create_api(cookies_path: Path | None) -> YouTubeTranscriptApi:
    """Create a YouTubeTranscriptApi instance with optional cookie authentication.

    Args:
        cookies_path: Path to a Netscape-format cookies.txt file, or None.

    Returns:
        A configured YouTubeTranscriptApi instance.
    """
    if cookies_path is None:
        return YouTubeTranscriptApi()

    session = requests.Session()
    cookie_jar = MozillaCookieJar(cookies_path)
    cookie_jar.load(ignore_discard=True, ignore_expires=True)
    session.cookies.update(cookie_jar)  # pyright: ignore[reportUnknownMemberType]
    return YouTubeTranscriptApi(http_client=session)


async def _fetch_and_format_transcript(
    video_id: str,
    lang: str,
    source: TranscriptSource,
    cookies_path: Path | None,
) -> str:
    """Fetch transcript from YouTube API and format as VTT.

    Args:
        video_id: The YouTube video ID.
        lang: Language code for the transcript.
        source: Source type (CREATOR or AUTO).
        cookies_path: Path to cookies.txt file for authentication, or None.

    Returns:
        VTT-formatted transcript string.

    Raises:
        YouTubeTranscriptUnavailableError: When the video has no transcripts
            available (disabled, unavailable, age-restricted) or when the
            requested source/language combination is not found.
        YouTubeTranscriptError: When YouTube blocks the request or network
            errors occur.
    """
    api = _create_api(cookies_path)

    try:
        transcript_list = await asyncio.to_thread(api.list, video_id)
    except (TranscriptsDisabled, VideoUnavailable, AgeRestricted) as e:
        raise YouTubeTranscriptUnavailableError(video_id=video_id) from e
    except (IpBlocked, RequestBlocked) as e:
        raise YouTubeTranscriptError(
            message="YouTube blocked transcript request.",
            video_id=video_id,
        ) from e
    except RequestException as e:
        raise YouTubeTranscriptError(
            message="Network error fetching transcript list.",
            video_id=video_id,
        ) from e

    if source == TranscriptSource.CREATOR:
        try:
            transcript = transcript_list.find_manually_created_transcript([lang])
        except NoTranscriptFound as e:
            raise YouTubeTranscriptUnavailableError(video_id=video_id, lang=lang) from e
    elif source == TranscriptSource.AUTO:
        try:
            transcript = transcript_list.find_generated_transcript([lang])
        except NoTranscriptFound as e:
            raise YouTubeTranscriptUnavailableError(video_id=video_id, lang=lang) from e
    else:
        # NOT_AVAILABLE or unexpected source type
        raise YouTubeTranscriptUnavailableError(video_id=video_id, lang=lang)

    try:
        fetched_transcript = await asyncio.to_thread(transcript.fetch)
    except (IpBlocked, RequestBlocked) as e:
        raise YouTubeTranscriptError(
            message="YouTube blocked transcript fetch request.",
            video_id=video_id,
            lang=lang,
        ) from e
    except YouTubeRequestFailed as e:
        raise YouTubeTranscriptError(
            message="YouTube request failed during transcript fetch.",
            video_id=video_id,
            lang=lang,
        ) from e
    except RequestException as e:
        raise YouTubeTranscriptError(
            message="Network error fetching transcript content.",
            video_id=video_id,
            lang=lang,
        ) from e

    vtt_content: str = WebVTTFormatter().format_transcript(fetched_transcript)  # pyright: ignore[reportUnknownMemberType]
    return vtt_content


async def _write_vtt_file(output_path: Path, content: str) -> None:
    """Write VTT content to file, creating parent directories if needed.

    Args:
        output_path: Path to write the VTT file.
        content: VTT-formatted content string.
    """
    await aiofiles.os.makedirs(output_path.parent, exist_ok=True)
    async with aiofiles.open(output_path, "w", encoding="utf-8") as f:
        await f.write(content)


async def download_transcript(
    video_id: str,
    lang: str,
    source: TranscriptSource,
    output_path: Path,
    cookies_path: Path | None = None,
) -> bool:
    """Download a YouTube transcript and write it as a VTT file.

    Fetches the transcript from YouTube's transcript API and formats it
    as WebVTT. This produces clean, non-overlapping cues unlike yt-dlp's
    subtitle download which may contain overlapping karaoke-style cues.

    Args:
        video_id: The YouTube video ID (not the full URL).
        lang: Language code for the transcript (e.g., "en").
        source: Source type (CREATOR for manual subtitles, AUTO for auto-generated).
        output_path: Full path where the VTT file should be written.
        cookies_path: Path to cookies.txt file for authentication, or None.

    Returns:
        True if transcript was downloaded and written successfully, False if
        transcript is not available for the video.

    Raises:
        YouTubeTranscriptError: When YouTube blocks the request (IP block,
            request block) or network errors occur.
    """
    log_params = {
        "video_id": video_id,
        "lang": lang,
        "source": str(source),
        "output_path": str(output_path),
    }

    logger.debug("Fetching YouTube transcript via transcript API.", extra=log_params)

    try:
        vtt_content = await _fetch_and_format_transcript(
            video_id, lang, source, cookies_path
        )
    except YouTubeTranscriptUnavailableError as e:
        logger.warning(
            "Transcript not available for video.", extra=log_params, exc_info=e
        )
        return False

    await _write_vtt_file(output_path, vtt_content)

    logger.debug("YouTube transcript downloaded successfully.", extra=log_params)
    return True
