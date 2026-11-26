# pyright: reportPrivateUsage=false

"""Unit tests for HandlerSelector routing and GenericHandler best-effort extraction."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from anypod.db.types import DownloadStatus, SourceType
from anypod.ffprobe import FFProbe
from anypod.ytdlp_wrapper.core import YtdlpArgs, YtdlpCore, YtdlpInfo, YtdlpRunResult
from anypod.ytdlp_wrapper.handlers import (
    GenericHandler,
    HandlerSelector,
    PatreonHandler,
    TwitterHandler,
    YoutubeHandler,
    YtdlpGenericFilteredOutError,
)


@pytest.mark.parametrize(
    ("url", "expected_cls"),
    [
        ("https://www.youtube.com/@channel/videos", YoutubeHandler),
        ("https://youtu.be/dQw4w9WgXcQ", YoutubeHandler),
        ("https://music.youtube.com/playlist?list=PL123", YoutubeHandler),
        ("https://www.youtube-nocookie.com/embed/abc", YoutubeHandler),
        ("https://www.patreon.com/creator", PatreonHandler),
        ("https://x.com/user/status/123", TwitterHandler),
        ("https://twitter.com/user/status/123", TwitterHandler),
        ("https://vimeo.com/123456", GenericHandler),
        ("https://soundcloud.com/artist/track", GenericHandler),
        ("https://www.twitch.tv/videos/98765", GenericHandler),
    ],
)
def test_handler_selector_routes_known_and_generic_hosts(
    url: str, expected_cls: type
) -> None:
    """Known platforms get first-class handlers; other yt-dlp URLs get GenericHandler."""
    selector = HandlerSelector(MagicMock(spec=FFProbe))
    handler = selector.select(url)
    assert isinstance(handler, expected_cls)


@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info", new_callable=AsyncMock)
async def test_generic_handler_determine_fetch_strategy(
    mock_extract: AsyncMock,
) -> None:
    """GenericHandler classifies playlists vs single videos."""
    handler = GenericHandler()
    mock_extract.return_value = YtdlpRunResult(
        YtdlpInfo(
            {
                "id": "album1",
                "title": "Album",
                "_type": "playlist",
                "webpage_url": "https://vimeo.com/showcase/album1",
            }
        ),
        None,
    )
    url, source_type = await handler.determine_fetch_strategy(
        "vimeo_feed", "https://vimeo.com/showcase/album1", YtdlpArgs()
    )
    assert url == "https://vimeo.com/showcase/album1"
    assert source_type == SourceType.PLAYLIST


@pytest.mark.asyncio
async def test_generic_handler_extract_download_metadata_with_fallbacks() -> None:
    """GenericHandler fills sensible fallbacks when duration/filesize/publish date are absent."""
    handler = GenericHandler()
    info = YtdlpInfo(
        {
            "id": "track-42",
            "title": "Ambient Track",
            "webpage_url": "https://soundcloud.com/artist/track-42",
            "ext": "mp3",
            "epoch": 1700000000,
        }
    )

    dl = await handler.extract_download_metadata("sc_feed", info)
    assert dl.id == "track-42"
    assert dl.ext == "mp3"
    assert dl.mime_type == "audio/mpeg"
    assert dl.duration == 1
    assert dl.filesize == 1
    assert dl.status == DownloadStatus.QUEUED


@pytest.mark.asyncio
async def test_generic_handler_filters_entry_without_extension() -> None:
    """Entries without downloadable media extension raise YtdlpGenericFilteredOutError."""
    handler = GenericHandler()
    info = YtdlpInfo(
        {
            "id": "no-media",
            "title": "Text post",
            "webpage_url": "https://example.org/post/1",
            "epoch": 1700000000,
        }
    )
    with pytest.raises(YtdlpGenericFilteredOutError):
        await handler.extract_download_metadata("generic_feed", info)
