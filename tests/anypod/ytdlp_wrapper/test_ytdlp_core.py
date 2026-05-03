"""Tests for low-level yt-dlp subprocess handling."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from anypod.exceptions import YtdlpApiError
from anypod.ytdlp_wrapper.core import YtdlpArgs, YtdlpCore


@pytest.mark.unit
@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
async def test_extract_downloads_info_raises_on_network_error(
    mock_create_subprocess_exec: AsyncMock,
):
    """Network failures make playlist metadata unreliable and should fail the sync."""
    mock_proc = MagicMock()
    mock_proc.returncode = 1
    mock_proc.communicate = AsyncMock(
        return_value=(
            b"",
            b"ERROR: Unable to download webpage: <urlopen error "
            b"[Errno -3] Temporary failure in name resolution>",
        )
    )
    mock_proc.wait = AsyncMock()
    mock_create_subprocess_exec.return_value = mock_proc

    with pytest.raises(YtdlpApiError, match="network error"):
        await YtdlpCore.extract_downloads_info(
            YtdlpArgs(), "https://youtube.com/playlist?list=test"
        )


@pytest.mark.unit
@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
async def test_extract_downloads_info_ignores_non_network_ytdlp_errors(
    mock_create_subprocess_exec: AsyncMock,
):
    """Non-network yt-dlp errors remain ignored for partial/filtered playlist cases."""
    mock_proc = MagicMock()
    mock_proc.returncode = 101
    mock_proc.communicate = AsyncMock(
        return_value=(
            b"",
            b"ERROR: [youtube] abc123: Video unavailable. This video is private",
        )
    )
    mock_proc.wait = AsyncMock()
    mock_create_subprocess_exec.return_value = mock_proc

    result = await YtdlpCore.extract_downloads_info(
        YtdlpArgs(), "https://youtube.com/playlist?list=test"
    )

    assert result.payload == []
