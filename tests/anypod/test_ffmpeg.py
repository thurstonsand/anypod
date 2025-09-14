# pyright: reportPrivateUsage=false

"""Unit tests for FFmpeg helper."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from anypod.exceptions import FFmpegError
from anypod.ffmpeg import FFmpeg


@pytest.mark.unit
@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
async def test_ffmpeg_convert_image_to_jpg_success(
    mock_cse: AsyncMock, tmp_path: Path
) -> None:
    """convert_image_to_jpg succeeds when ffmpeg returns 0."""
    ffm = FFmpeg()
    mock_proc = AsyncMock()
    mock_proc.returncode = 0
    mock_proc.communicate.return_value = (b"", b"")
    mock_cse.return_value = mock_proc

    await ffm.convert_image_to_jpg(tmp_path / "in.png", tmp_path / "out.jpg")


@pytest.mark.unit
@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
async def test_ffmpeg_convert_image_to_jpg_failure(
    mock_cse: AsyncMock, tmp_path: Path
) -> None:
    """convert_image_to_jpg raises FFmpegError when ffmpeg fails."""
    ffm = FFmpeg()
    mock_proc = AsyncMock()
    mock_proc.returncode = 1
    mock_proc.communicate.return_value = (b"", b"err")
    mock_cse.return_value = mock_proc

    with pytest.raises(FFmpegError):
        await ffm.convert_image_to_jpg(tmp_path / "in.png", tmp_path / "out.jpg")
