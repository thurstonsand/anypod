# pyright: reportPrivateUsage=false

"""Unit tests for FFProbe helper functions."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from anypod.exceptions import FFProbeError
from anypod.ffprobe import FFProbe


@pytest.mark.unit
@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
async def test_ffprobe_is_jpg_file_success(mock_cse: AsyncMock, tmp_path: Path) -> None:
    """is_jpg_file returns True when codec is mjpeg."""
    probe = FFProbe()
    fake_json = b'{"streams":[{"codec_name":"mjpeg"}]}'

    mock_proc = AsyncMock()
    mock_proc.returncode = 0
    mock_proc.communicate.return_value = (fake_json, b"")
    mock_cse.return_value = mock_proc

    assert await probe.is_jpg_file(tmp_path / "file") is True


@pytest.mark.unit
@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
async def test_ffprobe_is_jpg_file_failure(mock_cse: AsyncMock, tmp_path: Path) -> None:
    """is_jpg_file raises FFProbeError when ffprobe fails."""
    probe = FFProbe()
    mock_proc = AsyncMock()
    mock_proc.returncode = 1
    mock_proc.communicate.return_value = (b"", b"err")
    mock_cse.return_value = mock_proc

    with pytest.raises(FFProbeError):
        await probe.is_jpg_file(tmp_path / "file")


@pytest.mark.unit
@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
async def test_ffprobe_duration_from_url_success(mock_cse: AsyncMock) -> None:
    """get_duration_seconds_from_url returns int on success."""
    probe = FFProbe()
    mock_proc = AsyncMock()
    mock_proc.returncode = 0
    mock_proc.communicate.return_value = (b"123.9", b"")
    mock_cse.return_value = mock_proc

    assert await probe.get_duration_seconds_from_url("http://x") == 123


@pytest.mark.unit
@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
async def test_ffprobe_duration_from_url_error(mock_cse: AsyncMock) -> None:
    """get_duration_seconds_from_url raises FFProbeError on failure."""
    probe = FFProbe()
    mock_proc = AsyncMock()
    mock_proc.returncode = 1
    mock_proc.communicate.return_value = (b"", b"bad")
    mock_cse.return_value = mock_proc

    with pytest.raises(FFProbeError):
        await probe.get_duration_seconds_from_url("http://x")
