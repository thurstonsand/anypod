"""Unit tests for ``YtdlpInfo`` utility methods."""

import pytest

from anypod.exceptions import YtdlpFieldInvalidError
from anypod.ytdlp_wrapper.core import YtdlpInfo

# --- Tests for YtdlpInfo.entries ---


@pytest.mark.unit
def test_entries_returns_none_when_no_entries():
    """If ``entries`` field is missing, ``None`` is returned."""
    info = YtdlpInfo({})
    assert info.entries() is None


@pytest.mark.unit
def test_entries_handles_none_entries_and_dicts():
    """``entries`` should return ``YtdlpInfo`` instances while preserving ``None``."""
    info_dict = {
        "entries": [
            {"id": "1"},
            None,
            {"id": "2"},
        ]
    }
    info = YtdlpInfo(info_dict)
    entries = info.entries()
    assert entries is not None
    assert len(entries) == 3
    assert isinstance(entries[0], YtdlpInfo)
    assert entries[1] is None
    assert isinstance(entries[2], YtdlpInfo)


@pytest.mark.unit
def test_entries_invalid_entry_type_raises():
    """Non-dict, non-``None`` entries should raise ``YtdlpFieldInvalidError``."""
    info_dict = {"entries": ["bad"]}
    info = YtdlpInfo(info_dict)
    with pytest.raises(YtdlpFieldInvalidError):
        info.entries()
