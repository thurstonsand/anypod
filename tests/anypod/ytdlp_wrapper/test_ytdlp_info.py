"""Unit tests for ``YtdlpInfo`` utility methods."""

import pytest

from anypod.db.types import TranscriptSource
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


# --- Tests for YtdlpInfo.transcript ---


@pytest.mark.unit
def test_transcript_returns_none_when_no_subtitles():
    """Returns ``None`` when neither subtitles nor automatic_captions exist."""
    info = YtdlpInfo({})
    result = info.transcript("en", [TranscriptSource.CREATOR, TranscriptSource.AUTO])
    assert result is None


@pytest.mark.unit
def test_transcript_returns_creator_when_prioritized_first():
    """Returns creator subtitles when prioritized first and both sources available."""
    creator_data = [{"ext": "vtt", "url": "creator.vtt"}]
    auto_data = [{"ext": "vtt", "url": "auto.vtt"}]
    info = YtdlpInfo(
        {
            "subtitles": {"en": creator_data},
            "automatic_captions": {"en": auto_data},
        }
    )

    result = info.transcript("en", [TranscriptSource.CREATOR, TranscriptSource.AUTO])

    assert result is not None
    assert result.source == TranscriptSource.CREATOR


@pytest.mark.unit
def test_transcript_returns_auto_when_prioritized_first():
    """Returns auto captions when prioritized first and both sources available."""
    creator_data = [{"ext": "vtt", "url": "creator.vtt"}]
    auto_data = [{"ext": "vtt", "url": "auto.vtt"}]
    info = YtdlpInfo(
        {
            "subtitles": {"en": creator_data},
            "automatic_captions": {"en": auto_data},
        }
    )

    result = info.transcript("en", [TranscriptSource.AUTO, TranscriptSource.CREATOR])

    assert result is not None
    assert result.source == TranscriptSource.AUTO


@pytest.mark.unit
def test_transcript_falls_back_to_second_priority():
    """Falls back to second priority source when first is unavailable."""
    auto_data = [{"ext": "vtt", "url": "auto.vtt"}]
    info = YtdlpInfo(
        {
            "automatic_captions": {"en": auto_data},
        }
    )

    result = info.transcript("en", [TranscriptSource.CREATOR, TranscriptSource.AUTO])

    assert result is not None
    assert result.source == TranscriptSource.AUTO


@pytest.mark.unit
def test_transcript_returns_none_when_language_missing():
    """Returns ``None`` when requested language not in any source."""
    info = YtdlpInfo(
        {
            "subtitles": {"fr": [{"ext": "vtt", "url": "fr.vtt"}]},
            "automatic_captions": {"de": [{"ext": "vtt", "url": "de.vtt"}]},
        }
    )

    result = info.transcript("en", [TranscriptSource.CREATOR, TranscriptSource.AUTO])

    assert result is None


@pytest.mark.unit
def test_transcript_with_single_source_priority():
    """Works correctly with a single-source priority list."""
    auto_data = [{"ext": "vtt", "url": "auto.vtt"}]
    info = YtdlpInfo(
        {
            "subtitles": {"en": [{"ext": "vtt", "url": "creator.vtt"}]},
            "automatic_captions": {"en": auto_data},
        }
    )

    result = info.transcript("en", [TranscriptSource.AUTO])

    assert result is not None
    assert result.source == TranscriptSource.AUTO
