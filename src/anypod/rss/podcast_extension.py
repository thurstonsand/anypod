"""Custom podcast extensions for feedgen with Podcasting 2.0 support.

This module provides custom podcast extensions for feedgen, including:
- Modern iTunes explicit values ("true"/"false")
- Podcasting 2.0 transcript support via <podcast:transcript> tag
"""

from typing import Any

from feedgen.ext.podcast import PodcastExtension as BasePodcastExtension  # type: ignore
from feedgen.ext.podcast_entry import (  # pyright: ignore[reportMissingTypeStubs]
    PodcastEntryExtension as BasePodcastEntryExtension,  # type: ignore  # pyright: ignore[reportUnknownVariableType]
)
from feedgen.util import xml_elem  # type: ignore
from lxml.etree import SubElement  # type: ignore

ITUNES_NS = "http://www.itunes.com/dtds/podcast-1.0.dtd"
PODCAST_NS = "https://podcastindex.org/namespace/1.0"


class Podcast(BasePodcastExtension):  # type: ignore[misc]
    """Custom podcast extension that supports modern iTunes explicit values.

    feedgen's built-in PodcastExtension only supports "yes", "no", and "clean"
    for itunes:explicit. Modern specs (Apple Podcasts, W3C Validator) require
    "true" or "false". This subclass overrides the behavior to support these values.

    Named 'Podcast' so it registers as 'podcast' extension in feedgen.
    """

    def __init__(self) -> None:
        super().__init__()  # type: ignore[reportUnknownMemberType]
        self._explicit: str | None = None

    def itunes_explicit(self, itunes_explicit: str | None = None) -> str | None:
        """Get or set the itunes:explicit value.

        Args:
            itunes_explicit: Either "true" or "false".

        Returns:
            The current explicit value.

        Raises:
            ValueError: If an invalid value is provided.
        """
        if itunes_explicit is not None:
            if itunes_explicit not in ("true", "false"):
                raise ValueError(
                    f"Invalid value for explicit tag: {itunes_explicit!r}. "
                    "Must be 'true' or 'false'."
                )
            self._explicit = itunes_explicit

        return self._explicit

    def extend_ns(self) -> dict[str, str]:
        """Extend the namespace dictionary with the podcast namespace.

        Returns:
            Dictionary of namespace prefixes to URIs.
        """
        ns = super().extend_ns() or {}  # type: ignore[reportUnknownMemberType]
        ns["podcast"] = PODCAST_NS
        return ns  # type: ignore[reportReturnType]

    def extend_rss(self, rss_feed: Any) -> Any:
        """Extend RSS feed with podcast elements.

        The base class only handles 'yes'/'no'/'clean' for itunes:explicit.
        When we pass 'true'/'false', the base class condition
        `if self.__itunes_explicit in ('yes', 'no', 'clean')` fails,
        so it doesn't add the explicit tag. We add it ourselves here.

        Args:
            rss_feed: The RSS feed element to extend.

        Returns:
            The extended RSS feed element.
        """
        # Let the base class handle all other iTunes tags
        rss_feed = super().extend_rss(rss_feed)  # type: ignore[reportUnknownMemberType]

        # Add explicit tag if set (base class won't handle true/false)
        if self._explicit is not None:
            channel = rss_feed[0]  # type: ignore[reportUnknownVariableType]

            # Check if already exists (shouldn't, since base class won't add it)
            existing = channel.find(  # type: ignore[reportUnknownVariableType, reportUnknownMemberType]
                f"{{{ITUNES_NS}}}explicit"
            )
            if existing is None:
                explicit_elem = xml_elem(  # type: ignore[reportUnknownVariableType]
                    f"{{{ITUNES_NS}}}explicit", channel
                )
                explicit_elem.text = self._explicit
            else:
                existing.text = self._explicit

        return rss_feed  # type: ignore[reportUnknownVariableType]


class PodcastEntryExtension(BasePodcastEntryExtension):  # type: ignore[misc]
    """Custom podcast entry extension with transcript support.

    Extends the base podcast entry extension to add Podcasting 2.0
    transcript support via the <podcast:transcript> tag.
    """

    def __init__(self) -> None:
        super().__init__()  # type: ignore[reportUnknownMemberType]
        self._transcript_url: str | None = None
        self._transcript_type: str | None = None
        self._transcript_language: str | None = None
        self._transcript_rel: str | None = None

    def transcript(
        self,
        url: str,
        type: str,
        language: str | None = None,
        rel: str | None = None,
    ) -> None:
        """Set the podcast:transcript tag for this entry.

        Args:
            url: Full URL to the transcript file.
            type: MIME type (e.g., "text/vtt", "application/x-subrip").
            language: Optional ISO language code (e.g., "en").
            rel: Optional relationship type ("captions" for timed captions).
        """
        self._transcript_url = url
        self._transcript_type = type
        self._transcript_language = language
        self._transcript_rel = rel

    def extend_rss(self, entry: Any) -> Any:
        """Extend RSS entry with podcast elements including transcript.

        Args:
            entry: The RSS item element to extend.

        Returns:
            The extended RSS entry element.
        """
        # Let the base class handle iTunes podcast tags
        entry = super().extend_rss(entry)  # type: ignore[reportUnknownMemberType]

        # Add podcast:transcript if set
        if self._transcript_url and self._transcript_type:
            nsmap = {None: PODCAST_NS}  # type: ignore[var-annotated]
            transcript_elem = SubElement(  # type: ignore[reportUnknownMemberType]
                entry,
                f"{{{PODCAST_NS}}}transcript",
                nsmap=nsmap,  # type: ignore[arg-type]
            )
            transcript_elem.set("url", self._transcript_url)  # type: ignore[reportUnknownMemberType]
            transcript_elem.set("type", self._transcript_type)  # type: ignore[reportUnknownMemberType]
            if self._transcript_language:
                transcript_elem.set("language", self._transcript_language)  # type: ignore[reportUnknownMemberType]
            if self._transcript_rel:
                transcript_elem.set("rel", self._transcript_rel)  # type: ignore[reportUnknownMemberType]

        return entry  # type: ignore[reportUnknownVariableType]
