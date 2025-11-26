"""Custom podcast extension for feedgen with modern iTunes explicit values."""

from typing import Any

from feedgen.ext.podcast import PodcastExtension as BasePodcastExtension  # type: ignore
from feedgen.util import xml_elem  # type: ignore

ITUNES_NS = "http://www.itunes.com/dtds/podcast-1.0.dtd"


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
