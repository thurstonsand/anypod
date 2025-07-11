"""Enumeration of podcast types."""

from enum import Enum


class PodcastType(Enum):
    """Represent the type of podcast for iTunes metadata.

    Indicates how episodes should be presented in podcast players.
    """

    EPISODIC = "EPISODIC"
    SERIAL = "SERIAL"

    def __str__(self) -> str:
        return self.value

    def rss_str(self) -> str:
        """Get the string value for RSS.

        Podcasts expect the value to be lowercase.

        Returns:
            String value formatted for RSS.
        """
        match self:
            case PodcastType.EPISODIC:
                return "episodic"
            case PodcastType.SERIAL:
                return "serial"
