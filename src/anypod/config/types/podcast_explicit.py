"""Podcast explicit content flags."""

from enum import Enum
from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema


class PodcastExplicit(str, Enum):
    """Explicit content flag values for podcasts.

    Represents the allowed values for the explicit content flag
    in podcast RSS feeds according to Apple Podcasts standards.
    """

    YES = "YES"
    NO = "NO"
    CLEAN = "CLEAN"

    @classmethod
    def from_str(cls, s: str) -> PodcastExplicit:
        """Create PodcastExplicit from string value.

        Args:
            s: String value to convert (case-insensitive).

        Returns:
            PodcastExplicit enum member.

        Raises:
            ValueError: If string is not a valid explicit value.
        """
        try:
            return cls(s.upper())
        except ValueError as e:
            raise ValueError(f"'{s}' is not one of {[m.value for m in cls]}") from e

    def __str__(self) -> str:
        return self.value

    def rss_str(self) -> str:
        """Get the string value for RSS.

        Podcasts expect the value to be lowercase.

        Returns:
            String value formatted for RSS.
        """
        match self:
            case PodcastExplicit.YES:
                return "yes"
            case PodcastExplicit.NO:
                return "no"
            case PodcastExplicit.CLEAN:
                return "clean"

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Generate core schema for PodcastExplicit validation."""
        # Allow parsing from str or direct values
        return core_schema.union_schema(
            [
                core_schema.is_instance_schema(cls),
                core_schema.no_info_after_validator_function(
                    cls.from_str, core_schema.str_schema()
                ),
            ]
        )
