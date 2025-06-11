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

    YES = "yes"
    NO = "no"
    CLEAN = "clean"

    @classmethod
    def from_str(cls, s: str) -> "PodcastExplicit":
        """Create PodcastExplicit from string value.

        Args:
            s: String value to convert (case-insensitive).

        Returns:
            PodcastExplicit enum member.

        Raises:
            ValueError: If string is not a valid explicit value.
        """
        try:
            return cls(s.lower())
        except ValueError as e:
            raise ValueError(f"'{s}' is not one of {[m.value for m in cls]}") from e

    def __str__(self) -> str:
        return self.value

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Generate core schema for PodcastExplicit validation."""
        # Allow validation from string values (converted to PodcastExplicit instances)
        # and existing PodcastExplicit instances
        return core_schema.union_schema(
            [
                core_schema.is_instance_schema(cls),
                core_schema.no_info_after_validator_function(
                    cls.from_str, core_schema.str_schema()
                ),
            ]
        )
