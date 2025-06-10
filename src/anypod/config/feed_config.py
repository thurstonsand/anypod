"""Feed configuration models for Anypod.

This module provides configuration models for podcast feeds, including
metadata definitions, category validation, and feed-specific settings
that control how content is fetched and processed.
"""

from collections.abc import Sequence
from datetime import datetime
from enum import Enum
import html
import shlex
from typing import Any, ClassVar, cast

from pydantic import BaseModel, Field, GetCoreSchemaHandler, field_validator
from pydantic_core import CoreSchema, core_schema

from ..ytdlp_wrapper.ytdlp_core import YtdlpCore


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


class PodcastCategory:
    """Encapsulates an Apple Podcasts category or subcategory.

    On init, validates and canonicalizes the input.
    repr(instance) → the exact Apple-approved name.

    See https://podcasters.apple.com/support/1691-apple-podcasts-categories
    """

    # Apple's exact supported category & subcategory names
    HIERARCHY: ClassVar[dict[str, set[str]]] = {
        "Arts": {
            "Books",
            "Design",
            "Fashion & Beauty",
            "Food",
            "Performing Arts",
            "Visual Arts",
        },
        "Business": {
            "Careers",
            "Entrepreneurship",
            "Investing",
            "Management",
            "Marketing",
            "Non-Profit",
        },
        "Comedy": {"Comedy Interviews", "Improv", "Stand-Up"},
        "Education": {"Courses", "How To", "Language Learning", "Self-Improvement"},
        "Fiction": {"Comedy Fiction", "Drama", "Science Fiction"},
        "Government": set(),
        "History": set(),
        "Health & Fitness": {
            "Alternative Health",
            "Fitness",
            "Medicine",
            "Mental Health",
            "Nutrition",
            "Sexuality",
        },
        "Kids & Family": {
            "Education for Kids",
            "Parenting",
            "Pets & Animals",
            "Stories for Kids",
        },
        "Leisure": {
            "Animation & Manga",
            "Automotive",
            "Aviation",
            "Crafts",
            "Games",
            "Hobbies",
            "Home & Garden",
            "Video Games",
        },
        "Music": {"Music Commentary", "Music History", "Music Interviews"},
        "News": {
            "Business News",
            "Daily News",
            "Entertainment News",
            "News Commentary",
            "Politics",
            "Sports News",
            "Tech News",
        },
        "Religion & Spirituality": {
            "Buddhism",
            "Christianity",
            "Hinduism",
            "Islam",
            "Judaism",
            "Religion",
            "Spirituality",
        },
        "Science": {
            "Astronomy",
            "Chemistry",
            "Earth Sciences",
            "Life Sciences",
            "Mathematics",
            "Natural Sciences",
            "Nature",
            "Physics",
            "Social Sciences",
        },
        "Society & Culture": {
            "Documentary",
            "Personal Journals",
            "Philosophy",
            "Places & Travel",
            "Relationships",
        },
        "Sports": {
            "Baseball",
            "Basketball",
            "Cricket",
            "Fantasy Sports",
            "Football",
            "Golf",
            "Hockey",
            "Rugby",
            "Running",
            "Soccer",
            "Swimming",
            "Tennis",
            "Volleyball",
            "Wilderness",
            "Wrestling",
        },
        "Technology": set(),
        "True Crime": set(),
        "TV & Film": {
            "After Shows",
            "Film History",
            "Film Interviews",
            "Film Reviews",
            "TV Reviews",
        },
    }

    MAINS: ClassVar[set[str]] = set(HIERARCHY.keys())
    SUBS: ClassVar[set[str]] = {sub for subs in HIERARCHY.values() for sub in subs}

    # Build a lookup: normalized_key -> canonical name
    _CANONICAL_MAIN: ClassVar[dict[str, str]] = {
        " ".join(html.unescape(cat).strip().split()).lower(): cat for cat in MAINS
    }
    _CANONICAL_SUB: ClassVar[dict[str, str]] = {
        " ".join(html.unescape(cat).strip().split()).lower(): cat for cat in SUBS
    }

    @staticmethod
    def _normalize(name: str) -> str:
        """Unescape HTML entities, collapse whitespace, and lowercase."""
        unescaped = html.unescape(name)
        collapsed = " ".join(unescaped.strip().split())
        return collapsed.lower()

    def __init__(self, main: str, sub: str | None = None):
        main_canonical = self._CANONICAL_MAIN.get(self._normalize(main))
        sub_canonical = self._CANONICAL_SUB.get(self._normalize(sub)) if sub else None

        invalid_category_err_msg = (
            f"Invalid Apple Podcasts category: {main!r}, {sub!r}. See "
            "https://podcasters.apple.com/support/1691-apple-podcasts-categories "
            "for a list of valid categories."
        )

        match (main_canonical, sub_canonical):
            # main must be valid
            case (None, _):
                raise ValueError(invalid_category_err_msg)
            # sub is optional
            case (main_canonical, None) if sub is None:
                self.main = main_canonical
                self.sub = None
            # sub must be valid if defined
            case (main_canonical, None):
                raise ValueError(invalid_category_err_msg)
            # both are valid
            case (main_canonical, sub_canonical):
                self.main = main_canonical
                self.sub = sub_canonical

    def asdict(self) -> dict[str, str]:
        """Convert category to dictionary representation.

        Returns:
            Dictionary with 'cat' key and optionally 'sub' key if subcategory exists,
            formatted for feedgen RSS generation.
        """
        return {"cat": self.main, "sub": self.sub} if self.sub else {"cat": self.main}

    def __repr__(self) -> str:
        """The exact Apple-approved string(s)."""
        return (self.main, self.sub).__repr__() if self.sub else self.main.__repr__()

    def __str__(self) -> str:
        """The exact Apple-approved string(s), with hierarchy."""
        return f"{self.main} > {self.sub}" if self.sub else self.main

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Generate core schema for PodcastCategory validation."""

        def validate_from_str(value: str) -> "PodcastCategory":
            """Validate and create PodcastCategory from string."""
            if ">" in value:
                parts = [part.strip() for part in value.split(">", 1)]
                return cls(parts[0], parts[1])
            else:
                return cls(value)

        def validate_from_dict(value: dict[str, Any]) -> "PodcastCategory":
            """Validate and create PodcastCategory from dict."""
            if set(value.keys()).issubset({"main", "sub"}):
                return cls(value["main"], value.get("sub"))
            else:
                raise ValueError(
                    f"Expected only 'main' and 'sub' keys, got {value.keys()!r}"
                )

        return core_schema.union_schema(
            [
                # Accept existing PodcastCategory instances
                core_schema.is_instance_schema(cls),
                # Accept strings (with ">" for main > sub format)
                core_schema.no_info_after_validator_function(
                    validate_from_str, core_schema.str_schema()
                ),
                # Accept dicts with main/sub keys
                core_schema.no_info_after_validator_function(
                    validate_from_dict, core_schema.dict_schema()
                ),
            ]
        )


class FeedMetadataOverrides(BaseModel):
    """Podcast metadata overrides for RSS feed generation.

    These values can be specified in the feed configuration to override
    metadata that would otherwise be extracted from the yt-dlp output.
    If not specified here, the system will attempt to extract these
    values from the source content where possible (e.g., channel name
    as title, channel description as description).

    All fields are optional to allow selective overrides.
    """

    title: str | None = Field(None, description="Podcast title")
    subtitle: str | None = Field(None, description="Podcast subtitle")
    description: str | None = Field(None, description="Podcast description")
    language: str | None = Field(
        None, description="Podcast language (e.g., 'en', 'es')"
    )
    category: list[PodcastCategory] = Field(
        default_factory=list[PodcastCategory],
        description="Apple Podcasts category/categories (max 2)",
    )
    explicit: PodcastExplicit | None = Field(None, description="Explicit content flag")
    image_url: str | None = Field(
        None,
        description="Podcast image URL, must be at least 1400x1400px, ideally 3000x3000px",
    )
    author: str | None = Field(None, description="Podcast author")

    @field_validator("category", mode="before")
    @classmethod
    def parse_category(cls, v: Any) -> list[PodcastCategory]:
        """Accept a PodcastCategory, a string, a dict of 'main' and/or 'sub', or a list of any of the above (at most 2), and return a validated PodcastCategory list.

        Raises:
            ValueError: if the string is not a valid Apple Podcasts category, or if
                the dict has neither 'main' nor 'sub' keys, or if the list has more
                than 2 elements.
            TypeError: if v is not a str, PodcastCategory, dict, or list.
        """
        match v:
            case PodcastCategory():
                return [v]

            case str() if ">" in v:
                p, s = [part.strip() for part in v.split(">", 1)]
                return [PodcastCategory(p, s)]
            case str():
                return [PodcastCategory(v)]
            case dict():
                # expecting either {"main": ...} or {"main": ..., "sub": ...}
                d = cast(dict[str, str], v)
                if "main" in d:
                    return [PodcastCategory(d["main"], d.get("sub"))]
                elif "sub" in d:
                    return [PodcastCategory(d["sub"])]
                else:
                    raise ValueError(
                        f"Expected 'main' and/or 'sub' keys, got {v.keys()!r}"
                    )
            case ls if (
                isinstance(ls, Sequence) and len(ls := cast(Sequence[Any], ls)) <= 2
            ):
                return [cat for any_cat in ls for cat in cls.parse_category(any_cat)]
            case ls if isinstance(ls, Sequence):
                raise ValueError(
                    f"There cannot be more than 2 categories in a list; got {len(cast(Sequence[Any], ls))}"
                )
            case _:
                raise TypeError(
                    "category must be a str, PodcastCategory, dict of 'main' and/or "
                    "'sub', or a list of any of the above (at most 2); got "
                    f"{type(v).__name__}"
                )

    @field_validator("explicit", mode="before")
    @classmethod
    def parse_explicit(cls, v: Any) -> PodcastExplicit | None:
        """Parse and validate explicit content field.

        Args:
            v: Value to parse (can be None, string, or PodcastExplicit).

        Returns:
            PodcastExplicit enum value or None.

        Raises:
            TypeError: If value is not None or string.
        """
        match v:
            case None:
                return None
            case str():
                return PodcastExplicit.from_str(v)
            case _:
                raise TypeError(f"explicit must be a str, got {type(v).__name__}")


class FeedConfig(BaseModel):
    """Configuration for a single podcast feed.

    Attributes:
        url: Feed source URL (e.g., YouTube channel, playlist).
        yt_args: Parsed arguments for yt-dlp from user-provided string.
        schedule: Cron schedule string for feed processing.
        keep_last: Number of latest downloads to keep (prune policy).
        since: Only download newer downloads since this ISO8601 timestamp (prune policy).
        max_errors: Max attempts for downloading before marking as ERROR.
        metadata: Podcast metadata overrides for RSS feed generation.
                  Any values not specified here will be extracted from
                  the source content where possible.
    """

    enabled: bool = Field(
        default=True,
        description="Whether the feed is enabled. If disabled, the feed will not be processed.",
    )
    url: str = Field(..., min_length=1, description="Feed source URL")
    yt_args: dict[str, Any] = Field(
        default_factory=dict[str, Any],
        description="Parsed arguments for yt-dlp, from user-provided string in config.",
    )
    schedule: str = Field(..., min_length=1, description="Cron schedule string")
    keep_last: int | None = Field(
        None, ge=1, description="Prune policy - number of latest downloads to keep"
    )
    since: datetime | None = Field(
        None, description="ISO8601 timestamp to ignore older downloads"
    )
    max_errors: int = Field(
        default=3,
        ge=1,
        description="Max attempts for downloading media before marking as ERROR.",
    )
    metadata: FeedMetadataOverrides | None = Field(
        None, description="Podcast metadata overrides"
    )

    @field_validator("yt_args", mode="before")
    @classmethod
    def parse_yt_args_string(cls, v: Any) -> dict[str, Any]:
        """Parse yt_args string into a dictionary of yt-dlp options.

        Args:
            v: Value to parse, can be string or None.

        Returns:
            Dictionary of parsed yt-dlp options.

        Raises:
            ValueError: If the string cannot be parsed.
            TypeError: If the value is not a string or None.
        """
        match v:
            case None:
                return {}
            case str() if not v.strip():  # Handle empty string
                return {}
            case str():
                try:
                    args_list = shlex.split(v)
                    parsed_opts = YtdlpCore.parse_options(args_list)
                    return parsed_opts
                except Exception as e:
                    raise ValueError(
                        f"Invalid yt_args string '{v}'. Failed to parse."
                    ) from e
            case _:
                raise TypeError(f"yt_args must be a string, got {type(v).__name__}")
