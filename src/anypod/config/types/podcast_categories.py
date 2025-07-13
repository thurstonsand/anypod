"""Apple Podcast category handling utilities."""

import html
from typing import Any, ClassVar

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema


class PodcastCategories:
    """Encapsulates an Apple Podcasts category/subcategory.

    On init, validates and canonicalizes the input.
    There can be no more than 2 categories, and each category can have at most 1 subcategory.
    repr(instance): the exact Apple-approved name(s).

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

    @classmethod
    def default(cls) -> "PodcastCategories":
        """Return the default category."""
        return cls("TV & Film")

    @staticmethod
    def _validate_single_category(
        main: str, sub: str | None = None
    ) -> tuple[str, str | None]:
        """Validate and canonicalize a single category.

        Returns:
            Tuple of (canonical_main, canonical_sub)

        Raises:
            ValueError: If the category is invalid.
        """
        main_canonical = PodcastCategories._CANONICAL_MAIN.get(
            PodcastCategories._normalize(main)
        )
        sub_canonical = (
            PodcastCategories._CANONICAL_SUB.get(PodcastCategories._normalize(sub))
            if sub
            else None
        )

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
                return main_canonical, None
            # sub must be valid if defined
            case (main_canonical, None):
                raise ValueError(invalid_category_err_msg)
            # both are valid
            case (main_canonical, sub_canonical):
                return main_canonical, sub_canonical

    @staticmethod
    def _parse_str_category(category: str) -> tuple[str, str | None]:
        """Parse a single category string into a tuple of (main, sub).

        Args:
            category: String representation of a category, in the format "main" or "main > sub".

        Returns:
            Tuple of (main, sub).

        Raises:
            ValueError: If the category is invalid.
        """
        return PodcastCategories._validate_single_category(
            *[parts.strip() for parts in category.split(">", 1)]
        )

    def __init__(
        self,
        categories: list[tuple[str, str | None]]
        | list[dict[str, str]]
        | list[str]
        | str
        | None = None,
    ):
        """Initialize with categories in various formats.

        Args:
            categories: Can be:
                - None: Empty categories
                - str: Single category "main" or "main > sub" or comma-separated list of such
                - list[str]: List of category strings
                - list[dict]: List of dicts with 'main' and optional 'sub' keys
                - list[tuple]: List of (main, sub) tuples

        Raises:
            ValueError: If more than 2 categories or invalid categories.
        """
        match categories:
            case None | "" | []:
                raise ValueError(
                    "Empty categories are not allowed. Use None instead of empty PodcastCategories."
                )
            case str() as multi_cat_str if "," in multi_cat_str:
                cat_strs = multi_cat_str.split(",")
                if len(cat_strs) > 2:
                    raise ValueError(
                        f"Maximum 2 categories allowed, got {len(cat_strs)} in {multi_cat_str}"
                    )
                self._categories = {
                    PodcastCategories._parse_str_category(cat_str)
                    for cat_str in cat_strs
                }
            case str() as multi_cat_str:
                self._categories = {
                    PodcastCategories._parse_str_category(multi_cat_str)
                }
            case list() as cats if len(cats) > 2:
                raise ValueError(f"Maximum 2 categories allowed, got {len(cats)}")
            case list() as cats:
                self._categories: set[tuple[str, str | None]] = set()
                for cat in cats:
                    match cat:
                        case str() as cat_str:
                            self._categories.add(
                                PodcastCategories._parse_str_category(cat_str)
                            )
                        # Guarantee that "main" and "sub" are the only valid keys, and that "main" is present
                        case dict() as cat_dict if "main" in cat_dict and all(
                            key in {"main", "sub"} for key in cat_dict
                        ):
                            self._categories.add(
                                PodcastCategories._validate_single_category(
                                    cat_dict["main"], cat_dict.get("sub")
                                )
                            )
                        case tuple() as cat_tuple if len(cat_tuple) == 2:
                            self._categories.add(
                                PodcastCategories._validate_single_category(
                                    cat[0], cat[1]
                                )
                            )
                        case _:
                            raise ValueError(f"Invalid category item: {cat!r}")

        # Ensure we never create empty categories
        if not self._categories:
            raise ValueError(
                "Empty categories are not allowed. Use None instead of empty PodcastCategories."
            )

    def __len__(self) -> int:
        """Return the number of categories."""
        return len(self._categories)

    def __bool__(self) -> bool:
        """Return True if there are any categories."""
        return bool(self._categories)

    def __iter__(self):
        """Iterate over the categories as (main, sub) tuples."""
        return iter(self._categories)

    def __eq__(self, other: object) -> bool:
        """Check equality with another PodcastCategories instance."""
        if not isinstance(other, PodcastCategories):
            return False
        return self._categories == other._categories

    def __str__(self) -> str:
        """String representation for database storage."""
        category_strings: list[str] = []
        for main, sub in self._categories:
            if sub:
                category_strings.append(f"{main} > {sub}")
            else:
                category_strings.append(main)
        return ", ".join(category_strings)

    def itunes_rss_list(self) -> list[dict[str, str]]:
        """Convert to list of dictionaries for iTunes RSS generation.

        Returns:
            List of dictionaries in format expected by feedgen library for iTunes categories.
        """
        return [
            {"cat": main, "sub": sub} if sub else {"cat": main}
            for main, sub in sorted(self._categories)
        ]

    def rss_list(self) -> list[dict[str, str]]:
        """Convert to list of dictionaries for standard RSS generation.

        Returns:
            List of dictionaries in format expected by feedgen library for standard RSS categories.
            Uses only the main category as the 'term' field.
        """
        return [{"term": main} for main, _ in sorted(self._categories)]

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Generate core schema for PodcastCategory validation."""

        def validate_from_str(value: str) -> "PodcastCategories":
            """Validate and create PodcastCategories from string."""
            return cls(value)

        def validate_from_list(
            value: list[Any],
        ) -> "PodcastCategories":
            """Validate and create PodcastCategories from list."""
            if any(not isinstance(item, str | dict | tuple) for item in value):
                raise ValueError(f"Invalid category item: {value!r}")
            return cls(value)

        return core_schema.union_schema(
            [
                # Accept existing PodcastCategories instances
                core_schema.is_instance_schema(cls),
                # Accept strings (with ">" for main > sub format)
                core_schema.no_info_after_validator_function(
                    validate_from_str, core_schema.str_schema()
                ),
                # Accept lists of tuples, dicts, or strings
                core_schema.no_info_after_validator_function(
                    validate_from_list, core_schema.list_schema()
                ),
            ]
        )
