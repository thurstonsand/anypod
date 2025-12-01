"""Input validation utilities for FastAPI endpoints."""

from typing import Annotated

from fastapi import HTTPException, Path
from pydantic.functional_validators import AfterValidator

SAFE_FEED_ID_PATTERN = r"^[a-zA-Z0-9_-]{1,255}$"

SAFE_FILENAME_PATTERN = r"^[a-zA-Z0-9_.-]{1,255}$"

SAFE_EXTENSION_PATTERN = r"^[a-zA-Z0-9]{1,10}$"


def _validate_no_path_traversal(value: str) -> str:
    """Validate that value is not a path traversal attempt."""
    if value in (".", ".."):
        raise HTTPException(status_code=400, detail="Invalid path parameter")
    return value


# Validated types that include both regex and security checks
ValidatedFeedId = Annotated[
    str,
    Path(
        description="Feed identifier",
        pattern=SAFE_FEED_ID_PATTERN,
        min_length=1,
        max_length=255,
    ),
    AfterValidator(_validate_no_path_traversal),
]

ValidatedFilename = Annotated[
    str,
    Path(
        description="Filename without extension",
        pattern=SAFE_FILENAME_PATTERN,
        min_length=1,
        max_length=255,
    ),
    AfterValidator(_validate_no_path_traversal),
]

ValidatedExtension = Annotated[
    str,
    Path(
        description="File extension",
        pattern=SAFE_EXTENSION_PATTERN,
        min_length=1,
        max_length=10,
    ),
]
