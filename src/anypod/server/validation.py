"""Input validation utilities for FastAPI endpoints."""

from typing import Annotated

from fastapi import Depends, HTTPException, Path

SAFE_FEED_ID_PATTERN = r"^[a-zA-Z0-9_-]{1,255}$"

SAFE_FILENAME_PATTERN = r"^[a-zA-Z0-9_.-]{1,255}$"

SAFE_EXTENSION_PATTERN = r"^[a-zA-Z0-9]{1,10}$"


# Security validation dependencies
def validate_feed_id(
    feed_id: Annotated[
        str,
        Path(
            description="Feed identifier",
            pattern=SAFE_FEED_ID_PATTERN,
            min_length=1,
            max_length=255,
        ),
    ],
) -> str:
    """Additional security validation for feed IDs beyond regex pattern."""
    if feed_id in (".", ".."):
        raise HTTPException(status_code=400, detail="Invalid feed ID")
    return feed_id


def validate_filename(
    filename: Annotated[
        str,
        Path(
            description="Filename without extension",
            pattern=SAFE_FILENAME_PATTERN,
            min_length=1,
            max_length=255,
        ),
    ],
) -> str:
    """Additional security validation for filenames beyond regex pattern."""
    if filename in (".", ".."):
        raise HTTPException(status_code=400, detail="Invalid filename")
    return filename


def validate_extension(
    ext: Annotated[
        str,
        Path(
            description="File extension",
            pattern=SAFE_EXTENSION_PATTERN,
            min_length=1,
            max_length=10,
        ),
    ],
) -> str:
    """Basic validation for file extensions (no additional security checks needed)."""
    return ext


# Validated types that include both regex and security checks
ValidatedFeedId = Annotated[str, Depends(validate_feed_id)]
ValidatedFilename = Annotated[str, Depends(validate_filename)]
ValidatedExtension = Annotated[str, Depends(validate_extension)]
