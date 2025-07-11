"""Centralized MIME type handling for Anypod.

This module configures and re-exports mimetypes functionality with
podcast-specific MIME type mappings to ensure consistent behavior
across platforms, particularly for macOS.
"""

import mimetypes

# Add podcast-specific MIME type mappings to fix platform inconsistencies
mimetypes.add_type("audio/mp4", ".m4a")
mimetypes.add_type("audio/flac", ".flac")

# Re-export mimetypes module for use throughout the application
# This ensures the custom mappings are applied everywhere
__all__ = ["mimetypes"]
