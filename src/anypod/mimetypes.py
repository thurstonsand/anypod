"""Centralized MIME type handling for Anypod.

This module configures and re-exports mimetypes functionality with
podcast-specific MIME type mappings to ensure consistent behavior
across platforms, particularly for macOS.
"""

import mimetypes

# Add podcast-specific MIME type mappings to fix platform inconsistencies
# .m4a returns audio/mp4a-latm natively, but podcast apps expect audio/mp4
# .flac returns audio/x-flac natively, but audio/flac is the standard
# .srt returns text/plain on Linux, but application/x-subrip is expected
# .vtt this is defensive for now -- it seems to return text/vtt appropriately
mimetypes.add_type("audio/mp4", ".m4a")
mimetypes.add_type("audio/flac", ".flac")
mimetypes.add_type("application/x-subrip", ".srt")
mimetypes.add_type("text/vtt", ".vtt")
mimetypes.add_type("video/x-matroska", ".mkv")

# Re-export mimetypes module for use throughout the application
# This ensures the custom mappings are applied everywhere
__all__ = ["mimetypes"]
