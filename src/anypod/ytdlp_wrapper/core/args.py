"""Builder for yt-dlp command-line arguments."""

from datetime import datetime
import os
from pathlib import Path


class YtdlpArgs:
    """Builder for yt-dlp command-line arguments.

    Provides a type-safe builder for constructing yt-dlp CLI arguments.
    User-provided arguments are preserved and prepended to the final argument list.

    Example:
        args = (YtdlpArgs(user_args)
                .quiet()
                .no_warnings()
                .skip_download()
                .playlist_items("1-5"))
    """

    # Cache pytest detection to avoid repeated environment checks
    _running_under_pytest_cached: bool | None = None

    @classmethod
    def _running_under_pytest(cls) -> bool:
        """Return True if executing under pytest.

        Returns:
            True when PYTEST_CURRENT_TEST environment variable is present.
        """
        if cls._running_under_pytest_cached is None:
            cls._running_under_pytest_cached = "PYTEST_CURRENT_TEST" in os.environ
        return cls._running_under_pytest_cached

    def __init__(self, user_args: list[str] | None = None):
        self._additional_args = user_args or []

        # Output control
        self._quiet = False
        self._no_warnings = False
        self._dump_single_json = False
        self._dump_json = False

        # Download control
        self._skip_download = False

        # Playlist control
        self._flat_playlist = False
        self._lazy_playlist = False
        self._playlist_limit: int | None = None
        self._break_match_filters: str | None = None

        # Date filtering
        self._dateafter: datetime | None = None
        self._datebefore: datetime | None = None

        # Output configuration
        self._output: str | None = None
        # Thumbnail-specific output
        self._convert_thumbnails: str | None = None
        self._write_thumbnails: bool = False
        self._thumbnail_output: str | None = None
        self._pl_thumbnail_output: str | None = None

        # Path configuration
        self._paths_temp: Path | None = None
        self._paths_home: Path | None = None
        self._paths_thumbnail: Path | None = None
        self._paths_pl_thumbnail: Path | None = None

        # Authentication
        self._cookies: Path | None = None

        # Update control
        self._update_to: str | None = None

        # Extractor args
        self._extractor_args: list[str] = []

        # Networking / filtering
        self._referer: str | None = None
        self._match_filter: str | None = None

    def quiet(self) -> "YtdlpArgs":
        """Enable quiet mode (suppress verbose output)."""
        self._quiet = True
        return self

    def no_warnings(self) -> "YtdlpArgs":
        """Suppress warning messages."""
        self._no_warnings = True
        return self

    def skip_download(self) -> "YtdlpArgs":
        """Extract metadata only, don't download media."""
        self._skip_download = True
        return self

    def flat_playlist(self) -> "YtdlpArgs":
        """Extract playlist metadata without individual entries."""
        self._flat_playlist = True
        return self

    def playlist_limit(self, limit: int) -> "YtdlpArgs":
        """Limit playlist items (e.g., "1-5")."""
        self._playlist_limit = limit
        return self

    def dateafter(self, date: datetime) -> "YtdlpArgs":
        """Only download videos uploaded on or after this date (day granularity)."""
        self._dateafter = date
        return self

    def datebefore(self, date: datetime) -> "YtdlpArgs":
        """Only download videos uploaded on or before this date (day granularity)."""
        self._datebefore = date
        return self

    def output(self, template: str) -> "YtdlpArgs":
        """Set output filename template."""
        self._output = template
        return self

    def convert_thumbnails(self, format: str) -> "YtdlpArgs":
        """Convert thumbnails to specified format (jpg, png, webp)."""
        self._convert_thumbnails = format
        return self

    def write_thumbnail(self) -> "YtdlpArgs":
        """Enable thumbnail downloading.

        Returns:
            The builder instance for chaining.
        """
        self._write_thumbnails = True
        return self

    def output_thumbnail(self, template: str) -> "YtdlpArgs":
        """Set output template for thumbnail files.

        Args:
            template: The output template string for thumbnails (e.g., "%(id)s.%(ext)s").

        Returns:
            The builder instance for chaining.
        """
        self._thumbnail_output = template
        return self

    def output_pl_thumbnail(self, template: str) -> "YtdlpArgs":
        """Set output template for playlist-level thumbnail files.

        Args:
            template: The output template string for playlist thumbnails (e.g., "%(id)s.%(ext)s").

        Returns:
            The builder instance for chaining.
        """
        self._pl_thumbnail_output = template
        return self

    def paths_temp(self, path: Path) -> "YtdlpArgs":
        """Set temporary directory path for downloads."""
        self._paths_temp = path
        return self

    def paths_home(self, path: Path) -> "YtdlpArgs":
        """Set final directory path for downloads."""
        self._paths_home = path
        return self

    def paths_thumbnail(self, path: Path) -> "YtdlpArgs":
        """Set the directory where thumbnails will be saved.

        Args:
            path: Filesystem path to save thumbnails in.

        Returns:
            The builder instance for chaining.
        """
        self._paths_thumbnail = path
        return self

    def paths_pl_thumbnail(self, path: Path) -> "YtdlpArgs":
        """Set the directory where playlist thumbnails will be saved.

        Args:
            path: Filesystem path to save playlist thumbnails in.

        Returns:
            The builder instance for chaining.
        """
        self._paths_pl_thumbnail = path
        return self

    def cookies(self, path: Path) -> "YtdlpArgs":
        """Set path to cookies file for authentication."""
        self._cookies = path
        return self

    def dump_single_json(self) -> "YtdlpArgs":
        """Output metadata as JSON without downloading."""
        self._dump_single_json = True
        return self

    def dump_json(self) -> "YtdlpArgs":
        """Output metadata as JSON for each video without downloading."""
        self._dump_json = True
        return self

    def lazy_playlist(self) -> "YtdlpArgs":
        """Process playlist entries sequentially, enables early termination."""
        self._lazy_playlist = True
        return self

    def break_match_filters(self, filter_expr: str) -> "YtdlpArgs":
        """Stop processing when filter condition fails.

        Args:
            filter_expr: Filter expression (e.g., "upload_date > 20230101").
        """
        self._break_match_filters = filter_expr
        return self

    def referer(self, url: str) -> "YtdlpArgs":
        """Set the HTTP Referer header for requests.

        Args:
            url: The referer URL to pass to yt-dlp (e.g., "https://www.patreon.com").

        Returns:
            The builder instance for chaining.
        """
        self._referer = url
        return self

    def match_filter(self, filter_expr: str) -> "YtdlpArgs":
        """Include only entries matching the filter expression.

        Args:
            filter_expr: A yt-dlp match filter expression (e.g., "vcodec").

        Returns:
            The builder instance for chaining.
        """
        self._match_filter = filter_expr
        return self

    def update_to(self, channel: str) -> "YtdlpArgs":
        """Update to a specific channel or version.

        Args:
            channel: Channel name (stable, nightly, master), version tag,
                    or repository (owner/repo format).
        """
        self._update_to = channel
        return self

    def extend_args(self, args: list[str]) -> "YtdlpArgs":
        """Add additional raw arguments to the end."""
        self._additional_args.extend(args)
        return self

    def extractor_args(self, value: str) -> "YtdlpArgs":
        """Append an ``--extractor-args`` flag value.

        Args:
            value: Raw extractor-args string (e.g.,
                "youtube:fetch_pot=never" or
                "youtubepot-bgutilhttp:base_url=http://host:4416").

        Returns:
            The builder instance for chaining.
        """
        if value:
            self._extractor_args.append(value)
        return self

    @property
    def additional_args_count(self) -> int:
        """Get the number of user-provided arguments."""
        return len(self._additional_args)

    @property
    def additional_args(self) -> list[str]:
        """Get a copy of the user-provided arguments."""
        return self._additional_args.copy()

    def to_list(self) -> list[str]:
        """Convert arguments to a complete command list for subprocess execution.

        Returns:
            Complete command list including yt-dlp binary and CLI arguments.
        """
        # Start with the yt-dlp command prefix
        cmd = ["uv", "run", "yt-dlp"] if self._running_under_pytest() else ["yt-dlp"]

        # Add user-provided arguments
        cmd.extend(self._additional_args)

        # Output control
        if self._quiet:
            cmd.append("--quiet")
        if self._no_warnings:
            cmd.append("--no-warnings")
        if self._dump_single_json:
            cmd.append("--dump-single-json")
        if self._dump_json:
            cmd.append("--dump-json")

        # Download control
        if self._skip_download:
            cmd.append("--skip-download")

        # Playlist control
        if self._flat_playlist:
            cmd.append("--flat-playlist")
        if self._lazy_playlist:
            cmd.append("--lazy-playlist")
        if self._playlist_limit is not None:
            cmd.extend(["--playlist-items", f":{self._playlist_limit}"])
        if self._break_match_filters is not None:
            cmd.extend(["--break-match-filters", self._break_match_filters])

        # Date filtering
        if self._dateafter is not None:
            cmd.extend(["--dateafter", self._dateafter.strftime("%Y%m%d")])
        if self._datebefore is not None:
            cmd.extend(["--datebefore", self._datebefore.strftime("%Y%m%d")])

        # Output configuration
        if self._output is not None:
            cmd.extend(["--output", self._output])

        # Thumbnail-specific output
        if self._convert_thumbnails is not None:
            cmd.extend(["--convert-thumbnails", self._convert_thumbnails])
        if self._write_thumbnails:
            cmd.append("--write-thumbnail")
        if self._pl_thumbnail_output is not None:
            cmd.extend(["--output", f"pl_thumbnail:{self._pl_thumbnail_output}"])
        if self._thumbnail_output is not None:
            cmd.extend(["--output", f"thumbnail:{self._thumbnail_output}"])

        # Path configuration
        if self._paths_temp is not None:
            cmd.extend(["--paths", f"temp:{self._paths_temp}"])
        if self._paths_home is not None:
            cmd.extend(["--paths", f"home:{self._paths_home}"])
        if self._paths_thumbnail is not None:
            cmd.extend(["--paths", f"thumbnail:{self._paths_thumbnail}"])
        if self._paths_pl_thumbnail is not None:
            cmd.extend(["--paths", f"pl_thumbnail:{self._paths_pl_thumbnail}"])

        # Authentication
        if self._cookies is not None:
            cmd.extend(["--cookies", str(self._cookies)])

        # Extractor args
        for ex_arg in self._extractor_args:
            cmd.extend(["--extractor-args", ex_arg])

        # Networking / filtering
        if self._referer is not None:
            cmd.extend(["--referer", self._referer])
        if self._match_filter is not None:
            cmd.extend(["--match-filter", self._match_filter])

        # Update control - skip in pytest to avoid issues with pip-installed yt-dlp
        if self._update_to is not None and not self._running_under_pytest():
            cmd.extend(["--update-to", self._update_to])

        return cmd

    def __str__(self) -> str:
        """Build command-line argument string for yt-dlp subprocess.

        Returns:
            Space-separated string of CLI arguments ready for yt-dlp execution.
        """
        return " ".join(self.to_list())
