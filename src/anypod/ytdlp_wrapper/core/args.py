"""Builder for yt-dlp command-line arguments."""

from datetime import datetime
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
        self._convert_thumbnails: str | None = None

        # Path configuration
        self._paths_temp: Path | None = None
        self._paths_home: Path | None = None

        # Authentication
        self._cookies: Path | None = None

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

    def paths_temp(self, path: Path) -> "YtdlpArgs":
        """Set temporary directory path for downloads."""
        self._paths_temp = path
        return self

    def paths_home(self, path: Path) -> "YtdlpArgs":
        """Set final directory path for downloads."""
        self._paths_home = path
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

    def extend_args(self, args: list[str]) -> "YtdlpArgs":
        """Add additional raw arguments to the end."""
        self._additional_args.extend(args)
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
        """Convert arguments to a list of strings for subprocess execution.

        Returns:
            List of CLI arguments ready for yt-dlp subprocess.
        """
        args: list[str] = []

        # Start with user-provided arguments
        args.extend(self._additional_args)

        # Add boolean flags
        # Output control
        if self._quiet:
            args.append("--quiet")
        if self._no_warnings:
            args.append("--no-warnings")
        if self._dump_single_json:
            args.append("--dump-single-json")
        if self._dump_json:
            args.append("--dump-json")

        # Download control
        if self._skip_download:
            args.append("--skip-download")

        # Playlist control
        if self._flat_playlist:
            args.append("--flat-playlist")
        if self._lazy_playlist:
            args.append("--lazy-playlist")

        # Add arguments with values
        # Playlist filtering and control
        if self._playlist_limit is not None:
            args.extend(["--playlist-items", f":{self._playlist_limit}"])
        if self._break_match_filters is not None:
            args.extend(["--break-match-filters", self._break_match_filters])

        # Date filtering
        if self._dateafter is not None:
            args.extend(["--dateafter", self._dateafter.strftime("%Y%m%d")])
        if self._datebefore is not None:
            args.extend(["--datebefore", self._datebefore.strftime("%Y%m%d")])

        # Output configuration
        if self._output is not None:
            args.extend(["--output", self._output])
        if self._convert_thumbnails is not None:
            args.extend(["--convert-thumbnails", self._convert_thumbnails])

        # Path configuration
        if self._paths_temp is not None:
            args.extend(["--paths", f"temp:{self._paths_temp}"])
        if self._paths_home is not None:
            args.extend(["--paths", f"home:{self._paths_home}"])

        # Authentication
        if self._cookies is not None:
            args.extend(["--cookies", str(self._cookies)])

        return args

    def __str__(self) -> str:
        """Build command-line argument string for yt-dlp subprocess.

        Returns:
            Space-separated string of CLI arguments ready for yt-dlp execution.
        """
        return " ".join(self.to_list())
