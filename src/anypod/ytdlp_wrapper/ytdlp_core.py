from typing import Any

import yt_dlp  # type: ignore
from yt_dlp.utils import ExtractorError, UserNotLive  # type: ignore

from ..exceptions import YtdlpApiError


class YtdlpCore:
    @staticmethod
    def parse_options(user_cli_args: list[str]) -> dict[str, Any]:
        _, _, _, parsed_user_opts = yt_dlp.parse_options(user_cli_args)  # type: ignore
        return parsed_user_opts  # type: ignore

    @staticmethod
    def extract_info(ydl_opts: dict[str, Any], url: str) -> dict[str, Any] | None:
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:  # type: ignore
                return ydl.extract_info(url, download=False)  # type: ignore
        except (ExtractorError, UserNotLive) as e:  # type: ignore
            raise YtdlpApiError(
                message="Failed to extract metadata.",
                url=url,
            ) from e
        except Exception as e:
            raise YtdlpApiError(
                message="Unexpected error occurred while extracting metadata.",
                url=url,
            ) from e

    @staticmethod
    def download(ydl_opts: dict[str, Any], url: str) -> None:
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:  # type: ignore
                ret_code: int = ydl.download([url])  # type: ignore
        except (ExtractorError, UserNotLive) as e:  # type: ignore
            raise YtdlpApiError(
                message="Failed to download media.",
                url=url,
            ) from e
        except Exception as e:
            raise YtdlpApiError(
                message="Unexpected error occurred while downloading media.",
                url=url,
            ) from e
        else:
            if ret_code != 0:
                raise YtdlpApiError(
                    message=f"Download failed with non-zero exit code: {ret_code}, may not exist",
                    url=url,
                )
