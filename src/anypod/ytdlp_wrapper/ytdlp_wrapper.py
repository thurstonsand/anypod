from collections.abc import Callable
import logging
from typing import Any

import yt_dlp
import yt_dlp.options
from yt_dlp.utils import NO_DEFAULT, function_with_repr

from ..db import Download
from ..exceptions import YtdlpApiError
from .base_handler import FetchPurpose, ReferenceType, SourceHandlerBase
from .youtube_handler import YoutubeHandler

logger = logging.getLogger(__name__)


class YtdlpWrapper:
    """
    Wrapper around yt-dlp for fetching and parsing metadata.
    """

    _source_handler: SourceHandlerBase = YoutubeHandler()

    FilterFunctionType = Callable[
        [dict[str, Any], bool | set[str]], (NO_DEFAULT | str) | None
    ]

    # I may not need this function, but leaving it here for now.
    def _compose_match_filters_and(
        self,
        *funcs: FilterFunctionType | None,
        name: str = "composed_filter_and",
    ) -> FilterFunctionType | None:
        """
        Composes multiple filter functions (typically from yt_dlp.utils.match_filter_func)
        with AND logic. The composed filter passes only if all underlying filters pass.

        Args:
            *funcs: The filter functions to compose.
            name: An optional name for the composed filter, used for its representation.

        Returns:
            A new filter function that combines all provided filters with AND logic.
        """
        logger.debug(
            "Composing match filters.",
            extra={"num_funcs_to_compose": len(funcs), "requested_name": name},
        )
        active_funcs = [f for f in funcs if f is not None]
        if not active_funcs:
            logger.debug("No active filter functions to compose.")
            return None
        elif len(active_funcs) == 1:
            logger.debug("Only one active filter function, returning it directly.")
            return active_funcs[0]
        else:
            # Create a meaningful repr for the composed function.
            # repr() on the function_with_repr instances should provide good strings.
            reprs = ", ".join(repr(f) for f in active_funcs)
            composed_repr = f"ytdlp_wrapper.{name}({reprs})"
            logger.debug(
                "Composing multiple filter functions.",
                extra={"composed_repr": composed_repr},
            )

            @function_with_repr.set_repr(composed_repr)
            def combined_filter(
                info_dict: dict[str, Any], incomplete: bool | set[str] = False
            ) -> None | NO_DEFAULT | str:
                """
                The actual combined filter.
                It passes if all underlying filters pass, respecting breaking conditions
                and interactive prompts (NO_DEFAULT).
                """
                results = []
                for f in active_funcs:
                    res = f(info_dict, incomplete)
                    if res not in (None, NO_DEFAULT):
                        return res
                    results.append(res)
                return NO_DEFAULT if any(r is NO_DEFAULT for r in results) else None

            return combined_filter

    def _prepare_ydl_options(
        self,
        user_cli_args: list[str],
        purpose: FetchPurpose,
        source_specific_opts: dict[str, Any],
    ) -> dict[str, Any]:
        logger.debug(
            "Preparing yt-dlp options.",
            extra={
                "purpose": purpose,
                "num_user_cli_args": len(user_cli_args),
                "source_specific_opts": list(source_specific_opts.keys()),
            },
        )
        try:
            _, _, _, parsed_user_opts = yt_dlp.parse_options(user_cli_args)
            logger.debug(
                "Successfully parsed user CLI arguments for yt-dlp options.",
                extra={"parsed_user_opts": list(parsed_user_opts.keys())},
            )
        except Exception as e:
            raise YtdlpApiError(
                message="Invalid yt-dlp CLI arguments provided.",
            ) from e

        base_opts: dict[str, Any] = {
            "logger": logger,
            "skip_download": True,
            "quiet": True,
            "ignoreerrors": True,
            "no_warnings": True,
            "verbose": False,
            **source_specific_opts,
        }
        logger.debug(
            "Initial base_opts prepared.",
            extra={"base_opts_keys": list(base_opts.keys())},
        )

        match_filter = self._compose_match_filters_and(
            parsed_user_opts.pop("match_filter", None),
            base_opts.pop("match_filter", None),
            name="final_match_filter",
        )
        if match_filter:
            base_opts["match_filter"] = match_filter

        match purpose:
            case FetchPurpose.DISCOVERY:
                final_opts = {
                    **base_opts,
                    "extract_flat": "in_playlist",
                    "playlist_items": "1-5",
                }
                logger.debug(
                    "Prepared DISCOVERY options.",
                    extra={"final_opts": list(final_opts.keys())},
                )
            case FetchPurpose.METADATA_FETCH:
                final_opts = {
                    **parsed_user_opts,
                    **base_opts,
                    "extract_flat": False,
                }
                logger.debug(
                    "Prepared METADATA_FETCH options.",
                    extra={"final_opts": list(final_opts.keys())},
                )

        return final_opts

    def _extract_yt_dlp_info_internal(
        self, ydl_opts: dict[str, Any], url: str
    ) -> dict[str, Any] | None:
        try:
            ydl_instance = yt_dlp.YoutubeDL(ydl_opts)
        except Exception as e:
            raise YtdlpApiError(
                message="Failed to instantiate yt_dlp.YoutubeDL to extract metadata.",
                url=url,
            ) from e

        logger.debug(
            "Calling yt-dlp extract_info.",
            extra={"url": url, "ydl_opts": list(ydl_opts.keys())},
        )
        try:
            extracted_info = ydl_instance.extract_info(url, download=False)
            logger.debug(
                "yt-dlp extract_info call successful.",
                extra={
                    "url": url,
                    "info_extracted": extracted_info is not None,
                    "type": type(extracted_info).__name__ if extracted_info else None,
                },
            )
            return extracted_info
        except yt_dlp.utils.DownloadError as e:
            logger.warning(
                "yt-dlp extract_info failed with DownloadError.",
                exc_info=e,
                extra={"url": url},
            )
            raise YtdlpApiError(
                message="yt-dlp failed to process metadata.", url=url
            ) from e
        except Exception as e:
            raise YtdlpApiError(
                message="Unexpected failure during yt_dlp.extract_info.", url=url
            ) from e

    def fetch_metadata(
        self,
        feed_name: str,
        url: str,
        yt_cli_args: list[str],
    ) -> list[Download]:
        logger.info(
            "Fetching metadata for feed.",
            extra={
                "feed_name": feed_name,
                "url": url,
                "num_yt_cli_args": len(yt_cli_args),
            },
        )

        source_specific_discovery_opts = (
            self._source_handler.get_source_specific_ydl_options(FetchPurpose.DISCOVERY)
        )

        # TODO: I'm understanding, but this is not the most clear code
        def discovery_caller(
            handler_discovery_opts: dict[str, Any], url_to_discover: str
        ) -> dict[str, Any] | None:
            logger.debug(
                "Discovery caller invoked by strategy handler.",
                extra={
                    "feed_name": feed_name,
                    "original_url": url,
                    "url_to_discover": url_to_discover,
                    "handler_discovery_opts": list(handler_discovery_opts.keys()),
                },
            )
            effective_discovery_opts = self._prepare_ydl_options(
                user_cli_args=[],  # Pass empty because this is just for discovery
                purpose=FetchPurpose.DISCOVERY,
                source_specific_opts=source_specific_discovery_opts,
            )
            effective_discovery_opts.update(handler_discovery_opts)
            return self._extract_yt_dlp_info_internal(
                effective_discovery_opts, url_to_discover
            )

        fetch_url, ref_type = self._source_handler.determine_fetch_strategy(
            url, discovery_caller
        )
        # Fallback to original if strategy returns None for URL
        actual_fetch_url = fetch_url or url
        if ref_type == ReferenceType.UNKNOWN_DIRECT_FETCH and not fetch_url:
            logger.info(
                "Discovery indicated direct fetch, using original URL.",
                extra={"feed_name": feed_name, "url": url},
            )
        elif not fetch_url:
            raise YtdlpApiError(
                message="Strategy determination returned no fetchable URL. Aborting.",
                feed_name=feed_name,
                url=url,
            )

        logger.info(
            "Acquiring metadata.",
            extra={
                "feed_name": feed_name,
                "actual_fetch_url": actual_fetch_url,
                "original_url": url,
                "reference_type": ref_type.name,
            },
        )

        source_specific_metadata_opts = (
            self._source_handler.get_source_specific_ydl_options(
                FetchPurpose.METADATA_FETCH
            )
        )
        main_fetch_opts = self._prepare_ydl_options(
            yt_cli_args, FetchPurpose.METADATA_FETCH, source_specific_metadata_opts
        )

        raw_info_dict = self._extract_yt_dlp_info_internal(
            main_fetch_opts, actual_fetch_url
        )

        if raw_info_dict is None:
            raise YtdlpApiError(
                message="No information extracted by yt-dlp. This might be due to filters or content unavailability.",
                feed_name=feed_name,
                url=actual_fetch_url,
            )

        parsed_downloads = self._source_handler.parse_metadata_to_downloads(
            raw_info_dict,
            source_identifier=feed_name,
            ref_type=ref_type,
        )
        logger.info(
            "Successfully processed metadata.",
            extra={
                "feed_name": feed_name,
                "fetch_url": actual_fetch_url,
                "original_url": url,
                "num_downloads_identified": len(parsed_downloads),
            },
        )
        return parsed_downloads
