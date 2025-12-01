"""State reconciliation for startup and configuration changes.

This module provides the StateReconciler class, which handles synchronization
between YAML configuration and database state during application startup and
when configuration changes are detected.
"""

from datetime import UTC, datetime
import logging
from pathlib import Path
from typing import Any

from .config import FeedConfig
from .data_coordinator import Pruner
from .db import DownloadDatabase
from .db.feed_db import FeedDatabase
from .db.types import Download, DownloadStatus, Feed, SourceType, TranscriptSource
from .exceptions import (
    DatabaseOperationError,
    FileOperationError,
    ImageDownloadError,
    PruneError,
    StateReconciliationError,
    YtdlpApiError,
    YtdlpError,
)
from .file_manager import FileManager
from .image_downloader import ImageDownloader
from .metadata import merge_feed_metadata
from .ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)
# YouTube's founding year
MIN_SYNC_DATE = datetime(2005, 1, 1, tzinfo=UTC)


class StateReconciler:
    """Manage state reconciliation between configuration and database.

    The StateReconciler handles synchronization of feed configuration with
    database state during startup and when configuration changes are detected.
    It ensures database consistency and applies configuration changes properly.

    Attributes:
        _file_manager: FileManager for file operations.
        _image_downloader: ImageDownloader for downloading images.
        _feed_db: Database manager for feed record operations.
        _download_db: Database manager for download record operations.
        _pruner: Pruner for feed pruning on deletion.
        _ytdlp_wrapper: YtdlpWrapper for feed discovery operations.
    """

    def __init__(
        self,
        file_manager: FileManager,
        image_downloader: ImageDownloader,
        feed_db: FeedDatabase,
        download_db: DownloadDatabase,
        ytdlp_wrapper: YtdlpWrapper,
        pruner: Pruner,
    ) -> None:
        self._file_manager = file_manager
        self._image_downloader = image_downloader
        self._feed_db = feed_db
        self._download_db = download_db
        self._ytdlp_wrapper = ytdlp_wrapper
        self._pruner = pruner
        logger.debug("StateReconciler initialized.")

    async def _fetch_feed_metadata(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        db_feed: Feed | None,
        cookies_path: Path | None,
    ) -> Feed:
        """Return feed metadata derived from either config or yt-dlp."""
        if feed_config.is_manual:
            return self._build_manual_feed_metadata(feed_id, feed_config)

        assert feed_config.url is not None  # Scheduled feeds validated earlier
        source_url = feed_config.url

        if (
            db_feed is None
            or db_feed.source_url != source_url
            or (not db_feed.is_enabled and feed_config.enabled)
        ):
            # Needs discovery
            (
                source_type,
                resolved_url,
            ) = await self._ytdlp_wrapper.discover_feed_properties(feed_id, source_url)
        else:
            source_type = db_feed.source_type
            resolved_url = db_feed.resolved_url

        fetched_feed = await self._ytdlp_wrapper.fetch_playlist_metadata(
            feed_id=feed_id,
            source_type=source_type,
            source_url=source_url,
            resolved_url=resolved_url,
            user_yt_cli_args=feed_config.yt_args,
            cookies_path=cookies_path,
        )
        return fetched_feed

    def _build_manual_feed_metadata(
        self, feed_id: str, feed_config: FeedConfig
    ) -> Feed:
        """Synthesize metadata for manual feeds from config overrides."""
        overrides = feed_config.metadata
        assert overrides is not None  # Enforced during validation

        manual_feed = Feed(
            id=feed_id,
            is_enabled=True,
            source_type=SourceType.MANUAL,
            source_url=feed_config.url,
            resolved_url=None,
            last_successful_sync=MIN_SYNC_DATE,
        )

        manual_feed.title = overrides.title
        manual_feed.subtitle = overrides.subtitle
        manual_feed.description = overrides.description
        manual_feed.language = overrides.language
        manual_feed.author = overrides.author
        manual_feed.author_email = overrides.author_email or manual_feed.author_email
        manual_feed.remote_image_url = overrides.image_url
        manual_feed.category = overrides.category or manual_feed.category
        manual_feed.podcast_type = overrides.podcast_type or manual_feed.podcast_type
        manual_feed.explicit = (
            overrides.explicit
            if overrides.explicit is not None
            else manual_feed.explicit
        )
        return manual_feed

    async def _handle_new_feed(
        self, feed_id: str, feed_config: FeedConfig, cookies_path: Path | None = None
    ) -> None:
        """Handle a new feed by inserting it into the database.

        Args:
            feed_id: The feed identifier.
            feed_config: The FeedConfig from YAML.
            cookies_path: Optional path to cookies file for yt-dlp authentication.

        Raises:
            StateReconciliationError: If database operations fail.
        """
        log_params = {
            "feed_id": feed_id,
        }
        if feed_config.url:
            log_params["feed_url"] = feed_config.url
        logger.info("Processing new feed.", extra=log_params)

        # Set initial sync timestamp to 'since' if provided, otherwise use min
        initial_sync = feed_config.since if feed_config.since else MIN_SYNC_DATE

        fetched_feed = await self._fetch_feed_metadata(
            feed_id, feed_config, None, cookies_path
        )

        merged_metadata = merge_feed_metadata(fetched_feed, feed_config)
        new_feed = Feed(
            id=feed_id,
            is_enabled=feed_config.enabled,
            source_type=fetched_feed.source_type,
            source_url=fetched_feed.source_url,
            resolved_url=fetched_feed.resolved_url,
            last_successful_sync=initial_sync,
            since=feed_config.since,
            keep_last=feed_config.keep_last,
            transcript_lang=feed_config.transcript_lang,
            transcript_source_priority=feed_config.transcript_source_priority,
            **merged_metadata,
        )

        if feed_config.is_manual:
            image_ext = await self._download_manual_image_override(feed_id, feed_config)
        else:
            image_ext = await self._download_initial_feed_image(
                feed_id, feed_config, fetched_feed, cookies_path
            )

        if image_ext:
            new_feed.image_ext = image_ext
            logger.debug(
                "Successfully downloaded feed image for new feed.",
                extra={**log_params, "image_ext": image_ext},
            )

        try:
            await self._feed_db.upsert_feed(new_feed)
        except (DatabaseOperationError, ValueError) as e:
            raise StateReconciliationError(
                "Failed to insert new feed into database.",
                feed_id=feed_id,
            ) from e

    async def _download_manual_image_override(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        existing_feed: Feed | None = None,
    ) -> str | None:
        """Download image override for manual feeds, if configured."""
        override_url = feed_config.metadata.image_url if feed_config.metadata else None
        if not override_url:
            return None

        if existing_feed and existing_feed.remote_image_url == override_url:
            return existing_feed.image_ext

        try:
            result = await self._image_downloader.download_feed_image_direct(
                feed_id, override_url
            )
        except ImageDownloadError as e:
            logger.warning(
                "Failed to download manual feed image override.",
                extra={"feed_id": feed_id, "image_url": override_url},
                exc_info=e,
            )
            return None

        logger.debug(
            "Downloaded manual feed image override.",
            extra={"feed_id": feed_id, "image_ext": result},
        )
        return result

    async def _download_initial_feed_image(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        fetched_feed: Feed,
        cookies_path: Path | None,
    ) -> str | None:
        """Download the image for newly created scheduled feeds."""
        image_ext = None
        override_url = feed_config.metadata.image_url if feed_config.metadata else None
        if override_url:
            try:
                image_ext = await self._image_downloader.download_feed_image_direct(
                    feed_id, override_url
                )
            except ImageDownloadError as e:
                logger.warning(
                    "Failed to download feed image from override URL.",
                    extra={"feed_id": feed_id, "image_url": override_url},
                    exc_info=e,
                )

        if image_ext:
            return image_ext

        try:
            # this is guaranteed by earlier filtering
            assert fetched_feed.source_url is not None
            return await self._image_downloader.download_feed_image_ytdlp(
                feed_id=feed_id,
                source_type=fetched_feed.source_type,
                source_url=fetched_feed.source_url,
                resolved_url=fetched_feed.resolved_url,
                user_yt_cli_args=feed_config.yt_args,
                cookies_path=cookies_path,
            )
        except ImageDownloadError as e:
            logger.warning(
                "Failed to download natural feed image via yt-dlp.",
                extra={"feed_id": feed_id},
                exc_info=e,
            )
            return None

    async def _handle_constraint_changes(
        self,
        feed_id: str,
        config_since: datetime | None,
        config_keep_last: int | None,
        db_feed: Feed,
        log_params: dict[str, Any],
    ) -> datetime | None:
        """Handle since/keep_last changes: restoration and sync timestamp resets.

        This method considers both retention policies simultaneously to determine
        the set of downloads to restore from archived status as well as the
        last_successful_sync.

        Args:
            feed_id: The feed identifier.
            config_since: The 'since' value from config (or None).
            config_keep_last: The 'keep_last' value from config (or None).
            db_feed: The existing Feed from database.
            log_params: Logging parameters for context.

        Returns:
            new_last_successful_sync: datetime to set, or None if no change needed

        Raises:
            StateReconciliationError: If database operations fail.
        """
        # Check if either retention policy has changed
        since_changed = config_since != db_feed.since
        keep_last_changed = config_keep_last != db_feed.keep_last

        if not since_changed and not keep_last_changed:
            return None

        new_sync: datetime | None = None
        # Default to since if it exists
        restoration_cutoff_date = config_since
        match (db_feed.since, config_since):
            case (None, None):
                should_restore = False
                restoration_cutoff_date = None
            case (None, config_since):
                # Adding 'since' filter - no restoration needed (making filter stricter)
                # Pruner will handle this case automatically
                logger.debug(
                    f"'since' filter added ({config_since}), no restoration needed.",
                    extra={
                        **log_params,
                        "old_since": None,
                        "new_since": config_since,
                    },
                )
                should_restore = False
            case (db_since, None):
                # Removing 'since' filter - potentially restore all archived downloads
                logger.info(
                    f"'since' filter removed (was {db_since}), considering all archived downloads for restoration.",
                    extra={**log_params, "old_since": db_since, "new_since": None},
                )
                should_restore = True
                restoration_cutoff_date = None
                # Reset sync to minimum baseline when since is removed
                new_sync = MIN_SYNC_DATE
            case (db_since, config_since) if config_since < db_since:
                # Expanding 'since' to earlier date - restore downloads between the dates, setting back sync time
                logger.info(
                    f"'since' date expanded from {db_since} to {config_since}, considering downloads after {config_since} for restoration.",
                    extra={
                        **log_params,
                        "old_since": db_since,
                        "new_since": config_since,
                    },
                )
                should_restore = True
                restoration_cutoff_date = config_since
                # Reset sync to expanded since date to allow discovery of unseen items
                new_sync = config_since
            case (db_since, config_since):
                # Unchanged or `since` filter made stricter - no restoration needed
                # Pruner will handle this case automatically
                logger.debug(
                    f"'since' date made stricter from {db_since} to {config_since}, no restoration needed.",
                    extra={
                        **log_params,
                        "old_since": db_since,
                        "new_since": config_since,
                    },
                )
                should_restore = False

        # Handle 'keep_last' changes and determine restoration limit
        match db_feed.keep_last, config_keep_last, db_feed.total_downloads:
            case (None, None, _):
                # No keep_last constraint, no contribution to restoration limit
                logger.debug(
                    "No 'keep_last' constraint, no contribution to restoration limit.",
                    extra=log_params,
                )
                restore_limit = -1
            case (_, config_keep_last, total_downloads) if (
                config_keep_last is not None and config_keep_last > total_downloads
            ):
                # Keep_last allows restoration - can restore up to the difference
                available_slots = config_keep_last - total_downloads
                logger.info(
                    f"'keep_last' limit allows restoration, can restore up to {available_slots} archived downloads.",
                    extra={
                        **log_params,
                        "old_keep_last": db_feed.keep_last,
                        "new_keep_last": config_keep_last,
                    },
                )
                should_restore = True
                restore_limit = available_slots
                # If we haven't decided on a new sync yet, baseline to since/MIN
                if new_sync is None:
                    new_sync = config_since if config_since else MIN_SYNC_DATE
            case (db_keep_last, None, _):
                # Removing 'keep_last' filter - potentially restore downloads based on since
                logger.info(
                    f"'keep_last' filter removed (was {db_keep_last}), considering all archived downloads for restoration.",
                    extra={
                        **log_params,
                        "old_keep_last": db_keep_last,
                        "new_keep_last": None,
                    },
                )
                should_restore = True
                restore_limit = -1
                if new_sync is None:
                    new_sync = config_since if config_since else MIN_SYNC_DATE
            case (_, config_keep_last, _):
                # Keep_last exists and is less than total downloads - constrains restoration
                # This overrides any since expansion since we're at/above the limit
                logger.debug(
                    "'keep_last' limit constrains restoration.",
                    extra={
                        **log_params,
                        "old_keep_last": db_feed.keep_last,
                        "new_keep_last": config_keep_last,
                    },
                )
                should_restore = False
                restore_limit = -1

        # Check if we should restore based on the combined policies
        if not should_restore:
            return new_sync

        # Find archived downloads that should be restored
        try:
            downloads_to_restore = await self._download_db.get_downloads_by_status(
                DownloadStatus.ARCHIVED,
                feed_id=feed_id,
                published_after=restoration_cutoff_date,  # None means all downloads
                limit=restore_limit,
            )
        except DatabaseOperationError as e:
            raise StateReconciliationError(
                "Failed to fetch archived downloads for retention policy check.",
                feed_id=feed_id,
            ) from e

        if not downloads_to_restore:
            logger.debug(
                "No archived downloads to restore for retention policy changes.",
                extra=log_params,
            )
            return new_sync

        # Log the restoration details
        restore_reason: list[str] = []
        if since_changed:
            restore_reason.append("'since' expansion")
        if keep_last_changed and config_keep_last is not None:
            restore_reason.append("'keep_last' increase")
        if keep_last_changed and config_keep_last is None:
            restore_reason.append("'keep_last' removed")

        logger.info(
            f"Restoring {len(downloads_to_restore)} archived downloads due to {' and '.join(restore_reason)}.",
            extra={
                **log_params,
                "since_date": config_since.isoformat() if config_since else None,
                "keep_last": config_keep_last,
            },
        )

        # Restore downloads to QUEUED status in batch
        download_ids = [dl.id for dl in downloads_to_restore]
        try:
            count_restored = await self._download_db.requeue_downloads(
                feed_id, download_ids, from_status=DownloadStatus.ARCHIVED
            )
            logger.info(
                f"Successfully restored {count_restored} archived downloads to QUEUED.",
                extra={**log_params, "count_restored": count_restored},
            )
        except DatabaseOperationError as e:
            raise StateReconciliationError(
                "Failed to restore archived downloads.",
                feed_id=feed_id,
            ) from e

        return new_sync

    async def _handle_image_url_changes(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        db_feed: Feed,
        updated_feed: Feed,
        cookies_path: Path | None = None,
    ) -> str | None:
        """Handle image URL override changes and removals.

        Args:
            feed_id: The feed identifier.
            feed_config: The FeedConfig from YAML.
            db_feed: The existing Feed from database.
            updated_feed: The updated Feed with merged metadata.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            New image_ext value or None if no change needed.

        Raises:
            StateReconciliationError: If image operations fail.
        """
        log_params = {"feed_id": feed_id}

        # Prepare inputs for decision: DB value, config override, and current natural URL
        config_image_url = (
            feed_config.metadata.image_url if feed_config.metadata else None
        )
        db_url = db_feed.remote_image_url
        curr_url = updated_feed.remote_image_url

        # Determine what action to take based on URL changes
        match (db_url, config_image_url, curr_url):
            # Override added/changed: config provides a URL different from DB value
            case (old_url, str() as new_url, _) if old_url != new_url:
                logger.debug(
                    "Image URL override change detected - will download new image.",
                    extra={**log_params, "old_url": old_url, "new_url": new_url},
                )
                should_download = True
                download_by_ytdlp = False
            # Override removed: DB had a URL; config cleared it; natural differs from DB value
            case (str() as old_url, None, str() as natural_url) if (
                old_url != natural_url
            ):
                logger.debug(
                    "Image URL override removed - will download natural feed image.",
                    extra={
                        **log_params,
                        "old_url": old_url,
                        "natural_url": natural_url,
                    },
                )
                should_download = True
                download_by_ytdlp = True
            case _:
                return None

        # Execute the download
        if should_download:
            try:
                if download_by_ytdlp:
                    # this is guaranteed by earlier filtering
                    assert updated_feed.source_url is not None
                    result = await self._image_downloader.download_feed_image_ytdlp(
                        feed_id=feed_id,
                        source_type=updated_feed.source_type,
                        source_url=updated_feed.source_url,
                        resolved_url=updated_feed.resolved_url,
                        user_yt_cli_args=feed_config.yt_args,
                        cookies_path=cookies_path,
                    )

                else:
                    # config_image_url is guaranteed to be defined due to match statement
                    assert config_image_url is not None
                    result = await self._image_downloader.download_feed_image_direct(
                        feed_id,
                        config_image_url,
                    )
            except ImageDownloadError as e:
                logger.warning(
                    f"Failed to download feed image via {'ytdlp' if download_by_ytdlp else 'direct'} method.",
                    extra=log_params,
                    exc_info=e,
                )
                return None
            else:
                if result:
                    logger.debug(
                        f"Successfully downloaded feed image via {'ytdlp' if download_by_ytdlp else 'direct'} method.",
                        extra={**log_params, "image_ext": result},
                    )
                    return result
                else:
                    logger.warning(
                        f"Feed image download via {'ytdlp' if download_by_ytdlp else 'direct'} method failed.",
                        extra=log_params,
                    )
                    return None

        return None

    async def _download_transcripts_for_downloads(
        self,
        feed_id: str,
        downloads: list[Download],
        lang: str,
        transcript_source_priority: list[TranscriptSource] | None,
        user_yt_cli_args: list[str],
        cookies_path: Path | None,
        log_params: dict[str, Any],
    ) -> None:
        """Download transcripts for a list of downloads.

        For each download, refreshes metadata to determine transcript_source,
        then downloads the transcript if available.

        Args:
            feed_id: The feed identifier.
            downloads: List of Download objects to process.
            lang: Language code for subtitles.
            transcript_source_priority: Ordered list of transcript sources to try.
            user_yt_cli_args: User-provided yt-dlp CLI arguments.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.
            log_params: Logging parameters for context.

        Raises:
            StateReconciliationError: If metadata fetch fails for a download.
        """
        success_count = 0
        unavailable_count = 0
        fail_count = 0

        for download in downloads:
            if download.transcript_ext:
                continue

            download_log_params = {**log_params, "download_id": download.id}

            # Refresh metadata to get transcript_source
            try:
                refreshed_downloads = (
                    await self._ytdlp_wrapper.fetch_new_downloads_metadata(
                        feed_id=feed_id,
                        source_type=SourceType.SINGLE_VIDEO,
                        source_url=download.source_url,
                        resolved_url=None,
                        user_yt_cli_args=user_yt_cli_args,
                        transcript_lang=lang,
                        transcript_source_priority=transcript_source_priority,
                        cookies_path=cookies_path,
                    )
                )
            except (YtdlpApiError, YtdlpError) as e:
                raise StateReconciliationError(
                    f"Failed to refresh metadata for transcript detection (download_id={download.id}).",
                    feed_id=feed_id,
                ) from e

            if not refreshed_downloads:
                logger.debug(
                    "No metadata returned for download during transcript backfill.",
                    extra=download_log_params,
                )
                unavailable_count += 1
                continue

            refreshed_download = refreshed_downloads[0]
            transcript_source = refreshed_download.transcript_source

            if (
                not transcript_source
                or transcript_source == TranscriptSource.NOT_AVAILABLE
            ):
                logger.debug(
                    "No transcript available for download.",
                    extra=download_log_params,
                )
                unavailable_count += 1
                continue

            # Download the transcript using the detected source
            ext = await self._ytdlp_wrapper.download_transcript_only(
                feed_id=feed_id,
                download_id=download.id,
                source_url=download.source_url,
                transcript_lang=lang,
                transcript_source=transcript_source,
                cookies_path=cookies_path,
            )

            if ext:
                try:
                    await self._download_db.set_transcript_metadata(
                        feed_id=feed_id,
                        download_id=download.id,
                        transcript_ext=ext,
                        transcript_lang=lang,
                        transcript_source=transcript_source,
                    )
                    success_count += 1
                except DatabaseOperationError as e:
                    logger.warning(
                        "Failed to persist transcript metadata.",
                        extra=download_log_params,
                        exc_info=e,
                    )
                    fail_count += 1
            else:
                logger.debug(
                    "Transcript file not found after download attempt.",
                    extra=download_log_params,
                )
                fail_count += 1

        logger.info(
            f"Transcript download complete: {success_count} succeeded, {unavailable_count} unavailable, {fail_count} failed.",
            extra={
                **log_params,
                "success_count": success_count,
                "unavailable_count": unavailable_count,
                "fail_count": fail_count,
            },
        )

    async def _delete_transcripts_for_downloads(
        self,
        feed_id: str,
        downloads: list[Download],
        log_params: dict[str, Any],
    ) -> None:
        """Delete transcripts for a list of downloads.

        Args:
            feed_id: The feed identifier.
            downloads: List of Download objects to process.
            log_params: Logging parameters for context.
        """
        deleted_count = 0
        for download in downloads:
            if not download.transcript_lang or not download.transcript_ext:
                continue

            try:
                await self._file_manager.delete_transcript(
                    feed_id,
                    download.id,
                    download.transcript_lang,
                    download.transcript_ext,
                )
                deleted_count += 1
            except FileNotFoundError:
                logger.debug(
                    "Transcript file not found for deletion.",
                    extra={**log_params, "download_id": download.id},
                )
            except FileOperationError as e:
                logger.warning(
                    "Failed to delete transcript file.",
                    extra={**log_params, "download_id": download.id},
                    exc_info=e,
                )

            try:
                await self._download_db.set_transcript_metadata(
                    feed_id=feed_id,
                    download_id=download.id,
                    transcript_ext=None,
                    transcript_lang=None,
                    transcript_source=None,
                )
            except DatabaseOperationError as e:
                logger.warning(
                    "Failed to clear transcript metadata.",
                    extra={**log_params, "download_id": download.id},
                    exc_info=e,
                )

        logger.info(
            f"Deleted {deleted_count} transcript files.",
            extra={**log_params, "deleted_count": deleted_count},
        )

    async def _handle_transcript_config_changes(
        self,
        feed_id: str,
        config_transcript_lang: str | None,
        config_transcript_source_priority: list[TranscriptSource] | None,
        db_transcript_lang: str | None,
        db_transcript_source_priority: list[TranscriptSource] | None,
        user_yt_cli_args: list[str],
        cookies_path: Path | None,
        log_params: dict[str, Any],
    ) -> None:
        """Handle transcript config changes: download or delete transcripts.

        Triggers re-processing when transcript_lang or transcript_source_priority
        changes. When transcript_lang is enabled (None → value), downloads transcripts
        for all DOWNLOADED items. When disabled (value → None), deletes all transcript
        files and clears metadata. When language or priority changes, re-downloads
        with the updated configuration.

        Args:
            feed_id: The feed identifier.
            config_transcript_lang: The transcript_lang from config (or None).
            config_transcript_source_priority: Ordered list of transcript sources to try.
            db_transcript_lang: The transcript_lang stored in database (or None).
            db_transcript_source_priority: Ordered list of transcript sources from database.
            user_yt_cli_args: User-provided yt-dlp CLI arguments.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.
            log_params: Logging parameters for context.

        Raises:
            StateReconciliationError: If database or file operations fail.
        """
        lang_changed = config_transcript_lang != db_transcript_lang
        priority_changed = (
            config_transcript_source_priority != db_transcript_source_priority
        )

        if not lang_changed and not priority_changed:
            return

        try:
            downloads = await self._download_db.get_downloads_by_status(
                DownloadStatus.DOWNLOADED, feed_id=feed_id
            )
        except DatabaseOperationError as e:
            raise StateReconciliationError(
                "Failed to fetch downloads for transcript reconciliation.",
                feed_id=feed_id,
            ) from e

        if not downloads:
            logger.debug(
                "No downloaded items to reconcile transcripts for.",
                extra=log_params,
            )
            return

        match (
            db_transcript_lang,
            config_transcript_lang,
            lang_changed,
            priority_changed,
        ):
            case (None, str() as new_lang, True, _):
                logger.info(
                    f"Transcript support enabled with lang '{new_lang}', downloading transcripts for {len(downloads)} items.",
                    extra={**log_params, "transcript_lang": new_lang},
                )
                await self._download_transcripts_for_downloads(
                    feed_id,
                    downloads,
                    new_lang,
                    config_transcript_source_priority,
                    user_yt_cli_args,
                    cookies_path,
                    log_params,
                )

            case (str() as old_lang, None, True, _):
                logger.info(
                    f"Transcript support disabled (was '{old_lang}'), deleting transcripts for {len(downloads)} items.",
                    extra={**log_params, "old_transcript_lang": old_lang},
                )
                await self._delete_transcripts_for_downloads(
                    feed_id, downloads, log_params
                )

            case (str() as old_lang, str() as new_lang, True, _):
                logger.info(
                    f"Transcript language changed from '{old_lang}' to '{new_lang}', re-downloading transcripts for {len(downloads)} items.",
                    extra={
                        **log_params,
                        "old_transcript_lang": old_lang,
                        "new_transcript_lang": new_lang,
                    },
                )
                await self._delete_transcripts_for_downloads(
                    feed_id, downloads, log_params
                )
                await self._download_transcripts_for_downloads(
                    feed_id,
                    downloads,
                    new_lang,
                    config_transcript_source_priority,
                    user_yt_cli_args,
                    cookies_path,
                    log_params,
                )

            case (_, str() as current_lang, _, True):
                logger.info(
                    f"Transcript source priority changed, re-downloading transcripts for {len(downloads)} items.",
                    extra={
                        **log_params,
                        "transcript_lang": current_lang,
                        "old_priority": [s.value for s in db_transcript_source_priority]
                        if db_transcript_source_priority
                        else None,
                        "new_priority": [
                            s.value for s in config_transcript_source_priority
                        ]
                        if config_transcript_source_priority
                        else None,
                    },
                )
                await self._delete_transcripts_for_downloads(
                    feed_id, downloads, log_params
                )
                await self._download_transcripts_for_downloads(
                    feed_id,
                    downloads,
                    current_lang,
                    config_transcript_source_priority,
                    user_yt_cli_args,
                    cookies_path,
                    log_params,
                )

            case _:
                pass

    async def _handle_existing_feed(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        db_feed: Feed,
        cookies_path: Path | None = None,
    ) -> bool:
        """Handle an existing feed by applying configuration changes.

        Args:
            feed_id: The feed identifier.
            feed_config: The FeedConfig from YAML.
            db_feed: The existing Feed from database.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            True if any changes were applied.

        Raises:
            StateReconciliationError: If database operations fail.
        """
        log_params = {"feed_id": feed_id}

        fetched_feed = await self._fetch_feed_metadata(
            feed_id, feed_config, db_feed, cookies_path
        )

        merged_metadata = merge_feed_metadata(fetched_feed, feed_config)
        updated_feed = db_feed.model_copy(
            update={
                **merged_metadata,
                "is_enabled": feed_config.enabled,
                "source_type": fetched_feed.source_type,
                "source_url": fetched_feed.source_url,
                "resolved_url": fetched_feed.resolved_url,
                "since": feed_config.since,
                "keep_last": feed_config.keep_last,
                "transcript_lang": feed_config.transcript_lang,
                "transcript_source_priority": feed_config.transcript_source_priority,
            }
        )

        if (
            not db_feed.is_enabled and feed_config.enabled
        ) or db_feed.source_url != fetched_feed.source_url:
            logger.info(
                "Feed metadata reset due to enablement or URL change.",
                extra=log_params,
            )
            updated_feed.last_successful_sync = (
                feed_config.since if feed_config.since else MIN_SYNC_DATE
            )
            updated_feed.last_failed_sync = None
            updated_feed.consecutive_failures = 0

        if db_feed.since is not None and feed_config.since is None:
            logger.info(
                "'since' constraint removed, resetting last_successful_sync to allow re-fetching all videos.",
                extra={**log_params, "old_since": db_feed.since, "new_since": None},
            )
            updated_feed.last_successful_sync = MIN_SYNC_DATE

        new_sync = await self._handle_constraint_changes(
            feed_id, feed_config.since, feed_config.keep_last, db_feed, log_params
        )
        if new_sync is not None:
            updated_feed.last_successful_sync = new_sync

        if feed_config.is_manual:
            new_image_ext = await self._download_manual_image_override(
                feed_id, feed_config, existing_feed=db_feed
            )
        else:
            new_image_ext = await self._handle_image_url_changes(
                feed_id, feed_config, db_feed, updated_feed, cookies_path
            )
        if new_image_ext is not None:
            updated_feed.image_ext = new_image_ext

        await self._handle_transcript_config_changes(
            feed_id,
            feed_config.transcript_lang,
            feed_config.transcript_source_priority,
            db_feed.transcript_lang,
            db_feed.transcript_source_priority,
            feed_config.yt_args,
            cookies_path,
            log_params,
        )

        if updated_feed != db_feed:
            logger.debug("Feed configuration changes applied.", extra=log_params)
            try:
                await self._feed_db.upsert_feed(updated_feed)
            except DatabaseOperationError as e:
                raise StateReconciliationError(
                    "Failed to update feed configuration.",
                    feed_id=feed_id,
                ) from e
            return True

        logger.debug("No feed configuration changes detected.", extra=log_params)
        return False

    async def _handle_removed_feed(self, feed_id: str) -> None:
        """Handle a removed feed by marking it as disabled in the database.

        Args:
            feed_id: The feed identifier.
            db_feed: The existing Feed from database.

        Raises:
            StateReconciliationError: If archive action fails.
        """
        try:
            await self._pruner.archive_feed(feed_id)
        except PruneError as e:
            raise StateReconciliationError(
                "Failed to archive feed.",
                feed_id=feed_id,
            ) from e

    async def reconcile_startup_state(
        self, config_feeds: dict[str, FeedConfig], cookies_path: Path | None = None
    ) -> list[str]:
        """Reconcile configuration feeds with database state on startup.

        Compares the current YAML configuration with database feeds and performs
        necessary updates:
        - New feeds: Insert into database with initial sync timestamp
        - Removed feeds: Mark as disabled in database (only if currently enabled)
        - Changed feeds: Update metadata and configuration
        - Paused feeds: Feeds disabled in config are kept but not scheduled

        Args:
            config_feeds: Dictionary mapping feed_id to FeedConfig from YAML.
            cookies_path: Optional path to cookies file for yt-dlp authentication.

        Returns:
            List of feed IDs that are ready for scheduling (enabled and valid).

        Raises:
            StateReconciliationError: If reconciliation fails for critical operations.
        """
        logger.debug(
            "Starting state reconciliation for startup.",
            extra={"config_feed_count": len(config_feeds)},
        )

        # Get all existing feeds from database
        try:
            db_feeds = await self._feed_db.get_feeds()
        except (DatabaseOperationError, ValueError) as e:
            raise StateReconciliationError(
                "Failed to fetch feeds from database.",
            ) from e

        db_feed_lookup = {feed.id: feed for feed in db_feeds}
        ready_feeds: list[str] = []
        new_count = 0
        changed_count = 0
        processed_feed_ids: set[str] = set()
        failed_feeds: dict[str, str] = {}  # Track feeds that failed with error summary

        # Process all feeds from configuration
        for feed_id, feed_config in config_feeds.items():
            processed_feed_ids.add(feed_id)
            db_feed = db_feed_lookup.get(feed_id)

            if db_feed is None:
                # New feed - add to database
                try:
                    await self._handle_new_feed(feed_id, feed_config, cookies_path)
                except StateReconciliationError as e:
                    error_summary = str(e)
                    failed_feeds[feed_id] = error_summary
                    logger.warning(
                        "Failed to add new feed, continuing with others.",
                        extra={"feed_id": feed_id},
                        exc_info=e,
                    )
                else:
                    new_count += 1
                    if feed_config.enabled and not feed_config.is_manual:
                        ready_feeds.append(feed_id)
            else:
                # Existing feed - check for changes
                try:
                    if await self._handle_existing_feed(
                        feed_id, feed_config, db_feed, cookies_path
                    ):
                        changed_count += 1
                except StateReconciliationError as e:
                    error_summary = str(e)
                    failed_feeds[feed_id] = error_summary
                    logger.warning(
                        "Failed to update existing feed, continuing with others.",
                        extra={"feed_id": feed_id},
                        exc_info=e,
                    )
                else:
                    if feed_config.enabled and not feed_config.is_manual:
                        ready_feeds.append(feed_id)

        # Handle removed feeds - only those that are enabled in DB but not in config
        removed_count = 0
        for feed_id, db_feed in db_feed_lookup.items():
            if feed_id not in processed_feed_ids and db_feed.is_enabled:
                # Feed is enabled in DB but not present in config - mark as removed
                try:
                    await self._handle_removed_feed(feed_id)
                except StateReconciliationError as e:
                    logger.warning(
                        "Failed to disable removed feed, continuing with others.",
                        extra={"feed_id": feed_id},
                        exc_info=e,
                    )
                else:
                    removed_count += 1

        logger.debug(
            "State reconciliation completed successfully.",
            extra={
                "new_feeds": new_count,
                "removed_feeds": removed_count,
                "changed_feeds": changed_count,
                "ready_feeds": len(ready_feeds),
                "failed_feeds": len(failed_feeds),
            },
        )

        # If we have configured feeds but none are ready, include error details
        if config_feeds and not ready_feeds and failed_feeds:
            logger.error(
                "All configured feeds failed during reconciliation.",
                extra={
                    "configured_feeds": len(config_feeds),
                    "failed_feeds": failed_feeds,
                },
            )

        return ready_feeds
