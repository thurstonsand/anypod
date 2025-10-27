"""Background trigger utility for manual feeds.

This module defines the ManualFeedRunner which schedules DataCoordinator
runs in response to manual download submissions without blocking HTTP
handlers. It ensures manual runs share the same semaphore as scheduled
jobs and de-duplicates pending tasks per feed.
"""

import asyncio
import logging
from typing import Any

from .config import FeedConfig
from .data_coordinator import DataCoordinator
from .data_coordinator.types import ProcessingResults

logger = logging.getLogger(__name__)


class ManualFeedRunner:
    """Schedule manual feed processing without blocking HTTP responses.

    Attributes:
        _data_coordinator: Orchestrates enqueue/download/prune/RSS stages.
        _feed_configs: Mapping of feed identifiers to configuration objects.
        _feed_semaphore: Semaphore shared with the scheduler to limit concurrency.
        _queued_tasks: Registry of currently scheduled asyncio tasks per feed.
        _lock: Async lock guarding access to ``_queued_tasks``.
    """

    def __init__(
        self,
        data_coordinator: DataCoordinator,
        feed_configs: dict[str, FeedConfig],
        feed_semaphore: asyncio.Semaphore,
    ) -> None:
        self._data_coordinator = data_coordinator
        self._feed_configs = feed_configs
        self._feed_semaphore = feed_semaphore
        self._queued_tasks: dict[str, asyncio.Task[ProcessingResults | None]] = {}
        self._lock = asyncio.Lock()

    async def _run_feed(
        self, feed_id: str, feed_config: FeedConfig
    ) -> ProcessingResults | None:
        """Execute coordinator work for ``feed_id`` while holding the semaphore.

        Args:
            feed_id: Feed to process.
            feed_config: Configuration snapshot for the feed.

        Returns:
            Processing results returned by :class:`DataCoordinator`.

        Raises:
            DataCoordinatorError: Raised when the coordinator fails for the feed.
        """
        log_params = {"feed_id": feed_id}
        results: ProcessingResults | None = None
        async with self._feed_semaphore:
            async with self._lock:
                self._queued_tasks.pop(feed_id, None)
            logger.info("Manual feed processing started.", extra=log_params)
            results = await self._data_coordinator.process_feed(feed_id, feed_config)
        logger.info(
            "Manual feed processing completed.",
            extra={**log_params, **results.summary_dict()},
        )
        return results

    def _task_done_callback(self, feed_id: str):
        """Create a callback that logs task cancellation or failure.

        Args:
            feed_id: Feed identifier associated with the task.

        Returns:
            Function suitable for :meth:`asyncio.Task.add_done_callback`.
        """

        def _callback(task: asyncio.Task[Any]) -> None:
            if task.cancelled():
                logger.warning(
                    "Manual feed task cancelled.",
                    extra={"feed_id": feed_id},
                )
                return
            exc = task.exception()
            if exc:
                logger.error(
                    "Manual feed processing task failed.",
                    extra={"feed_id": feed_id},
                    exc_info=exc,
                )

        return _callback

    async def trigger(self, feed_id: str, feed_config: FeedConfig) -> None:
        """Queue a manual run if another task is not already pending.

        Args:
            feed_id: Identifier of the manual-only feed.
            feed_config: Configuration for the feed.
        """
        async with self._lock:
            existing_task = self._queued_tasks.get(feed_id)
            if existing_task and not existing_task.done():
                logger.debug(
                    "Manual processing already queued.",
                    extra={"feed_id": feed_id},
                )
                return

            task = asyncio.create_task(self._run_feed(feed_id, feed_config))
            self._queued_tasks[feed_id] = task
            task.add_done_callback(self._task_done_callback(feed_id))

    async def shutdown(self) -> None:
        """Cancel all pending tasks and wait for completion.

        Call during application shutdown to ensure clean termination
        of background processing.
        """
        async with self._lock:
            tasks = list(self._queued_tasks.values())
            self._queued_tasks.clear()

        if not tasks:
            return

        logger.info("Cancelling manual feed tasks.", extra={"count": len(tasks)})
        for task in tasks:
            if not task.done():
                task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Manual feed tasks cancelled.")
