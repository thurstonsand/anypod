"""Processing results for DataCoordinator operations.

This module defines the ProcessingResults dataclass used to track
the outcomes of feed processing operations across all phases.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .phase_result import PhaseResult


@dataclass
class ProcessingResults:
    """Comprehensive results from a DataCoordinator.process_feed() operation.

    Tracks results and timing for each phase of feed processing:
    enqueue, download, prune, and RSS generation.

    Attributes:
        feed_id: The feed identifier that was processed.
        start_time: When processing began.
        total_duration_seconds: Total time for all phases.
        overall_success: True if all critical phases succeeded.
        enqueue_result: Results from the enqueue phase.
        download_result: Results from the download phase.
        prune_result: Results from the prune phase.
        rss_generation_result: Results from RSS generation phase.
        feed_sync_updated: Whether the feed's last_sync timestamp was updated.
        fatal_error: Any fatal error that stopped processing entirely.
    """

    feed_id: str
    start_time: datetime
    total_duration_seconds: float = 0.0
    overall_success: bool = False

    # Phase-specific results
    enqueue_result: PhaseResult = field(
        default_factory=lambda: PhaseResult(success=False, count=0)
    )
    download_result: PhaseResult = field(
        default_factory=lambda: PhaseResult(success=False, count=0)
    )
    prune_result: PhaseResult = field(
        default_factory=lambda: PhaseResult(success=False, count=0)
    )
    rss_generation_result: PhaseResult = field(
        default_factory=lambda: PhaseResult(success=False, count=0)
    )

    # Feed-level tracking
    feed_sync_updated: bool = False
    fatal_error: Exception | None = None

    @property
    def total_enqueued(self) -> int:
        """Total items enqueued."""
        return self.enqueue_result.count

    @property
    def total_downloaded(self) -> int:
        """Total items successfully downloaded."""
        return self.download_result.count

    @property
    def total_archived(self) -> int:
        """Total items archived during pruning."""
        return self.prune_result.count

    @property
    def all_errors(self) -> list[Exception]:
        """All errors from all phases."""
        errors: list[Exception] = []
        if self.fatal_error:
            errors.append(self.fatal_error)
        errors.extend(self.enqueue_result.errors)
        errors.extend(self.download_result.errors)
        errors.extend(self.prune_result.errors)
        errors.extend(self.rss_generation_result.errors)
        return errors

    @property
    def has_errors(self) -> bool:
        """True if any errors occurred during processing."""
        return len(self.all_errors) > 0

    def summary_dict(self) -> dict[str, Any]:
        """Return a dictionary summary suitable for logging."""
        return {
            "feed_id": self.feed_id,
            "overall_success": self.overall_success,
            "total_duration_seconds": self.total_duration_seconds,
            "enqueued": self.total_enqueued,
            "downloaded": self.total_downloaded,
            "archived": self.total_archived,
            "rss_generated": self.rss_generation_result.success,
            "feed_sync_updated": self.feed_sync_updated,
            "error_count": len(self.all_errors),
            "fatal_error": str(self.fatal_error) if self.fatal_error else None,
        }
