"""Phase result tracking for DataCoordinator operations.

This module defines the PhaseResult dataclass used to track the outcome,
timing, and error information for individual phases of feed processing
within the DataCoordinator workflow.

The PhaseResult is used to capture results from:
- Enqueue phase: New downloads discovered and queued
- Download phase: Media files successfully downloaded
- Prune phase: Old downloads archived and files deleted
- RSS generation phase: Feed XML generated and cached

Each phase result includes success status, item counts, timing information,
and any errors that occurred during execution.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PhaseResult:
    """Results from a single processing phase.

    Attributes:
        success: Whether the phase completed successfully.
        count: Number of items processed in this phase.
        errors: List of errors that occurred during this phase.
        duration_seconds: Time taken to complete this phase.
    """

    success: bool
    count: int
    errors: list[Exception] = field(default_factory=list[Exception])
    duration_seconds: float = 0.0
