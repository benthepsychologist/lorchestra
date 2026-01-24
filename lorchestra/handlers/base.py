"""
Base handler protocol and common implementations.

Handlers are responsible for executing StepManifests dispatched by the Executor.
Each handler handles operations for a specific backend type:
- data_plane: query.*, write.*, assert.* (via storacle)
- compute: compute.* (via compute service)
- orchestration: job.* (via lorchestra itself)
"""

from abc import ABC, abstractmethod
from typing import Any

from lorchestra.schemas import StepManifest


class Handler(ABC):
    """
    Abstract base class for execution handlers.

    Handlers receive StepManifests and execute the corresponding operation,
    returning the result as a dictionary.
    """

    @abstractmethod
    def execute(self, manifest: StepManifest) -> dict[str, Any]:
        """
        Execute a step manifest.

        Args:
            manifest: The StepManifest containing operation details

        Returns:
            The execution result as a dictionary

        Raises:
            Exception: If execution fails
        """
        pass


class NoOpHandler(Handler):
    """
    No-op handler for testing and dry-run mode.

    Returns empty results without executing anything.
    """

    def execute(self, manifest: StepManifest) -> dict[str, Any]:
        """Return a no-op result without executing."""
        return {
            "status": "noop",
            "backend": manifest.backend,
            "op": manifest.op.value,
            "params": manifest.resolved_params,
        }
