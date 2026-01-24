"""
lorchestra - Lightweight job orchestrator

Discovers and runs jobs from installed packages via entrypoints.
Jobs emit events to BigQuery for tracking and observability.

v2 API (e005-command-plane):
  execute(envelope) -> ExecutionResult

  Envelope schema: {job_id: str, ctx: dict, payload: dict, handlers: HandlerRegistry}

Handler Architecture (e005-03-handlers):
  - HandlerRegistry dispatches StepManifests to appropriate handlers
  - DataPlaneHandler: query.*, write.*, assert.* (via StoracleClient)
  - ComputeHandler: compute.* (via ComputeClient)
  - OrchestrationHandler: job.* (via lorchestra itself)
"""

__version__ = "0.1.0"
__author__ = "Local Pipeline Team"


__all__ = [
    # Legacy config
    "LorchestraConfig",
    "load_config",
    "get_lorchestra_home",
    # v2 executor
    "execute",
    "ExecutionResult",
    # v2 handlers (e005-03)
    "HandlerRegistry",
]

from .config import LorchestraConfig, load_config, get_lorchestra_home

# v2 executor - the primary public API per epic e005-command-plane
from .executor import execute, ExecutionResult

# v2 handlers - handler registry for step dispatch (e005-03-handlers)
from .handlers import HandlerRegistry

