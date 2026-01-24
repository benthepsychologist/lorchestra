"""
lorchestra - Lightweight job orchestrator

Discovers and runs jobs from installed packages via entrypoints.
Jobs emit events to BigQuery for tracking and observability.

v2 API:
  execute(envelope) -> ExecutionResult

  Envelope schema: {job_id: str, ctx: dict, payload: dict}
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
]

from .config import LorchestraConfig, load_config, get_lorchestra_home

# v2 executor - the primary public API per epic e005-command-plane
from .executor import execute, ExecutionResult

