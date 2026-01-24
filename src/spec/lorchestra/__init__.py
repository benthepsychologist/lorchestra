"""
src.spec.lorchestra - Pure orchestration layer for lorchestra.

This module implements the executor pattern for lorchestra:
  JobDef → JobInstance → RunRecord → StepManifest → AttemptRecord

Components:
  - JobRegistry: Loads and validates JobDefs from storage
  - Compiler: Compiles JobDef + context → JobInstance
  - RunStore: Persists execution artifacts (manifests, outputs, attempts)
  - Executor: Step dispatch, retry logic, and execution flow

Boundaries enforced:
  - lorchestra: Orchestration only (job lifecycle, step dispatch, retry)
  - storacle (data_plane): Data operations (query.*, write.*, assert.*)
  - compute: External IO operations (compute.*)
"""

from .registry import JobRegistry
from .compiler import Compiler, compile_job
from .run_store import RunStore
from .executor import Executor, execute

__all__ = [
    # Registry
    "JobRegistry",
    # Compiler
    "Compiler",
    "compile_job",
    # Run Store
    "RunStore",
    # Executor
    "Executor",
    "execute",
]
