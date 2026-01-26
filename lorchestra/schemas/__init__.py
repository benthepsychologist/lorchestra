"""
lorchestra.schemas - Schema definitions for the orchestration layer.

This module defines the core data structures for lorchestra:

JobDef -> JobInstance -> RunRecord -> StepManifest -> AttemptRecord

Lifecycle:
1. JobDef: Static, version-controlled job definition with @ctx.*, @payload.*, @run.* refs
2. JobInstance: Compiled job with resolved @ctx.* and @payload.* refs, fixed step list
3. RunRecord: Runtime record when a job starts execution
4. StepManifest: Dispatchable unit sent to backends (data_plane, compute, orchestration)
5. AttemptRecord: Tracks execution attempts and step outcomes

Boundaries:
- lorchestra: Orchestration (job lifecycle, step dispatch, retry logic)
- storacle (data_plane): Data operations (query.*, write.*, assert.*)
- compute: External IO operations (compute.*)
"""

from .ops import Op
from .job_def import (
    JobDef,
    StepDef,
    IdempotencyConfig,
    CompileError,
)
from .job_instance import (
    JobInstance,
    JobStepInstance,
)
from .run_record import (
    RunRecord,
    ULID,
)
from .step_manifest import (
    StepManifest,
)
from .attempt import (
    AttemptRecord,
    StepOutcome,
    StepStatus,
)

__all__ = [
    # Ops
    "Op",
    # Job Definition
    "JobDef",
    "StepDef",
    "IdempotencyConfig",
    "CompileError",
    # Job Instance
    "JobInstance",
    "JobStepInstance",
    # Run Record
    "RunRecord",
    "ULID",
    # Step Manifest
    "StepManifest",
    # Attempt
    "AttemptRecord",
    "StepOutcome",
    "StepStatus",
]
