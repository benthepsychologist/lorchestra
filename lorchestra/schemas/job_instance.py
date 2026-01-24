"""
JobInstance schema - a compiled, ready-to-execute job.

A JobInstance is created by compiling a JobDef with a specific context.
All @ctx.* and @payload.* references in step params are resolved,
if conditions are evaluated, and the step list is fixed.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from .ops import Op


@dataclass(frozen=True)
class JobStepInstance:
    """
    A compiled step ready for execution.

    All compile-time references (@ctx.*, @payload.*) have been resolved.
    The step list is fixed - no job.run expansion, no loops.

    Attributes:
        step_id: Unique identifier for the step
        op: The operation to execute
        params: Resolved parameters (no @ctx.* or @payload.* refs, may have @run.*)
        phase_id: Optional phase grouping (no execution semantics in v0)
        timeout_s: Optional step-level timeout in seconds
        continue_on_error: If true, job continues even if this step fails
        compiled_skip: True if step.if evaluated to false at compile time
    """
    step_id: str
    op: Op
    params: dict[str, Any] = field(default_factory=dict)
    phase_id: Optional[str] = None
    timeout_s: int = 300  # Default 300s per e005 spec
    continue_on_error: bool = False
    compiled_skip: bool = False


@dataclass(frozen=True)
class JobInstance:
    """
    A compiled job instance ready for execution.

    Created by compiling a JobDef with a specific context (@ctx.*) and payload (@payload.*).
    The instance has:
    - Fixed step list (no dynamic expansion)
    - Resolved compile-time references
    - Evaluated if conditions (compiled_skip set accordingly)
    - Content-addressed via job_def_sha256

    Attributes:
        job_id: The job identifier (from JobDef)
        job_version: The version (from JobDef)
        job_def_sha256: SHA256 hash of the source JobDef for content addressing
        compiled_at: When this instance was compiled
        steps: Fixed, ordered tuple of compiled steps
    """
    job_id: str
    job_version: str
    job_def_sha256: str
    compiled_at: datetime
    steps: tuple[JobStepInstance, ...] = field(default_factory=tuple)

    def get_step(self, step_id: str) -> Optional[JobStepInstance]:
        """Get a step by ID."""
        for step in self.steps:
            if step.step_id == step_id:
                return step
        return None

    def get_executable_steps(self) -> tuple[JobStepInstance, ...]:
        """Get steps that should be executed (not skipped at compile time)."""
        return tuple(s for s in self.steps if not s.compiled_skip)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON output."""
        return {
            "job_id": self.job_id,
            "job_version": self.job_version,
            "job_def_sha256": self.job_def_sha256,
            "compiled_at": self.compiled_at.isoformat(),
            "steps": [
                {
                    "step_id": s.step_id,
                    "op": s.op.value,
                    "params": s.params,
                    **({"phase_id": s.phase_id} if s.phase_id else {}),
                    "timeout_s": s.timeout_s,  # Always include (default 300s)
                    **({"continue_on_error": s.continue_on_error} if s.continue_on_error else {}),
                    **({"compiled_skip": s.compiled_skip} if s.compiled_skip else {}),
                }
                for s in self.steps
            ],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobInstance":
        """Deserialize from dictionary."""
        steps = []
        for step_data in data.get("steps", []):
            steps.append(JobStepInstance(
                step_id=step_data["step_id"],
                op=Op.from_string(step_data["op"]),
                params=step_data.get("params", {}),
                phase_id=step_data.get("phase_id"),
                timeout_s=step_data.get("timeout_s", 300),  # Default 300s per e005 spec
                continue_on_error=step_data.get("continue_on_error", False),
                compiled_skip=step_data.get("compiled_skip", False),
            ))

        return cls(
            job_id=data["job_id"],
            job_version=data["job_version"],
            job_def_sha256=data["job_def_sha256"],
            compiled_at=datetime.fromisoformat(data["compiled_at"]),
            steps=tuple(steps),
        )
