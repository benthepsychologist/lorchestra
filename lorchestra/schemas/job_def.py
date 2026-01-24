"""
JobDef schema - the declarative job definition.

A JobDef is the static, version-controlled definition of a job.
It contains step definitions with parameter references (@ctx.*, @payload.*, @run.*)
that are resolved during compilation and execution.
"""

from dataclasses import dataclass, field
from typing import Any, Literal, Optional

from .ops import Op


class CompileError(Exception):
    """Raised when a JobDef cannot be compiled."""
    pass


@dataclass(frozen=True)
class IdempotencyConfig:
    """
    Idempotency configuration for write operations.

    scope:
        - "run": Key includes run_id (default for write ops)
        - "semantic": Key based on semantic_key_ref from params
        - "explicit": Key provided explicitly in params

    semantic_key_ref: Required when scope is "semantic".
        References a param that provides the semantic key (e.g., "@payload.entity_id")

    include_payload_hash: Whether to include a hash of the resolved params
        in the idempotency key. Default false.
    """
    scope: Literal["run", "semantic", "explicit"]
    semantic_key_ref: Optional[str] = None
    include_payload_hash: bool = False

    def __post_init__(self):
        if self.scope == "semantic" and not self.semantic_key_ref:
            raise ValueError("semantic_key_ref is required when scope is 'semantic'")
        if self.scope != "semantic" and self.semantic_key_ref:
            raise ValueError("semantic_key_ref is only valid when scope is 'semantic'")


@dataclass(frozen=True)
class StepDef:
    """
    A step definition within a JobDef.

    Attributes:
        step_id: Unique identifier for the step within the job
        op: The operation to execute (from Op enum)
        params: Parameters for the operation, may contain @ctx.*, @payload.*, @run.* refs
        phase_id: Optional phase grouping (no execution semantics in v0)
        timeout_s: Optional step-level timeout in seconds
        continue_on_error: If true, job continues even if this step fails
        if_: Compile-time conditional (must use only @ctx.* and @payload.*, not @run.*)
        idempotency: Required for write ops, must be absent for non-write ops
    """
    step_id: str
    op: Op
    params: dict[str, Any] = field(default_factory=dict)
    phase_id: Optional[str] = None
    timeout_s: int = 300  # Default 300s per e005 spec
    continue_on_error: bool = False
    if_: Optional[str] = None
    idempotency: Optional[IdempotencyConfig] = None

    def __post_init__(self):
        # Validate if_ condition only uses compile-time refs
        if self.if_ is not None:
            self._validate_if_condition()

        # Validate idempotency rules
        self._validate_idempotency()

    def _validate_if_condition(self):
        """
        Validate that the if_ condition only uses compile-time decidable refs.
        @ctx.* and @payload.* are allowed, @run.* is not.
        """
        if self.if_ and "@run." in self.if_:
            raise CompileError(
                f"Step '{self.step_id}': if condition must be compile-time decidable. "
                f"@run.* references are not allowed in if conditions. "
                f"Use only @ctx.* and @payload.* references."
            )

    def _validate_idempotency(self):
        """
        Validate idempotency configuration based on op type.
        - Write ops: idempotency defaults to {scope: run} if omitted (handled at creation)
        - Non-write ops: idempotency MUST be absent/null
        """
        if self.op.requires_idempotency:
            # Write ops are valid with or without explicit idempotency
            # (default applied at compile time)
            pass
        else:
            # Non-write ops must NOT have idempotency
            if self.idempotency is not None:
                raise CompileError(
                    f"Step '{self.step_id}': idempotency is only valid for write operations, "
                    f"but op is '{self.op.value}'"
                )


@dataclass(frozen=True)
class JobDef:
    """
    A job definition - the declarative specification of a job.

    JobDef is immutable and version-controlled. It defines what a job does,
    but not when or with what specific data.

    Attributes:
        job_id: Unique identifier for the job
        version: Semantic version of the job definition
        steps: Ordered list of step definitions
    """
    job_id: str
    version: str
    steps: tuple[StepDef, ...] = field(default_factory=tuple)

    def __post_init__(self):
        # Validate unique step IDs
        step_ids = [s.step_id for s in self.steps]
        if len(step_ids) != len(set(step_ids)):
            duplicates = [sid for sid in step_ids if step_ids.count(sid) > 1]
            raise CompileError(f"Duplicate step IDs: {set(duplicates)}")

    def get_step(self, step_id: str) -> Optional[StepDef]:
        """Get a step by ID."""
        for step in self.steps:
            if step.step_id == step_id:
                return step
        return None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON/YAML output."""
        return {
            "job_id": self.job_id,
            "version": self.version,
            "steps": [
                {
                    "step_id": s.step_id,
                    "op": s.op.value,
                    "params": s.params,
                    **({"phase_id": s.phase_id} if s.phase_id else {}),
                    "timeout_s": s.timeout_s,  # Always include (default 300s)
                    **({"continue_on_error": s.continue_on_error} if s.continue_on_error else {}),
                    **({"if": s.if_} if s.if_ else {}),
                    **({"idempotency": {
                        "scope": s.idempotency.scope,
                        **({"semantic_key_ref": s.idempotency.semantic_key_ref}
                           if s.idempotency.semantic_key_ref else {}),
                        **({"include_payload_hash": s.idempotency.include_payload_hash}
                           if s.idempotency.include_payload_hash else {}),
                    }} if s.idempotency else {}),
                }
                for s in self.steps
            ],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobDef":
        """Deserialize from dictionary."""
        steps = []
        for step_data in data.get("steps", []):
            idempotency = None
            if "idempotency" in step_data:
                idem_data = step_data["idempotency"]
                idempotency = IdempotencyConfig(
                    scope=idem_data["scope"],
                    semantic_key_ref=idem_data.get("semantic_key_ref"),
                    include_payload_hash=idem_data.get("include_payload_hash", False),
                )

            steps.append(StepDef(
                step_id=step_data["step_id"],
                op=Op.from_string(step_data["op"]),
                params=step_data.get("params", {}),
                phase_id=step_data.get("phase_id"),
                timeout_s=step_data.get("timeout_s", 300),  # Default 300s per e005 spec
                continue_on_error=step_data.get("continue_on_error", False),
                if_=step_data.get("if"),
                idempotency=idempotency,
            ))

        return cls(
            job_id=data["job_id"],
            version=data["version"],
            steps=tuple(steps),
        )
