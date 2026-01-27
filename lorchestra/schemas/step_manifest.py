"""
StepManifest schema - the dispatchable unit for step execution.

A StepManifest captures everything needed to execute or replay a step,
including resolved parameters, backend routing, and idempotency key.
"""

from dataclasses import dataclass, field
from typing import Any, Literal, Optional

from .ops import Op

# ULID type alias for documentation
ULID = str


@dataclass(frozen=True)
class StepManifest:
    """
    A dispatchable manifest for step execution.

    The StepManifest is the unit of work dispatched to backends.
    It is replay-safe: given the same manifest, execution produces
    the same result (idempotent execution).

    Attributes:
        run_id: ULID of the run this step belongs to
        step_id: Identifier of the step within the job
        backend: Target backend derived from op prefix
                 (callable for call.*, inferometer for compute.*, orchestration for job.*)
        op: The operation to execute
        resolved_params: Fully resolved parameters (no @ctx.*, @payload.*, or @run.* refs)
        prompt_hash: SHA256 of LLM prompt if applicable (nullable for non-LLM ops)
        idempotency_key: Computed key for idempotent execution
    """
    run_id: ULID
    step_id: str
    backend: Literal["callable", "inferometer", "orchestration"]
    op: Op
    resolved_params: dict[str, Any] = field(default_factory=dict)
    prompt_hash: Optional[str] = None
    idempotency_key: str = ""

    def __post_init__(self):
        # Validate backend matches op
        expected_backend = self.op.backend
        if self.backend != expected_backend:
            raise ValueError(
                f"Backend mismatch: op '{self.op.value}' expects backend '{expected_backend}', "
                f"but got '{self.backend}'"
            )

    @classmethod
    def from_op(
        cls,
        run_id: ULID,
        step_id: str,
        op: Op,
        resolved_params: dict[str, Any],
        idempotency_key: str,
        prompt_hash: Optional[str] = None,
    ) -> "StepManifest":
        """
        Create a StepManifest from an op, automatically deriving the backend.

        Args:
            run_id: ULID of the run
            step_id: Step identifier
            op: The operation
            resolved_params: Fully resolved parameters
            idempotency_key: Computed idempotency key
            prompt_hash: Optional prompt hash for LLM operations

        Returns:
            A new StepManifest with backend derived from op
        """
        return cls(
            run_id=run_id,
            step_id=step_id,
            backend=op.backend,
            op=op,
            resolved_params=resolved_params,
            prompt_hash=prompt_hash,
            idempotency_key=idempotency_key,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON output."""
        result = {
            "run_id": self.run_id,
            "step_id": self.step_id,
            "backend": self.backend,
            "op": self.op.value,
            "resolved_params": self.resolved_params,
            "idempotency_key": self.idempotency_key,
        }
        if self.prompt_hash is not None:
            result["prompt_hash"] = self.prompt_hash
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StepManifest":
        """Deserialize from dictionary."""
        return cls(
            run_id=data["run_id"],
            step_id=data["step_id"],
            backend=data["backend"],
            op=Op.from_string(data["op"]),
            resolved_params=data.get("resolved_params", {}),
            prompt_hash=data.get("prompt_hash"),
            idempotency_key=data.get("idempotency_key", ""),
        )
