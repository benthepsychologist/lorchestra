"""
Op enum defining the operation taxonomy for lorchestra.

Operations are categorized by their backend:
- call.*    -> callable: in-proc callable dispatch
- compute.* -> inferometer: LLM and external compute
- job.*     -> orchestration: sub-job execution
"""

from enum import Enum
from typing import Literal


class Op(str, Enum):
    """
    Enumeration of all valid operations in lorchestra.

    Naming convention: {category}.{action}

    Backend mapping:
    - call.* => callable (in-proc dispatch)
    - compute.* => inferometer (LLM service)
    - job.* => orchestration (lorchestra)
    """
    # Callable dispatch (in-proc)
    CALL_INJEST = "call.injest"
    CALL_CANONIZER = "call.canonizer"
    CALL_FINALFORM = "call.finalform"
    CALL_PROJECTIONIST = "call.projectionist"
    CALL_WORKMAN = "call.workman"

    # LLM (via inferometer)
    COMPUTE_LLM = "compute.llm"

    # Nested job
    JOB_RUN = "job.run"

    @property
    def backend(self) -> Literal["callable", "inferometer", "orchestration"]:
        """
        Get the backend responsible for executing this operation.

        Returns:
            - "callable" for call.* (handled by in-proc callable dispatch)
            - "inferometer" for compute.* (handled by inferometer service)
            - "orchestration" for job.* (handled by lorchestra itself)
        """
        if self.value.startswith("call."):
            return "callable"
        elif self.value.startswith("compute."):
            return "inferometer"
        else:
            return "orchestration"

    @classmethod
    def from_string(cls, value: str) -> "Op":
        """Parse an Op from its string value."""
        for op in cls:
            if op.value == value:
                return op
        raise ValueError(f"Unknown operation: {value}")
