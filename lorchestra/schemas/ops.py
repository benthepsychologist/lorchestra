"""
Op enum defining the operation taxonomy for lorchestra.

Operations are categorized by their backend:
- call.*    -> callable: in-proc callable dispatch
- compute.* -> inferator: LLM and external compute
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
    - compute.* => inferator (LLM service)
    - job.* => orchestration (lorchestra)
    """
    # Callable dispatch (in-proc)
    CALL_INJEST = "call.injest"
    CALL_CANONIZER = "call.canonizer"
    CALL_FINALFORM = "call.finalform"
    CALL_PROJECTIONIST = "call.projectionist"
    CALL_WORKMAN = "call.workman"

    # LLM (via inferator)
    COMPUTE_LLM = "compute.llm"

    # Nested job
    JOB_RUN = "job.run"

    @property
    def backend(self) -> Literal["callable", "inferator", "orchestration"]:
        """
        Get the backend responsible for executing this operation.

        Returns:
            - "callable" for call.* (handled by in-proc callable dispatch)
            - "inferator" for compute.* (handled by inferator service)
            - "orchestration" for job.* (handled by lorchestra itself)
        """
        if self.value.startswith("call."):
            return "callable"
        elif self.value.startswith("compute."):
            return "inferator"
        else:
            return "orchestration"

    @classmethod
    def from_string(cls, value: str) -> "Op":
        """Parse an Op from its string value."""
        for op in cls:
            if op.value == value:
                return op
        raise ValueError(f"Unknown operation: {value}")
