"""
Op enum defining the operation taxonomy for lorchestra.

Operations are categorized by their backend:
- call           -> callable: in-proc callable dispatch (generic, name in params)
- plan.build     -> native: lorchestra-native plan construction
- storacle.submit -> native: lorchestra-native storacle submission
- compute.*      -> inferometer: LLM and external compute
- job.*          -> orchestration: sub-job execution
"""

from enum import Enum
from typing import Literal


class Op(str, Enum):
    """
    Enumeration of all valid operations in lorchestra.

    Backend mapping:
    - call              => callable (in-proc dispatch, callable name in params)
    - plan.build        => native (lorchestra plan construction)
    - storacle.submit   => native (lorchestra storacle submission)
    - compute.*         => inferometer (LLM service)
    - job.*             => orchestration (lorchestra)
    """
    # Callable dispatch (in-proc, generic)
    CALL = "call"

    # Native lorchestra ops
    PLAN_BUILD = "plan.build"
    STORACLE_SUBMIT = "storacle.submit"

    # LLM (via inferometer)
    COMPUTE_LLM = "compute.llm"

    # Nested job
    JOB_RUN = "job.run"

    @property
    def backend(self) -> Literal["callable", "native", "inferometer", "orchestration"]:
        """
        Get the backend responsible for executing this operation.

        Returns:
            - "callable" for call (handled by in-proc callable dispatch)
            - "native" for plan.build, storacle.submit (handled by executor directly)
            - "inferometer" for compute.* (handled by inferometer service)
            - "orchestration" for job.* (handled by lorchestra itself)
        """
        if self.value == "call":
            return "callable"
        elif self.value in ("plan.build", "storacle.submit"):
            return "native"
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
