"""
Op enum defining the operation taxonomy for lorchestra.

Operations are categorized by their backend and purpose:
- query.*  -> data_plane: read-only data operations
- write.*  -> data_plane: write data operations (require idempotency)
- assert.* -> data_plane: validation/assertion operations
- compute.* -> compute: external IO and computation
- job.run  -> orchestration: sub-job execution
"""

from enum import Enum
from typing import Literal


class OpCategory(str, Enum):
    """High-level operation categories."""
    QUERY = "query"
    WRITE = "write"
    ASSERT = "assert"
    COMPUTE = "compute"
    JOB = "job"


class Op(str, Enum):
    """
    Enumeration of all valid operations in lorchestra.

    Naming convention: {category}.{action}

    Backend mapping:
    - query.*, write.*, assert.* => data_plane (storacle)
    - compute.* => compute
    - job.* => orchestration (lorchestra)
    """
    # Query operations (data_plane, read-only)
    # Per e005 spec: typed methods only, no raw SQL
    QUERY_RAW_OBJECTS = "query.raw_objects"
    QUERY_CANONICAL_OBJECTS = "query.canonical_objects"
    QUERY_LAST_SYNC = "query.last_sync"

    # Write operations (data_plane, require idempotency)
    WRITE_UPSERT = "write.upsert"
    WRITE_INSERT = "write.insert"
    WRITE_DELETE = "write.delete"
    WRITE_MERGE = "write.merge"

    # Assert operations (data_plane, validation)
    ASSERT_ROWS = "assert.rows"
    ASSERT_SCHEMA = "assert.schema"
    ASSERT_UNIQUE = "assert.unique"

    # Compute operations (compute backend, external IO)
    # Per e005 spec: llm, transform, extract, render
    COMPUTE_LLM = "compute.llm"
    COMPUTE_TRANSFORM = "compute.transform"
    COMPUTE_EXTRACT = "compute.extract"
    COMPUTE_RENDER = "compute.render"

    # Job operations (orchestration)
    JOB_RUN = "job.run"

    @property
    def category(self) -> OpCategory:
        """Get the category of this operation."""
        prefix = self.value.split(".")[0]
        return OpCategory(prefix)

    @property
    def backend(self) -> Literal["data_plane", "compute", "orchestration"]:
        """
        Get the backend responsible for executing this operation.

        Returns:
            - "data_plane" for query.*, write.*, assert.* (handled by storacle)
            - "compute" for compute.* (handled by compute service)
            - "orchestration" for job.* (handled by lorchestra itself)
        """
        cat = self.category
        if cat in (OpCategory.QUERY, OpCategory.WRITE, OpCategory.ASSERT):
            return "data_plane"
        elif cat == OpCategory.COMPUTE:
            return "compute"
        else:  # JOB
            return "orchestration"

    @property
    def requires_idempotency(self) -> bool:
        """
        Check if this operation requires idempotency configuration.

        Only write operations require idempotency settings.
        """
        return self.category == OpCategory.WRITE

    @classmethod
    def from_string(cls, value: str) -> "Op":
        """Parse an Op from its string value."""
        for op in cls:
            if op.value == value:
                return op
        raise ValueError(f"Unknown operation: {value}")

    def is_write_op(self) -> bool:
        """Check if this is a write operation."""
        return self.category == OpCategory.WRITE

    def is_query_op(self) -> bool:
        """Check if this is a query operation."""
        return self.category == OpCategory.QUERY

    def is_assert_op(self) -> bool:
        """Check if this is an assert operation."""
        return self.category == OpCategory.ASSERT

    def is_compute_op(self) -> bool:
        """Check if this is a compute operation."""
        return self.category == OpCategory.COMPUTE

    def is_job_op(self) -> bool:
        """Check if this is a job operation."""
        return self.category == OpCategory.JOB
