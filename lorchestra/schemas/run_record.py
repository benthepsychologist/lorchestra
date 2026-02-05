"""
RunRecord schema - tracks a job execution run.

A RunRecord is created when a JobInstance begins execution.
It captures the runtime context and tracks the run's progress.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

# ULID type alias for documentation
ULID = str


def _utcnow() -> datetime:
    """Return current UTC time as timezone-aware datetime."""
    return datetime.now(timezone.utc)


@dataclass
class RunRecord:
    """
    A record of a job run.

    Created when a JobInstance starts execution. The run_id is a ULID
    providing both uniqueness and time-ordering. Updated on completion
    with status, duration, row counts, and any errors.

    Attributes:
        run_id: ULID uniquely identifying this run
        job_id: The job being run (from JobInstance)
        job_def_sha256: Content hash linking to the exact JobDef version
        envelope: Runtime context/payload passed to the job
        started_at: When the run started
        completed_at: When the run completed (None if still running)
        status: 'running', 'success', 'failed'
        duration_ms: Total execution time in milliseconds
        rows_read: Number of rows read (from call/query steps)
        rows_written: Number of rows written (from storacle.submit)
        errors: List of error messages if any
    """
    run_id: ULID
    job_id: str
    job_def_sha256: str
    envelope: dict[str, Any] = field(default_factory=dict)
    started_at: datetime = field(default_factory=_utcnow)
    completed_at: Optional[datetime] = None
    status: str = "running"
    duration_ms: Optional[int] = None
    rows_read: int = 0
    rows_written: int = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON output."""
        result = {
            "run_id": self.run_id,
            "job_id": self.job_id,
            "job_def_sha256": self.job_def_sha256,
            "envelope": self.envelope,
            "started_at": self.started_at.isoformat(),
            "status": self.status,
        }
        if self.completed_at:
            result["completed_at"] = self.completed_at.isoformat()
        if self.duration_ms is not None:
            result["duration_ms"] = self.duration_ms
        if self.rows_read:
            result["rows_read"] = self.rows_read
        if self.rows_written:
            result["rows_written"] = self.rows_written
        if self.errors:
            result["errors"] = self.errors
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RunRecord":
        """Deserialize from dictionary."""
        completed_at = None
        if data.get("completed_at"):
            completed_at = datetime.fromisoformat(data["completed_at"])
        return cls(
            run_id=data["run_id"],
            job_id=data["job_id"],
            job_def_sha256=data["job_def_sha256"],
            envelope=data.get("envelope", {}),
            started_at=datetime.fromisoformat(data["started_at"]),
            completed_at=completed_at,
            status=data.get("status", "running"),
            duration_ms=data.get("duration_ms"),
            rows_read=data.get("rows_read", 0),
            rows_written=data.get("rows_written", 0),
            errors=data.get("errors", []),
        )
