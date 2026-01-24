"""
RunRecord schema - tracks a job execution run.

A RunRecord is created when a JobInstance begins execution.
It captures the runtime context and tracks the run's progress.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

# ULID type alias for documentation
ULID = str


def _utcnow() -> datetime:
    """Return current UTC time as timezone-aware datetime."""
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class RunRecord:
    """
    A record of a job run.

    Created when a JobInstance starts execution. The run_id is a ULID
    providing both uniqueness and time-ordering.

    Attributes:
        run_id: ULID uniquely identifying this run
        job_id: The job being run (from JobInstance)
        job_def_sha256: Content hash linking to the exact JobDef version
        envelope: Runtime context/payload passed to the job
        started_at: When the run started
    """
    run_id: ULID
    job_id: str
    job_def_sha256: str
    envelope: dict[str, Any] = field(default_factory=dict)
    started_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON output."""
        return {
            "run_id": self.run_id,
            "job_id": self.job_id,
            "job_def_sha256": self.job_def_sha256,
            "envelope": self.envelope,
            "started_at": self.started_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RunRecord":
        """Deserialize from dictionary."""
        return cls(
            run_id=data["run_id"],
            job_id=data["job_id"],
            job_def_sha256=data["job_def_sha256"],
            envelope=data.get("envelope", {}),
            started_at=datetime.fromisoformat(data["started_at"]),
        )
