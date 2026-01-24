"""
Attempt schemas - tracking execution attempts and step outcomes.

AttemptRecord tracks a single execution attempt of a run.
StepOutcome tracks the result of executing a single step within an attempt.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

# ULID type alias for documentation
ULID = str


class StepStatus(str, Enum):
    """Status of a step execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class StepOutcome:
    """
    The outcome of executing a single step within an attempt.

    Attributes:
        step_id: Identifier of the step
        status: Execution status (pending, running, completed, failed, skipped)
        started_at: When step execution started (null if pending/skipped)
        completed_at: When step execution completed (null if pending/running)
        manifest_ref: Reference to the StepManifest used for execution
        output_ref: Reference to stored output (e.g., GCS path or inline)
        error: Error details if status is failed
    """
    step_id: str
    status: StepStatus
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    manifest_ref: Optional[str] = None
    output_ref: Optional[str] = None
    error: Optional[dict[str, Any]] = None

    def __post_init__(self):
        # Validate status-dependent fields
        if self.status == StepStatus.PENDING:
            if self.started_at is not None or self.completed_at is not None:
                raise ValueError("Pending steps should not have started_at or completed_at")
        elif self.status == StepStatus.RUNNING:
            if self.started_at is None:
                raise ValueError("Running steps must have started_at")
            if self.completed_at is not None:
                raise ValueError("Running steps should not have completed_at")
        elif self.status in (StepStatus.COMPLETED, StepStatus.FAILED):
            if self.started_at is None or self.completed_at is None:
                raise ValueError(f"{self.status.value} steps must have started_at and completed_at")
        # SKIPPED steps may or may not have timestamps depending on when skip was determined

    @property
    def duration_ms(self) -> Optional[int]:
        """Calculate execution duration in milliseconds if both timestamps present."""
        if self.started_at and self.completed_at:
            delta = self.completed_at - self.started_at
            return int(delta.total_seconds() * 1000)
        return None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON output."""
        result: dict[str, Any] = {
            "step_id": self.step_id,
            "status": self.status.value,
        }
        if self.started_at is not None:
            result["started_at"] = self.started_at.isoformat()
        if self.completed_at is not None:
            result["completed_at"] = self.completed_at.isoformat()
        if self.manifest_ref is not None:
            result["manifest_ref"] = self.manifest_ref
        if self.output_ref is not None:
            result["output_ref"] = self.output_ref
        if self.error is not None:
            result["error"] = self.error
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StepOutcome":
        """Deserialize from dictionary."""
        return cls(
            step_id=data["step_id"],
            status=StepStatus(data["status"]),
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            manifest_ref=data.get("manifest_ref"),
            output_ref=data.get("output_ref"),
            error=data.get("error"),
        )


@dataclass(frozen=True)
class AttemptRecord:
    """
    A record of a single execution attempt for a run.

    Jobs may be retried, creating multiple attempts. Each attempt
    tracks which steps were executed and their outcomes.

    Attributes:
        run_id: ULID of the run this attempt belongs to
        attempt_n: Attempt number (1-indexed)
        started_at: When this attempt started
        completed_at: When this attempt completed (null if still running)
        status: Overall attempt status (derived from step outcomes)
        step_outcomes: List of outcomes for each step in this attempt
    """
    run_id: ULID
    attempt_n: int
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: StepStatus = StepStatus.PENDING
    step_outcomes: tuple[StepOutcome, ...] = field(default_factory=tuple)

    def __post_init__(self):
        if self.attempt_n < 1:
            raise ValueError("attempt_n must be >= 1")

    def get_outcome(self, step_id: str) -> Optional[StepOutcome]:
        """Get the outcome for a specific step."""
        for outcome in self.step_outcomes:
            if outcome.step_id == step_id:
                return outcome
        return None

    def get_failed_steps(self) -> tuple[StepOutcome, ...]:
        """Get all failed step outcomes."""
        return tuple(o for o in self.step_outcomes if o.status == StepStatus.FAILED)

    def get_completed_steps(self) -> tuple[StepOutcome, ...]:
        """Get all successfully completed step outcomes."""
        return tuple(o for o in self.step_outcomes if o.status == StepStatus.COMPLETED)

    @property
    def duration_ms(self) -> Optional[int]:
        """Calculate attempt duration in milliseconds if completed."""
        if self.completed_at:
            delta = self.completed_at - self.started_at
            return int(delta.total_seconds() * 1000)
        return None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON output."""
        result: dict[str, Any] = {
            "run_id": self.run_id,
            "attempt_n": self.attempt_n,
            "started_at": self.started_at.isoformat(),
            "status": self.status.value,
            "step_outcomes": [o.to_dict() for o in self.step_outcomes],
        }
        if self.completed_at is not None:
            result["completed_at"] = self.completed_at.isoformat()
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AttemptRecord":
        """Deserialize from dictionary."""
        return cls(
            run_id=data["run_id"],
            attempt_n=data["attempt_n"],
            started_at=datetime.fromisoformat(data["started_at"]),
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            status=StepStatus(data.get("status", "pending")),
            step_outcomes=tuple(StepOutcome.from_dict(o) for o in data.get("step_outcomes", [])),
        )
